// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"bytes"
	"strconv"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	log "github.com/sirupsen/logrus"
)

const (
	scanLimit                    = 1000
	defaultAdjacentPeerLimit     = 1
	defaultAdjacentLeaderLimit   = 64
	minAdjacentSchedulerInterval = time.Second
	maxAdjacentSchedulerInterval = 30 * time.Second
)

func init() {
	schedule.RegisterScheduler("adjacent-region", func(limiter *schedule.Limiter, args []string) (schedule.Scheduler, error) {
		log.Infof("new BalanceAdjacentRegionScheduler")
		if len(args) == 2 {
			leaderLimit, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return nil, errors.Trace(err)
			}
			peerLimit, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return newBalanceAdjacentRegionScheduler(limiter, leaderLimit, peerLimit), nil
		}
		return newBalanceAdjacentRegionScheduler(limiter), nil
	})
}

// balanceAdjacentRegionScheduler will disperse adjacent regions.
// we will scan a part regions order by key, then select the longest
// adjacent regions and disperse them. finally, we will guarantee
// 1. any two adjacent regions' leader will not in the same store
// 2. the two regions' leader will not in the public store of this two regions
type balanceAdjacentRegionScheduler struct {
	*baseScheduler
	selector             schedule.Selector
	leaderLimit          uint64
	peerLimit            uint64
	lastKey              []byte
	cacheRegions         *adjacentState
	adjacentRegionsCount int
}

type adjacentState struct {
	assignedStoreIds []uint64
	regions          []*core.RegionInfo
	head             int
}

func (a *adjacentState) clear() {
	a.assignedStoreIds = a.assignedStoreIds[:0]
	a.regions = a.regions[:0]
	a.head = 0
	log.Infof("clear adjacentState (cache)")
}

func (a *adjacentState) len() int {
	return len(a.regions) - a.head
}

// newBalanceAdjacentRegionScheduler creates a scheduler that tends to disperse adjacent region
// on each store.
func newBalanceAdjacentRegionScheduler(limiter *schedule.Limiter, args ...uint64) schedule.Scheduler {
	filters := []schedule.Filter{
		schedule.NewBlockFilter(),
		schedule.NewStateFilter(),
		schedule.NewHealthFilter(),
		schedule.NewSnapshotCountFilter(),
		schedule.NewPendingPeerCountFilter(),
		schedule.NewRejectLeaderFilter(),
	}
	base := newBaseScheduler(limiter)
	s := &balanceAdjacentRegionScheduler{
		baseScheduler: base,
		selector:      schedule.NewRandomSelector(filters),
		leaderLimit:   defaultAdjacentLeaderLimit,
		peerLimit:     defaultAdjacentPeerLimit,
		lastKey:       []byte(""),
	}
	if len(args) == 2 {
		s.leaderLimit = args[0]
		s.peerLimit = args[1]
	}
	log.Infof("filters in BalanceAdjacentRegionScheduler RandomSelector")
	schedule.PFilters(s.selector.GetFilters())
	return s
}

func (l *balanceAdjacentRegionScheduler) GetName() string {
	return "balance-adjacent-region-scheduler"
}

func (l *balanceAdjacentRegionScheduler) GetType() string {
	return "adjacent-region"
}

func (l *balanceAdjacentRegionScheduler) GetMinInterval() time.Duration {
	return minAdjacentSchedulerInterval
}

func (l *balanceAdjacentRegionScheduler) GetNextInterval(interval time.Duration) time.Duration {
	return intervalGrow(interval, maxAdjacentSchedulerInterval, linearGrowth)
}

func (l *balanceAdjacentRegionScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return l.allowBalanceLeader() || l.allowBalanceLeader()
}

func (l *balanceAdjacentRegionScheduler) allowBalanceLeader() bool {
	r := l.limiter.OperatorCount(schedule.OpAdjacent|schedule.OpLeader) < l.leaderLimit
	log.Infof("balanceAdjacentRS check leader balance: %v", r)
	return r
}

func (l *balanceAdjacentRegionScheduler) allowBalancePeer() bool {
	return l.limiter.OperatorCount(schedule.OpAdjacent|schedule.OpRegion) < l.peerLimit
}

func (l *balanceAdjacentRegionScheduler) Schedule(cluster schedule.Cluster, opInfluence schedule.OpInfluence) []*schedule.Operator {
	log.Infof("--------- balanceAdjacentRegionScheduler Schedule")
	if l.cacheRegions == nil {
		log.Infof("new adjacentState ")
		l.cacheRegions = &adjacentState{
			assignedStoreIds: make([]uint64, 0, len(cluster.GetStores())),
			regions:          make([]*core.RegionInfo, 0, scanLimit),
			head:             0,
		}
	}
	// we will process cache firstly
	if l.cacheRegions.len() >= 2 {
		log.Infof("if we have cacheRegions, we will process them first")
		return l.process(cluster)
	}

	l.cacheRegions.clear()
	regions := cluster.ScanRegions(l.lastKey, scanLimit)
	log.Infof("got regions [%s]", regions)
	// scan to the end
	if len(regions) <= 1 {
		l.adjacentRegionsCount = 0
		schedulerStatus.WithLabelValues(l.GetName(), "adjacent_count").Set(float64(l.adjacentRegionsCount))
		l.lastKey = []byte("")
		log.Infof("have no more regions to schedule")
		return nil
	}

	// calculate max adjacentRegions and record to the cache
	adjacentRegions := make([]*core.RegionInfo, 0, scanLimit)
	adjacentRegions = append(adjacentRegions, regions[0])
	maxLen := 0
	for i, r := range regions[1:] {
		log.Infof("check region[%s]", r)
		l.lastKey = r.StartKey

		// append if the region are adjacent
		lastRegion := adjacentRegions[len(adjacentRegions)-1]
		if lastRegion.Leader.GetStoreId() == r.Leader.GetStoreId() && bytes.Equal(lastRegion.EndKey, r.StartKey) {
			log.Infof("r1[%s] and r2[%s] are adjacent and leader on the same store", lastRegion, r)
			adjacentRegions = append(adjacentRegions, r)
			if i != len(regions)-2 { // not the last element
				continue
			}
		}

		if len(adjacentRegions) == 1 {
			adjacentRegions[0] = r
		} else {
			// got an max length adjacent regions in this range
			if maxLen < len(adjacentRegions) {
				l.cacheRegions.clear()
				maxLen = len(adjacentRegions)
				l.cacheRegions.regions = append(l.cacheRegions.regions, adjacentRegions...)
				log.Infof("cache adjacent regions: %s", l.cacheRegions.regions)
				adjacentRegions = adjacentRegions[:0]
				adjacentRegions = append(adjacentRegions, r)
			} else {
				log.Infof("these regions is less than cache, so we ignore them [%s]", adjacentRegions)
			}
		}
	}

	l.adjacentRegionsCount += maxLen
	return l.process(cluster)
}

func (l *balanceAdjacentRegionScheduler) process(cluster schedule.Cluster) []*schedule.Operator {
	if l.cacheRegions.len() < 2 {
		return nil
	}
	head := l.cacheRegions.head
	r1 := l.cacheRegions.regions[head]
	r2 := l.cacheRegions.regions[head+1]
	log.Infof("in process, prepare to chec regions: r1[%s] ,r2[%s]", r1, r2)

	defer func() {
		if l.cacheRegions.len() < 0 {
			log.Fatalf("[%s]the cache overflow should never happen", l.GetName())
		}
		l.cacheRegions.head = head + 1
		l.lastKey = r2.StartKey
	}()
	if l.unsafeToBalance(cluster, r1) {
		schedulerCounter.WithLabelValues(l.GetName(), "skip").Inc()
		log.Infof("check region[%s] as source failed by filters in selector and not hot, all replicas", r1)
		return nil
	}
	log.Infof("check region[%s] as source passed by filters in selector and not hot, all replicas", r1)

	op := l.disperseLeader(cluster, r1, r2)
	if op == nil {
		schedulerCounter.WithLabelValues(l.GetName(), "no_leader").Inc()
		op = l.dispersePeer(cluster, r1)
	}
	if op == nil {
		schedulerCounter.WithLabelValues(l.GetName(), "no_peer").Inc()
		l.cacheRegions.assignedStoreIds = l.cacheRegions.assignedStoreIds[:0]
		return nil
	}
	return []*schedule.Operator{op}
}

func (l *balanceAdjacentRegionScheduler) unsafeToBalance(cluster schedule.Cluster, region *core.RegionInfo) bool {
	if len(region.GetPeers()) != cluster.GetMaxReplicas() {
		return true
	}
	store := cluster.GetStore(region.Leader.GetStoreId())
	s := l.selector.SelectSource(cluster, []*core.StoreInfo{store})
	if s == nil {
		return true
	}
	// Skip hot regions.
	if cluster.IsRegionHot(region.GetId()) {
		schedulerCounter.WithLabelValues(l.GetName(), "region_hot").Inc()
		return true
	}
	return false
}

// code_analysis disperse 分散
func (l *balanceAdjacentRegionScheduler) disperseLeader(cluster schedule.Cluster, before *core.RegionInfo, after *core.RegionInfo) *schedule.Operator {
	log.Infof("try to disperse leader for region[%s] [%s]", before, after)
	if !l.allowBalanceLeader() {
		return nil
	}
	diffPeers := before.GetDiffFollowers(after)
	if len(diffPeers) == 0 {
		return nil
	}
	storesInfo := make([]*core.StoreInfo, 0, len(diffPeers))
	for _, p := range diffPeers {
		storesInfo = append(storesInfo, cluster.GetStore(p.GetStoreId()))
	}
	log.Infof("to make adjacent regions disperse , we filter stores that not have next region")
	target := l.selector.SelectTarget(cluster, storesInfo)
	if target == nil {
		return nil
	}
	step := schedule.TransferLeader{FromStore: before.Leader.GetStoreId(), ToStore: target.GetId()}
	log.Infof("new step[TransferLeader] %s", step)
	op := schedule.NewOperator("balance-adjacent-leader", before.GetId(), before.GetRegionEpoch(), schedule.OpAdjacent|schedule.OpLeader, step)
	op.SetPriorityLevel(core.LowPriority)
	log.Infof("new operator[%s]", op)
	schedulerCounter.WithLabelValues(l.GetName(), "adjacent_leader").Inc()
	return op
}

func (l *balanceAdjacentRegionScheduler) dispersePeer(cluster schedule.Cluster, region *core.RegionInfo) *schedule.Operator {
	log.Infof("try to disperse peer for region[%s]", region)
	if !l.allowBalancePeer() {
		return nil
	}
	// scoreGuard guarantees that the distinct score will not decrease.
	leaderStoreID := region.Leader.GetStoreId()
	stores := cluster.GetRegionStores(region)
	source := cluster.GetStore(leaderStoreID)
	scoreGuard := schedule.NewDistinctScoreFilter(cluster.GetLocationLabels(), stores, source)
	excludeStores := region.GetStoreIds()
	for _, storeID := range l.cacheRegions.assignedStoreIds {
		if _, ok := excludeStores[storeID]; !ok {
			excludeStores[storeID] = struct{}{}
		}
	}

	filters := []schedule.Filter{
		schedule.NewExcludedFilter(nil, excludeStores),
		scoreGuard,
	}
	target := l.selector.SelectTarget(cluster, cluster.GetStores(), filters...)
	log.Infof("disperse peer got target store[%s]", target)
	if target == nil {
		return nil
	}
	newPeer, err := cluster.AllocPeer(target.GetId())
	if err != nil {
		return nil
	}
	log.Infof("disperse peer got target peer id[%d] ", newPeer.Id)
	if newPeer == nil {
		schedulerCounter.WithLabelValues(l.GetName(), "no_peer").Inc()
		return nil
	}

	// record the store id and exclude it in next time
	l.cacheRegions.assignedStoreIds = append(l.cacheRegions.assignedStoreIds, newPeer.GetStoreId())

	op, err := schedule.CreateMovePeerOperator("balance-adjacent-peer", cluster, region, schedule.OpAdjacent, leaderStoreID, newPeer.GetStoreId(), newPeer.GetId())
	if err != nil {
		schedulerCounter.WithLabelValues(l.GetName(), "create_operator_fail").Inc()
		return nil
	}
	op.SetPriorityLevel(core.LowPriority)
	schedulerCounter.WithLabelValues(l.GetName(), "adjacent_peer").Inc()
	return op
}
