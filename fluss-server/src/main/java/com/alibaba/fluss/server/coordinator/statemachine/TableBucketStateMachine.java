/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.coordinator.statemachine;

import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.server.coordinator.CoordinatorContext;
import com.alibaba.fluss.server.coordinator.CoordinatorRequestBatch;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.shaded.guava32.com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** A state machine for {@link TableBucket}. */
public class TableBucketStateMachine {

    private static final Logger LOG = LoggerFactory.getLogger(TableBucketStateMachine.class);

    private final CoordinatorContext coordinatorContext;
    private final CoordinatorRequestBatch coordinatorRequestBatch;
    private final ZooKeeperClient zooKeeperClient;

    public TableBucketStateMachine(
            CoordinatorContext coordinatorContext,
            CoordinatorRequestBatch coordinatorRequestBatch,
            ZooKeeperClient zooKeeperClient) {
        this.coordinatorContext = coordinatorContext;
        this.coordinatorRequestBatch = coordinatorRequestBatch;
        this.zooKeeperClient = zooKeeperClient;
    }

    public void startup() {
        LOG.info("Initializing bucket state machine.");
        initializeBucketState();
        LOG.info("Triggering online table bucket changes");
        triggerOnlineBucketStateChange();
        LOG.debug(
                "Started bucket state machine with initial state {}.",
                coordinatorContext.getBucketStates());
    }

    /**
     * Invoked on startup of the table bucket's state machine to set initial state for all existing
     * table buckets in zookeeper.
     */
    private void initializeBucketState() {
        Set<TableBucket> tableBuckets = coordinatorContext.allBuckets();
        for (TableBucket tableBucket : tableBuckets) {
            BucketState bucketState =
                    coordinatorContext
                            .getBucketLeaderAndIsr(tableBucket)
                            .map(
                                    leaderAndIsr -> {
                                        // ONLINE if the leader is alive, otherwise, it's OFFLINE
                                        if (coordinatorContext.isReplicaAndServerOnline(
                                                leaderAndIsr.leader(), tableBucket)) {
                                            return BucketState.OnlineBucket;
                                        } else {
                                            return BucketState.OfflineBucket;
                                        }
                                    })
                            // if the leader info not exist, then it's in NEW state
                            .orElse(BucketState.NewBucket);
            coordinatorContext.putBucketState(tableBucket, bucketState);
        }
    }

    public void triggerOnlineBucketStateChange() {
        Set<TableBucket> buckets =
                coordinatorContext.bucketsInStates(
                        Sets.newHashSet(BucketState.NewBucket, BucketState.OfflineBucket));

        buckets =
                buckets.stream()
                        .filter(tableBucket -> !coordinatorContext.isToBeDeleted(tableBucket))
                        .collect(Collectors.toSet());
        handleStateChange(buckets, BucketState.OnlineBucket);
    }

    public void shutdown() {
        LOG.info("Shutdown table bucket state machine.");
    }

    public void handleStateChange(Set<TableBucket> tableBuckets, BucketState targetState) {
        try {
            coordinatorRequestBatch.newBatch();
            for (TableBucket tableBucket : tableBuckets) {
                doHandleStateChange(tableBucket, targetState);
            }
            coordinatorRequestBatch.sendRequestToTabletServers(
                    coordinatorContext.getCoordinatorEpoch());
        } catch (Throwable e) {
            LOG.error("Failed to move table buckets {} to state {}.", tableBuckets, targetState, e);
        }
    }

    /**
     * Handle the state change of TableBucket. It's the core state transition logic of the state
     * machine. It ensures that every state transition happens from a legal previous state to the
     * target state. The valid state transitions for the state machine are as follows:
     *
     * <p>NonExistentBucket -> NewBucket:
     *
     * <p>-- Case1: create a new bucket while creating a new table; Do: mark it as NewBucket
     *
     * <p>-- Case2: load table assignment from zookeeper, and haven't ever elected a leader for the
     * bucket; Do: mark it as NewBucket
     *
     * <p>NewBucket -> OnlineBucket:
     *
     * <p>-- The bucket hasn't been elected a leader. Do: elect a leader for it, send the leader
     * info to the servers that hold the replicas of the bucket and mark it as OnlineBucket.
     *
     * <p>OnlineBucket, OfflineBucket -> OnlineBucket:
     *
     * <p>-- For OfflineBucket -> OnlineBucket, it happens that we choose a new replica as the
     * leader since the previous leader fail. Do: choose a new leader, send the leader info to the
     * servers that hold the replicas of the bucket and mark it as OnlineBucket.
     *
     * <p>-- For OnlineBucket -> OnlineBucket, it happens on tablet server that holds leaders of
     * bucket shutdown graceful. Coordinator server receives the shutdown request from tablet server
     * and choose other replicas as the leader. Do: choose a new leader, send the leader info to the
     * servers that hold the replicas of the bucket and mark it as OnlineBucket.
     *
     * <p>NewBucket, OnlineBucket, OfflineBucket -> OfflineBucket
     *
     * <p>-- For NewBucket, OfflineBucket -> OfflineBucket, it happens that we drop the table bucket
     * whiling dropping the table. Do: mark it as OfflineBucket
     *
     * <p>-- For OnlineBucket -> OfflineBucket, it happens that the tablet server holds the previous
     * leader replica fail; Do: mark it as OfflineBucket.
     *
     * <p>OfflinePartition -> NonExistentPartition
     *
     * <p>-- Only happens when dropping the table. Do: remove it from state machine.
     *
     * @param tableBucket The table bucket that is to do state change
     * @param targetState the target state that is to change to
     */
    private void doHandleStateChange(TableBucket tableBucket, BucketState targetState) {
        coordinatorContext.putBucketStateIfNotExists(tableBucket, BucketState.NonExistentBucket);
        if (!checkValidTableBucketStateChange(tableBucket, targetState)) {
            return;
        }
        BucketState currentState = coordinatorContext.getBucketState(tableBucket);
        switch (targetState) {
            case NewBucket:
                doStateChange(tableBucket, targetState);
                break;
            case OnlineBucket:
                String partitionName = null;
                if (tableBucket.getPartitionId() != null) {
                    partitionName =
                            coordinatorContext.getPartitionName(tableBucket.getPartitionId());
                    if (partitionName == null) {
                        LOG.error(
                                "Can't find partition name for partition: {}.",
                                tableBucket.getBucket());
                        logFailedStateChange(tableBucket, currentState, targetState);
                        return;
                    }
                }
                if (currentState == BucketState.NewBucket) {
                    List<Integer> assignedServers = coordinatorContext.getAssignment(tableBucket);
                    // init the leader for table bucket
                    Optional<ElectionResult> optionalElectionResult =
                            initLeaderForTableBuckets(tableBucket, assignedServers);
                    if (!optionalElectionResult.isPresent()) {
                        logFailedStateChange(tableBucket, currentState, targetState);
                        return;
                    } else {
                        // transmit state
                        doStateChange(tableBucket, targetState);
                        // then send request to the tablet servers
                        coordinatorRequestBatch.addNotifyLeaderRequestForTabletServers(
                                new HashSet<>(optionalElectionResult.get().liveReplicas),
                                PhysicalTablePath.of(
                                        coordinatorContext.getTablePathById(
                                                tableBucket.getTableId()),
                                        partitionName),
                                tableBucket,
                                coordinatorContext.getAssignment(tableBucket),
                                optionalElectionResult.get().leaderAndIsr);
                    }
                } else {
                    // current state is Online or Offline
                    // not new bucket, we then need to update leader/epoch for the bucket
                    Optional<ElectionResult> optionalElectionResult =
                            electNewLeaderForTableBuckets(tableBucket);
                    if (!optionalElectionResult.isPresent()) {
                        logFailedStateChange(tableBucket, currentState, targetState);
                        return;
                    } else {
                        // transmit state
                        doStateChange(tableBucket, targetState);
                        ElectionResult electionResult = optionalElectionResult.get();
                        // then send request to the tablet servers
                        coordinatorRequestBatch.addNotifyLeaderRequestForTabletServers(
                                new HashSet<>(electionResult.liveReplicas),
                                PhysicalTablePath.of(
                                        coordinatorContext.getTablePathById(
                                                tableBucket.getTableId()),
                                        partitionName),
                                tableBucket,
                                coordinatorContext.getAssignment(tableBucket),
                                electionResult.leaderAndIsr);
                    }
                }
                break;
            case OfflineBucket:
                doStateChange(tableBucket, targetState);
                break;
            case NonExistentBucket:
                coordinatorContext.removeBucketState(tableBucket);
                break;
        }
        logSuccessfulStateChange(tableBucket, currentState, targetState);
    }

    private Optional<ElectionResult> initLeaderForTableBuckets(
            TableBucket tableBucket, List<Integer> assignedServers) {
        // filter out the live servers
        List<Integer> liveServers =
                assignedServers.stream()
                        .filter(
                                (server) ->
                                        coordinatorContext.isReplicaAndServerOnline(
                                                server, tableBucket))
                        .collect(Collectors.toList());
        // todo, consider this case, may reassign with other servers?
        if (liveServers.isEmpty()) {
            LOG.error(
                    "Encountered error during state change of table bucket {} from "
                            + "New to Online, assigned replicas are {}, live tablet servers are empty, "
                            + "No assigned replica is alive.",
                    stringifyBucket(tableBucket),
                    assignedServers);
            return Optional.empty();
        }
        if (liveServers.size() != assignedServers.size()) {
            LOG.warn(
                    "The assigned replicas are {}, but the live tablet servers are {}, which is less than "
                            + "assigned replicas.",
                    assignedServers,
                    liveServers);
        }
        // For the case that the table bucket has been initialized, we use all the live assigned
        // servers as inSyncReplica set.
        List<Integer> isr = liveServers;
        Optional<Integer> leaderOpt =
                ReplicaLeaderElectionAlgorithms.defaultReplicaLeaderElection(
                        assignedServers, liveServers, isr);
        if (!leaderOpt.isPresent()) {
            LOG.error(
                    "The leader election for table bucket {} is empty.",
                    stringifyBucket(tableBucket));
            return Optional.empty();
        }
        int leader = leaderOpt.get();

        // Register the initial leader and isr.
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(leader, 0, isr, coordinatorContext.getCoordinatorEpoch(), 0);
        try {
            zooKeeperClient.registerLeaderAndIsr(tableBucket, leaderAndIsr);
        } catch (Exception e) {
            LOG.error(
                    "Fail to create state node for table bucket {} in zookeeper.",
                    stringifyBucket(tableBucket),
                    e);
            return Optional.empty();
        }
        coordinatorContext.putBucketLeaderAndIsr(tableBucket, leaderAndIsr);
        return Optional.of(new ElectionResult(liveServers, leaderAndIsr));
    }

    private Optional<ElectionResult> electNewLeaderForTableBuckets(TableBucket tableBucket) {
        LeaderAndIsr leaderAndIsr;
        try {
            leaderAndIsr = zooKeeperClient.getLeaderAndIsr(tableBucket).get();
        } catch (Exception e) {
            LOG.error("Can't get state for table bucket {}.", stringifyBucket(tableBucket), e);
            return Optional.empty();
        }
        if (leaderAndIsr.coordinatorEpoch() > coordinatorContext.getCoordinatorEpoch()) {
            LOG.error(
                    "Aborted leader election for table bucket {} since the bucket state path was "
                            + "already written by another coordinator server. This probably means that the current coordinator server {}"
                            + " went through a soft failure and another coordinator was elected with epoch {}.",
                    tableBucket,
                    coordinatorContext.getCoordinatorEpoch(),
                    leaderAndIsr.coordinatorEpoch());
            return Optional.empty();
        }
        // re-election
        Optional<ElectionResult> optionalElectionResult =
                leaderForOffline(tableBucket, leaderAndIsr);
        if (!optionalElectionResult.isPresent()) {
            LOG.error(
                    "The result of elect leader for table bucket {} is empty.",
                    stringifyBucket(tableBucket));
            return Optional.empty();
        }
        ElectionResult electionResult = optionalElectionResult.get();
        try {
            zooKeeperClient.updateLeaderAndIsr(tableBucket, electionResult.leaderAndIsr);
        } catch (Exception e) {
            LOG.error(
                    "Fail to update bucket LeaderAndIsr for table bucket {}.",
                    stringifyBucket(tableBucket),
                    e);
            return Optional.empty();
        }
        coordinatorContext.putBucketLeaderAndIsr(tableBucket, electionResult.leaderAndIsr);
        return Optional.of(electionResult);
    }

    private boolean checkValidTableBucketStateChange(
            TableBucket tableBucket, BucketState targetState) {
        BucketState curState = coordinatorContext.getBucketState(tableBucket);
        if (isValidReplicaStateTransition(curState, targetState)) {
            return true;
        } else {
            logInvalidTransition(tableBucket, curState, targetState);
            logFailedStateChange(tableBucket, curState, targetState);
            return false;
        }
    }

    private void doStateChange(TableBucket tableBucket, BucketState targetState) {
        coordinatorContext.putBucketState(tableBucket, targetState);
    }

    private boolean isValidReplicaStateTransition(BucketState curState, BucketState targetState) {
        return targetState.getValidPreviousStates().contains(curState);
    }

    private void logInvalidTransition(
            TableBucket tableBucket, BucketState curState, BucketState targetState) {
        LOG.error(
                "Table bucket {} should be one of {} states before moving to {} state."
                        + " Instead it is in {}.",
                stringifyBucket(tableBucket),
                targetState.getValidPreviousStates(),
                targetState,
                curState);
    }

    private void logFailedStateChange(
            TableBucket tableBucket, BucketState currState, BucketState targetState) {
        LOG.error(
                "Fail to change state for table bucket {} from {} to {}.",
                stringifyBucket(tableBucket),
                currState,
                targetState);
    }

    private void logSuccessfulStateChange(
            TableBucket tableBucket, BucketState currState, BucketState targetState) {
        LOG.debug(
                "Successfully changed state for table bucket {} from {} to {}.",
                stringifyBucket(tableBucket),
                currState,
                targetState);
    }

    private String stringifyBucket(TableBucket tableBucket) {
        if (tableBucket.getPartitionId() == null) {
            return String.format(
                    "TableBucket{tableId=%d, bucket=%d, tablePath=%s}",
                    tableBucket.getTableId(),
                    tableBucket.getBucket(),
                    coordinatorContext.getTablePathById(tableBucket.getTableId()));
        } else {
            return String.format(
                    "TableBucket{tableId=%d, partitionId=%d, bucket=%d, tablePath=%s, partition=%s}",
                    tableBucket.getTableId(),
                    tableBucket.getPartitionId(),
                    tableBucket.getBucket(),
                    coordinatorContext.getTablePathById(tableBucket.getTableId()),
                    coordinatorContext.getPartitionName(tableBucket.getPartitionId()));
        }
    }

    /**
     * Elect a new leader for new or offline bucket, it'll always elect one from the live replicas
     * in isr set.
     */
    private Optional<ElectionResult> leaderForOffline(
            TableBucket tableBucket, LeaderAndIsr leaderAndIsr) {
        List<Integer> assignment = coordinatorContext.getAssignment(tableBucket);
        // filter out the live servers
        List<Integer> liveReplicas =
                assignment.stream()
                        .filter(
                                replica ->
                                        coordinatorContext.isReplicaAndServerOnline(
                                                replica, tableBucket))
                        .collect(Collectors.toList());
        // we'd like use the first live replica as the new leader
        if (liveReplicas.isEmpty()) {
            LOG.warn("No any live replica for table bucket {}.", stringifyBucket(tableBucket));
            return Optional.empty();
        }

        Optional<Integer> leaderOpt =
                ReplicaLeaderElectionAlgorithms.defaultReplicaLeaderElection(
                        assignment, liveReplicas, leaderAndIsr.isr());
        if (!leaderOpt.isPresent()) {
            LOG.error(
                    "The leader election for table bucket {} is empty.",
                    stringifyBucket(tableBucket));
            return Optional.empty();
        }

        // get the updated leader and isr
        LeaderAndIsr newLeaderAndIsr =
                new LeaderAndIsr(
                        leaderOpt.get(),
                        leaderAndIsr.leaderEpoch() + 1,
                        leaderAndIsr.isr(),
                        coordinatorContext.getCoordinatorEpoch(),
                        leaderAndIsr.bucketEpoch() + 1);

        return Optional.of(new ElectionResult(liveReplicas, newLeaderAndIsr));
    }

    private static class ElectionResult {
        private final List<Integer> liveReplicas;
        private final LeaderAndIsr leaderAndIsr;

        public ElectionResult(List<Integer> liveReplicas, LeaderAndIsr leaderAndIsr) {
            this.liveReplicas = liveReplicas;
            this.leaderAndIsr = leaderAndIsr;
        }
    }
}
