/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.SlotProviderStrategy;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default {@link ExecutionSlotAllocator} which will use {@link SlotProvider} to allocate slots and
 * keep the unfulfilled requests for further cancellation.
 */
class DefaultExecutionSlotAllocator extends AbstractExecutionSlotAllocator {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultExecutionSlotAllocator.class);

    private final SlotProviderStrategy slotProviderStrategy;

    DefaultExecutionSlotAllocator(
            final SlotProviderStrategy slotProviderStrategy,
            final PreferredLocationsRetriever preferredLocationsRetriever) {

        super(preferredLocationsRetriever);
        this.slotProviderStrategy = checkNotNull(slotProviderStrategy);
    }

    /*********************
     * clouding 注释: 2021/6/14 17:51
     *   1. 使用 slotExecutionVertexAssignments 封装需要申请slot的描述
     *   2. 依次遍历申请，每次申请一个 SlotExecutionVertexAssignment
     *********************/
    @Override
    public List<SlotExecutionVertexAssignment> allocateSlotsFor(
            List<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements) {

        validateSchedulingRequirements(executionVertexSchedulingRequirements);

        // clouding 注释: 2021/9/20 14:58
        //          executionVertexSchedulingRequirements --》 SlotExecutionVertexAssignment 的转换
        List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
                new ArrayList<>(executionVertexSchedulingRequirements.size());

        // clouding 注释: 2021/9/20 14:59
        //          所有的 AllocationIds
        Set<AllocationID> allPreviousAllocationIds =
                computeAllPriorAllocationIds(executionVertexSchedulingRequirements);

        // clouding 注释: 2021/9/20 14:59
        //          遍历所有的 executionVertexSchedulingRequirements，如果申请完成，就放入 slotExecutionVertexAssignments，
        //          上面创建了slotExecutionVertexAssignments这个集合
        for (ExecutionVertexSchedulingRequirements schedulingRequirements : executionVertexSchedulingRequirements) {

            final ExecutionVertexID executionVertexId =
                    schedulingRequirements.getExecutionVertexId();
            final SlotSharingGroupId slotSharingGroupId =
                    schedulingRequirements.getSlotSharingGroupId();

            final SlotRequestId slotRequestId = new SlotRequestId();

            // clouding 注释: 2021/6/14 17:53
            //          异步申请，申请一个Vertex的slot，拿到了 LogicalSlot
            //          LogicalSlot 逻辑Slot，  在JobMaster中申请体现，也就是下面这段代码
            //          PhysicalSlot 物理Slot。 在TaskExecutor中管理
            //          可能存在多个逻辑Slot隶属同一个物理Slot。主要是有个slot复用的情况。
            final CompletableFuture<LogicalSlot> slotFuture =
                    allocateSlot(schedulingRequirements, slotRequestId, allPreviousAllocationIds);

            final SlotExecutionVertexAssignment slotExecutionVertexAssignment =
                    createAndRegisterSlotExecutionVertexAssignment(
                            executionVertexId,
                            slotFuture,
                            // clouding 注释: 2021/9/20 15:11
                            //          这个是申请异常时，取消申请的处理
                            throwable ->
                                    slotProviderStrategy.cancelSlotRequest(
                                            slotRequestId, slotSharingGroupId, throwable));

            slotExecutionVertexAssignments.add(slotExecutionVertexAssignment);
        }

        return slotExecutionVertexAssignments;
    }

    private CompletableFuture<LogicalSlot> allocateSlot(
            final ExecutionVertexSchedulingRequirements schedulingRequirements,
            final SlotRequestId slotRequestId,
            final Set<AllocationID> allPreviousAllocationIds) {

        final ExecutionVertexID executionVertexId = schedulingRequirements.getExecutionVertexId();

        LOG.debug("Allocate slot with id {} for execution {}", slotRequestId, executionVertexId);

        // clouding 注释: 2021/9/20 15:09
        //          这里是计算了这个vertex的本地性
        final CompletableFuture<SlotProfile> slotProfileFuture =
                getSlotProfileFuture(
                        schedulingRequirements,
                        schedulingRequirements.getPhysicalSlotResourceProfile(),
                        Collections.emptySet(),
                        allPreviousAllocationIds);

        return slotProfileFuture.thenCompose(
                slotProfile ->
                        // clouding 注释: 2021/9/20 15:14
                        //           slotProviderStrategy = NormalSlotProviderStrategy
                        //          真正计算申请的地方
                        slotProviderStrategy.allocateSlot(
                                slotRequestId,
                                new ScheduledUnit(
                                        executionVertexId,
                                        schedulingRequirements.getSlotSharingGroupId(),
                                        schedulingRequirements.getCoLocationConstraint()),
                                slotProfile));
    }
}
