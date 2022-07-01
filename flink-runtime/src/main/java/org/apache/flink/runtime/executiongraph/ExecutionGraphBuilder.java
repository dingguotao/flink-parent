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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategyLoader;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategyFactoryLoader;
import org.apache.flink.runtime.executiongraph.metrics.DownTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.RestartTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.UpTimeGauge;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class to encapsulate the logic of building an {@link ExecutionGraph} from a {@link
 * JobGraph}.
 */
public class ExecutionGraphBuilder {

    /**
     * Builds the ExecutionGraph from the JobGraph. If a prior execution graph exists, the JobGraph
     * will be attached. If no prior execution graph exists, then the JobGraph will become attach to
     * a new empty execution graph.
     */
    public static ExecutionGraph buildGraph(
            @Nullable ExecutionGraph prior,
            JobGraph jobGraph,
            Configuration jobManagerConfig,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            SlotProvider slotProvider,
            ClassLoader classLoader,
            CheckpointRecoveryFactory recoveryFactory,
            Time rpcTimeout,
            RestartStrategy restartStrategy,
            MetricGroup metrics,
            BlobWriter blobWriter,
            Time allocationTimeout,
            Logger log,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker partitionTracker)
            throws JobExecutionException, JobException {

        final FailoverStrategy.Factory failoverStrategy =
                FailoverStrategyLoader.loadFailoverStrategy(jobManagerConfig, log);

        return buildGraph(
                prior,
                jobGraph,
                jobManagerConfig,
                futureExecutor,
                ioExecutor,
                slotProvider,
                classLoader,
                recoveryFactory,
                rpcTimeout,
                restartStrategy,
                metrics,
                blobWriter,
                allocationTimeout,
                log,
                shuffleMaster,
                partitionTracker,
                failoverStrategy);
    }

    public static ExecutionGraph buildGraph(
            @Nullable ExecutionGraph prior,
            JobGraph jobGraph,
            Configuration jobManagerConfig,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            SlotProvider slotProvider,
            ClassLoader classLoader,
            CheckpointRecoveryFactory recoveryFactory,
            Time rpcTimeout,
            RestartStrategy restartStrategy,
            MetricGroup metrics,
            BlobWriter blobWriter,
            Time allocationTimeout,
            Logger log,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker partitionTracker,
            FailoverStrategy.Factory failoverStrategyFactory)
            throws JobExecutionException, JobException {

        checkNotNull(jobGraph, "job graph cannot be null");

        final String jobName = jobGraph.getName();
        final JobID jobId = jobGraph.getJobID();

        final JobInformation jobInformation =
                new JobInformation(
                        jobId,
                        jobName,
                        jobGraph.getSerializedExecutionConfig(),
                        jobGraph.getJobConfiguration(),
                        jobGraph.getUserJarBlobKeys(),
                        jobGraph.getClasspaths());

        final int maxPriorAttemptsHistoryLength =
                jobManagerConfig.getInteger(JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE);

        final PartitionReleaseStrategy.Factory partitionReleaseStrategyFactory =
                PartitionReleaseStrategyFactoryLoader.loadPartitionReleaseStrategyFactory(
                        jobManagerConfig);

        // create a new execution graph, if none exists so far
        final ExecutionGraph executionGraph;
        try {
            executionGraph =
                    (prior != null)
                            ? prior
                            // clouding 注释: 2021/9/19 22:08
                            //          核心的 new ExecutionGraph
                            : new ExecutionGraph(
                                    jobInformation,
                                    futureExecutor, // 异步执行线程池
                                    ioExecutor,  // io操作线程池
                                    rpcTimeout, // rpc调用超时时间
                                    restartStrategy,    // 重启策略
                                    maxPriorAttemptsHistoryLength,  // 保留历史记录的最大重试次数
                                    failoverStrategyFactory,    // 失败重试工厂
                                    slotProvider,   // 向rm申请资源对象
                                    classLoader,
                                    blobWriter, // 用以将数据写入blob server
                                    allocationTimeout,  // 分配请求时间
                                    partitionReleaseStrategyFactory,    //  intermediateResultPartition释放工厂类
                                    shuffleMaster,  // 用以注册intermediateResultPartition, 向JobMaster注册数据分区
                                    partitionTracker,
                                    jobGraph.getScheduleMode());
        } catch (IOException e) {
            throw new JobException("Could not create the ExecutionGraph.", e);
        }

        // set the basic properties

        try {
            // clouding 注释: 2021/9/19 22:09
            //          把 jobGraph 转成JSON 放进来
            executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
        } catch (Throwable t) {
            log.warn("Cannot create JSON plan for job", t);
            // give the graph an empty plan
            executionGraph.setJsonPlan("{}");
        }

        // initialize the vertices that have a master initialization hook
        // file output formats create directories here, input formats create splits

        final long initMasterStart = System.nanoTime();
        log.info("Running initialization on master for job {} ({}).", jobName, jobId);

        // clouding 注释: 2021/9/19 22:27
        //          处理 JobGraph中的每一个Vertex，把JobVertex初始化
        for (JobVertex vertex : jobGraph.getVertices()) {
            // clouding 注释: 2022/6/25 17:59
            //          获取节点的调用类名
            String executableClass = vertex.getInvokableClassName();
            if (executableClass == null || executableClass.isEmpty()) {
                throw new JobSubmissionException(
                        jobId,
                        "The vertex "
                                + vertex.getID()
                                + " ("
                                + vertex.getName()
                                + ") has no invokable class.");
            }

            try {
                // clouding 注释: 2022/6/25 19:23
                //          根据不用节点类型,调用job启动时的逻辑
                vertex.initializeOnMaster(classLoader);
            } catch (Throwable t) {
                throw new JobExecutionException(
                        jobId,
                        "Cannot initialize task '" + vertex.getName() + "': " + t.getMessage(),
                        t);
            }
        }

        log.info(
                "Successfully ran initialization on master in {} ms.",
                (System.nanoTime() - initMasterStart) / 1_000_000);

        // topologically sort the job vertices and attach the graph to the existing one
        // clouding 注释: 2022/6/25 19:23
        //          按照顺序,获取所有的JobVertex
        List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
        if (log.isDebugEnabled()) {
            log.debug(
                    "Adding {} vertices from job graph {} ({}).",
                    sortedTopology.size(),
                    jobName,
                    jobId);
        }
        // clouding 注释: 2021/9/19 22:16
        //          把JobGraph转换成ExecutionGraph入口
        executionGraph.attachJobGraph(sortedTopology);

        if (log.isDebugEnabled()) {
            log.debug(
                    "Successfully created execution graph from job graph {} ({}).", jobName, jobId);
        }

        // configure the state checkpointing
        // clouding 注释: 2021/10/16 16:55
        //          配置checkpoint的
        JobCheckpointingSettings snapshotSettings = jobGraph.getCheckpointingSettings();
        if (snapshotSettings != null) {
            // clouding 注释: 2021/10/16 16:55
            //          这里把JobGraph的Vertex，转换成了ExecutionJobVertex
            List<ExecutionJobVertex> triggerVertices =
                    idToVertex(snapshotSettings.getVerticesToTrigger(), executionGraph);

            List<ExecutionJobVertex> ackVertices =
                    idToVertex(snapshotSettings.getVerticesToAcknowledge(), executionGraph);

            List<ExecutionJobVertex> confirmVertices =
                    idToVertex(snapshotSettings.getVerticesToConfirm(), executionGraph);

            CompletedCheckpointStore completedCheckpoints;
            CheckpointIDCounter checkpointIdCounter;
            try {
                // clouding 注释: 2022/6/25 19:26
                //          获取checkpoint保留的个数
                int maxNumberOfCheckpointsToRetain =
                        jobManagerConfig.getInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS);

                if (maxNumberOfCheckpointsToRetain <= 0) {
                    // warning and use 1 as the default value if the setting in
                    // state.checkpoints.max-retained-checkpoints is not greater than 0.
                    log.warn(
                            "The setting for '{} : {}' is invalid. Using default value of {}",
                            CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key(),
                            maxNumberOfCheckpointsToRetain,
                            CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue());

                    maxNumberOfCheckpointsToRetain =
                            CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue();
                }

                completedCheckpoints =
                        recoveryFactory.createCheckpointStore(
                                jobId, maxNumberOfCheckpointsToRetain, classLoader);
                checkpointIdCounter = recoveryFactory.createCheckpointIDCounter(jobId);
            } catch (Exception e) {
                throw new JobExecutionException(
                        jobId, "Failed to initialize high-availability checkpoint handler", e);
            }

            // Maximum number of remembered checkpoints
            // clouding 注释: 2022/6/25 19:26
            //          获取历史记录checkpoint最大的数量
            int historySize = jobManagerConfig.getInteger(WebOptions.CHECKPOINTS_HISTORY_SIZE);

            CheckpointStatsTracker checkpointStatsTracker =
                    new CheckpointStatsTracker(
                            historySize,
                            ackVertices,
                            snapshotSettings.getCheckpointCoordinatorConfiguration(),
                            metrics);

            // load the state backend from the application settings
            // clouding 注释: 2022/6/25 20:16
            //          state backend的配置
            final StateBackend applicationConfiguredBackend;
            final SerializedValue<StateBackend> serializedAppConfigured =
                    snapshotSettings.getDefaultStateBackend();

            if (serializedAppConfigured == null) {
                applicationConfiguredBackend = null;
            } else {
                try {
                    applicationConfiguredBackend =
                            serializedAppConfigured.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(
                            jobId, "Could not deserialize application-defined state backend.", e);
                }
            }

            final StateBackend rootBackend;
            try {
                rootBackend =
                        StateBackendLoader.fromApplicationOrConfigOrDefault(
                                applicationConfiguredBackend, jobManagerConfig, classLoader, log);
            } catch (IllegalConfigurationException | IOException | DynamicCodeLoadingException e) {
                throw new JobExecutionException(
                        jobId, "Could not instantiate configured state backend", e);
            }

            // instantiate the user-defined checkpoint hooks

            // clouding 注释: 2022/6/25 20:17
            //          实例化用户checkpoint的钩子,在触发或者恢复快照时执行
            final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks =
                    snapshotSettings.getMasterHooks();
            final List<MasterTriggerRestoreHook<?>> hooks;

            if (serializedHooks == null) {
                hooks = Collections.emptyList();
            } else {
                final MasterTriggerRestoreHook.Factory[] hookFactories;
                try {
                    hookFactories = serializedHooks.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(
                            jobId, "Could not instantiate user-defined checkpoint hooks", e);
                }

                final Thread thread = Thread.currentThread();
                final ClassLoader originalClassLoader = thread.getContextClassLoader();
                thread.setContextClassLoader(classLoader);

                try {
                    hooks = new ArrayList<>(hookFactories.length);
                    for (MasterTriggerRestoreHook.Factory factory : hookFactories) {
                        hooks.add(MasterHooks.wrapHook(factory.create(), classLoader));
                    }
                } finally {
                    thread.setContextClassLoader(originalClassLoader);
                }
            }

            final CheckpointCoordinatorConfiguration chkConfig =
                    snapshotSettings.getCheckpointCoordinatorConfiguration();

            // clouding 注释: 2021/10/16 16:56
            //          这里开启了 CheckpointCoordinator
            executionGraph.enableCheckpointing(
                    chkConfig,
                    triggerVertices,
                    ackVertices,
                    confirmVertices,
                    hooks,
                    checkpointIdCounter,
                    completedCheckpoints,
                    rootBackend,
                    checkpointStatsTracker);
        }

        // create all the metrics for the Execution Graph

        // clouding 注释: 2022/6/25 20:46
        //          监控任务的metric
        metrics.gauge(RestartTimeGauge.METRIC_NAME, new RestartTimeGauge(executionGraph));
        metrics.gauge(DownTimeGauge.METRIC_NAME, new DownTimeGauge(executionGraph));
        metrics.gauge(UpTimeGauge.METRIC_NAME, new UpTimeGauge(executionGraph));

        executionGraph.getFailoverStrategy().registerMetrics(metrics);

        return executionGraph;
    }

    private static List<ExecutionJobVertex> idToVertex(
            List<JobVertexID> jobVertices, ExecutionGraph executionGraph)
            throws IllegalArgumentException {

        List<ExecutionJobVertex> result = new ArrayList<>(jobVertices.size());

        for (JobVertexID id : jobVertices) {
            ExecutionJobVertex vertex = executionGraph.getJobVertex(id);
            if (vertex != null) {
                result.add(vertex);
            } else {
                throw new IllegalArgumentException(
                        "The snapshot checkpointing settings refer to non-existent vertex " + id);
            }
        }

        return result;
    }

    // ------------------------------------------------------------------------

    /** This class is not supposed to be instantiated. */
    private ExecutionGraphBuilder() {}
}
