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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.planner.utils.ExecutorUtils;
import org.apache.flink.util.StringUtils;

import java.util.List;

/** Default implementation of {@link Executor}. */
@Internal
public class DefaultExecutor implements Executor {

    private static final String DEFAULT_JOB_NAME = "Flink Exec Table Job";

    private final StreamExecutionEnvironment executionEnvironment;

    public DefaultExecutor(StreamExecutionEnvironment executionEnvironment) {
        this.executionEnvironment = executionEnvironment;
    }

    public StreamExecutionEnvironment getExecutionEnvironment() {
        return executionEnvironment;
    }

    @Override
    public ReadableConfig getConfiguration() {
        return executionEnvironment.getConfiguration();
    }

    @Override
    public Pipeline createPipeline(
            List<Transformation<?>> transformations,
            ReadableConfig configuration,
            String defaultJobName) {

        // reconfigure before a stream graph is generated
        executionEnvironment.configure(configuration);

        // create stream graph
        final RuntimeExecutionMode mode = configuration.get(ExecutionOptions.RUNTIME_MODE);
        final StreamGraph graph;
        switch (mode) {
            case BATCH:
                graph = createBatchGraph(transformations, configuration);
                break;
            case STREAMING:
                graph = createStreamingGraph(transformations);
                break;
            case AUTOMATIC:
            default:
                throw new TableException(String.format("Unsupported runtime mode: %s", mode));
        }
        setJobName(graph, defaultJobName);
        return graph;
    }

    @Override
    public JobExecutionResult execute(Pipeline pipeline) throws Exception {
        return executionEnvironment.execute((StreamGraph) pipeline);
    }

    @Override
    public JobClient executeAsync(Pipeline pipeline) throws Exception {
        return executionEnvironment.executeAsync((StreamGraph) pipeline);
    }

    private StreamGraph createBatchGraph(
            List<Transformation<?>> transformations, ReadableConfig configuration) {
        ExecutorUtils.setBatchProperties(executionEnvironment);
        StreamGraph graph =
                ExecutorUtils.generateStreamGraph(executionEnvironment, transformations);
        ExecutorUtils.setBatchProperties(graph, configuration);
        return graph;
    }

    private StreamGraph createStreamingGraph(List<Transformation<?>> transformations) {
        return ExecutorUtils.generateStreamGraph(executionEnvironment, transformations);
    }

    private void setJobName(StreamGraph streamGraph, String defaultJobName) {
        final String adjustedDefaultJobName =
                StringUtils.isNullOrWhitespaceOnly(defaultJobName)
                        ? DEFAULT_JOB_NAME
                        : defaultJobName;
        final String jobName =
                getConfiguration().getOptional(PipelineOptions.NAME).orElse(adjustedDefaultJobName);
        streamGraph.setJobName(jobName);
    }
}
