/*
 * Copyright (c) 2014-2015 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.vmware.xenon.services.common;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.TaskState;

/**
 * Service document describing a multi-stage query task used for traversing a
 * document graph
 */
public class GraphQueryTask extends TaskService.TaskServiceState {

    public enum GraphQueryOption {
        /**
         * Removes document links from stage N-1, if the link did not contribute to a result
         * in stage N. For example, if in stage zero we find two documents, one (doc A) that has
         * a link that produces a result in stage one, and another that does not (doc B), we
         * will remove the result document B from stage zero.
         * The logic is applied recursively starting at the final stage, and can result in
         * modifications across all stages (except the final one)
         */
        FILTER_STAGE_RESULTS
    }

    public EnumSet<GraphQueryOption> options = EnumSet.noneOf(GraphQueryOption.class);

    /**
     * Specifies a sequence of query specifications that select the graph nodes
     * serving as the origin of the graph search, at a given depth/stage in the query.
     * The query returns zero or more documents serving as the origin nodes for the
     * next stage of the graph traversal.
     */
    public List<QueryTask> stages;

    /**
     * A list of authorization context links which can access this service.
     */
    public Set<String> tenantLinks;

    /**
     * Links to the query task service instances with results for each query stage. The
     * list tracks the query task link for a given {@link GraphQueryTask#currentDepth} value.
     */
    @PropertyOptions(usage = { PropertyUsageOption.SERVICE_USE })
    public List<String> resultLinks = new ArrayList<>();

    @PropertyOptions(usage = {
            PropertyUsageOption.SINGLE_ASSIGNMENT,
            PropertyUsageOption.SERVICE_USE })
    public int depthLimit;

    @PropertyOptions(usage = {
            PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL,
            PropertyUsageOption.SERVICE_USE })
    public int currentDepth;

    /**
     * Builder class for constructing {@linkplain com.vmware.xenon.services.common.QueryTask query tasks}.
     */
    public static class Builder {
        private final GraphQueryTask task;

        private Builder(int depthLimit) {
            this.task = new GraphQueryTask();
            this.task.taskInfo = new TaskState();
            this.task.stages = new ArrayList<>();
            this.task.depthLimit = depthLimit;
        }

        /**
         * Constructs an asynchronous query task.
         * @param depthLimit the number of stages in the query
         * @return a reference to this object
         */
        public static Builder create(int depthLimit) {
            return new Builder(depthLimit);
        }

        public Builder addOption(GraphQueryOption option) {
            this.task.options.add(option);
            return this;
        }

        public Builder setDirect(boolean isDirect) {
            this.task.taskInfo.isDirect = isDirect;
            return this;
        }

        /**
         * Sets the query specification for the given stage index
         * @param querySpec the query specification to use for the specified stage/depth
         * @return a reference to this object
         */
        public Builder setQueryStage(int depthIndex, QueryTask query) {
            this.task.stages.set(depthIndex, query);
            return this;
        }

        /**
         * Adds a query specification in the query stages
         * @param queryTask the query task with the specification to use for this stage
         * @return a reference to this object
         */
        public Builder addQueryStage(QueryTask queryTask) {
            this.task.stages.add(queryTask);
            return this;
        }

        /**
         * Sets the maximum number of "hops", or stage results, to execute. Its independent of
         * of the number of {@link GraphQueryTask#stages} since we can keep executing the last
         * stage N times.
         * @param depthLimit the depth limit.
         * @return a reference to this object
         */
        public Builder setDepthLimit(int depthLimit) {
            this.task.depthLimit = depthLimit;
            return this;
        }

        /**
         * Return the constructed {@link com.vmware.xenon.services.common.GraphQueryTask} object.
         * @return the graph query task object.
         */
        public GraphQueryTask build() {
            return this.task;
        }
    }
}