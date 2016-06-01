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

import java.util.List;

import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.services.common.GraphQueryTaskService.QueryStageSpec;

public class GraphQueryTask extends TaskService.TaskServiceState {
    /**
     * Specifies a sequence of query specifications that select the graph nodes
     * serving as the origin of the graph search, at a given depth/stage in the query.
     * The query returns zero or more documents serving as the origin nodes for the
     * next stage of the graph traversal.
     */
    @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
    public List<QueryStageSpec> traversalSpecs;

    /**
     * Links to the query task service instances with results for each query stage
     */
    @PropertyOptions(usage = {
            PropertyUsageOption.LINK,
            PropertyUsageOption.SERVICE_USE })
    public List<String> resultLinks;

    @UsageOption(option = PropertyUsageOption.SINGLE_ASSIGNMENT)
    public int depthLimit;
    @UsageOption(option = PropertyUsageOption.SERVICE_USE)
    public int currentDepth;
}