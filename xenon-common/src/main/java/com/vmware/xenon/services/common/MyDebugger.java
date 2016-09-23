/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

import com.vmware.xenon.common.RoundRobinOperationQueue;

public class MyDebugger {

    private static RoundRobinOperationQueue QUEUE_FOR_DOCUMENT;
    private static RoundRobinOperationQueue QUEUE_FOR_OPERATION;

    public static RoundRobinOperationQueue getQueueForDocument() {
        return QUEUE_FOR_DOCUMENT;
    }

    public static void setQueueForDocument(RoundRobinOperationQueue queueForDocument) {
        QUEUE_FOR_DOCUMENT = queueForDocument;
    }

    public static RoundRobinOperationQueue getQueueForOperation() {
        return QUEUE_FOR_OPERATION;
    }

    public static void setQueueForOperation(RoundRobinOperationQueue queueForOperation) {
        QUEUE_FOR_OPERATION = queueForOperation;
    }

    public static synchronized String getQueueStat() {
        StringBuilder sb = new StringBuilder();
        sb.append("{[docQ=");
        if (QUEUE_FOR_DOCUMENT == null) {
            sb.append("NULL");
        } else {
            sb.append(QUEUE_FOR_DOCUMENT.getState());
        }

        sb.append("], opQ=[");
        if (QUEUE_FOR_OPERATION == null) {
            sb.append("NULL");
        } else {
            sb.append(QUEUE_FOR_OPERATION.getState());
        }
        sb.append("]}");
        return sb.toString();
    }
}
