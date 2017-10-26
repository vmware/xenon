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

import com.vmware.xenon.common.NodeSelectorService;
import com.vmware.xenon.common.NodeSelectorState;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationContext;
import com.vmware.xenon.common.ServiceSubscriptionState;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * task scheduler
 */
public class TaskScheduleService extends StatelessService {

    private TaskService.TaskServiceState taskTemplate;
    private String taskFactoryLink;
    private String nodeSelectorLink = ServiceUriPaths.DEFAULT_NODE_SELECTOR;
    private int maxInProgress = 1;
    private Map<String, TaskService.TaskServiceState> tasks = new HashMap<>();

    public TaskScheduleService(TaskService.TaskServiceState taskTemplate,
                               String taskFactoryLink, long period) {
        super();
        toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        this.setMaintenanceIntervalMicros(period);
        this.taskTemplate = taskTemplate;
        this.taskFactoryLink = taskFactoryLink;
    }

    @Override
    public void handleRequest(Operation op) {
        if (!op.isNotification()) {
            super.handleRequest(op);
            return;
        }
        handleTaskNotification(op);
    }

    @Override
    public void handlePeriodicMaintenance(Operation maintOp) {
        if (!handleTasksInProgress()) {
            maintOp.complete();
            return;
        }
        Operation.CompletionHandler ch = (op, ex) -> {
            if (ex != null) {
                maintOp.fail(ex);
                return;
            }
            NodeSelectorState nss = op.getBody(NodeSelectorState.class);
            if (nss.status != NodeSelectorState.Status.AVAILABLE) {
                logInfo("node selector %s unavailable", this.nodeSelectorLink);
                maintOp.complete();
            }

            OperationContext opContext = OperationContext.getOperationContext();
            // Only one node is responsible for task scheduling.
            // Ask the runtime if this is the owner node, using the self link as the key.
            Operation selectOwnerOp = maintOp.clone().setExpiration(Utils.fromNowMicrosUtc(
                    getHost().getOperationTimeoutMicros()));
            selectOwnerOp.setCompletion((o, e) -> {
                OperationContext.restoreOperationContext(opContext);
                if (e != null) {
                    logWarning("owner selection failed: %s", e.toString());
                    maintOp.fail(e);
                    return;
                }
                NodeSelectorService.SelectOwnerResponse rsp = o.getBody(NodeSelectorService.SelectOwnerResponse.class);
                if (!rsp.isLocalHostOwner) {
                    // We do not need to do anything
                    maintOp.complete();
                    return;
                }
                logInfo("%s elected as owner for task %s. Start scheduling ...",
                        getHost().getId(), this.taskFactoryLink);
                scheduleTaskAsOwner(maintOp);
            });

            getHost().selectOwner(this.nodeSelectorLink, this.getSelfLink(), selectOwnerOp);
        };

        Operation.createGet(UriUtils.buildUri(getHost(), this.nodeSelectorLink))
                .setCompletion(ch)
                .sendWith(this);

    }

    /**
     * return true if more tasks could be scheduled
     */
    private boolean handleTasksInProgress() {
        synchronized (this) {
            Iterator<Map.Entry<String, TaskService.TaskServiceState>> taskItr = this.tasks.entrySet().iterator();
            while (taskItr.hasNext()) {
                TaskService.TaskServiceState task = taskItr.next().getValue();
                Long expiration = task.documentExpirationTimeMicros;
                if (expiration < Utils.getNowMicrosUtc()) {
                    taskItr.remove();
                }
            }
            return this.tasks.size() < this.maxInProgress;
        }
    }

    private void scheduleTaskAsOwner(Operation maintOp) {
        Operation.createPost(getHost(), this.taskFactoryLink)
                .setBody(this.taskTemplate)
                .setReferer(getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        logInfo("Failed to create task");
                        maintOp.fail(e);
                        return;
                    }
                    TaskService.TaskServiceState s = o.getBody(this.taskTemplate.getClass());
                    logInfo("task %s created", s.documentSelfLink);
                    synchronized (this) {
                        this.tasks.put(s.documentSelfLink, s);
                    }
                    maintOp.complete();
                    subscribeTask(s.documentSelfLink);
                }).sendWith(this);
    }

    private void subscribeTask(String taskSelfLink) {
        Operation post = Operation
                .createPost(getHost(), taskSelfLink)
                .setReferer(getHost().getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        logInfo("subscribe to %s failed", taskSelfLink);
                    }
                });

        ServiceSubscriptionState.ServiceSubscriber sr = ServiceSubscriptionState.ServiceSubscriber
                .create(true)
                .setUsePublicUri(true)
                .setSubscriberReference(this.getUri());

        // Create subscription service with processResults as callback to process the results.
        getHost().startSubscriptionService(post, this, sr);
    }

    private void handleTaskNotification(Operation notifOp) {
        notifOp.complete();
        if (!notifOp.hasBody()) {
            return;
        }
        TaskService.TaskServiceState ts = notifOp.getBody(this.taskTemplate.getClass());
        if (ts == null) {
            return;
        }
        logInfo("%s %s", ts.documentSelfLink, ts.taskInfo.stage);
        // System.out.println(Utils.toJsonHtml(ts));
        synchronized (this) {
            if (!this.tasks.containsKey(ts.documentSelfLink)) {
                // expired
                return;
            }
            if (!TaskState.isInProgress(ts.taskInfo)) {
                // task done
                this.tasks.remove(ts.documentSelfLink);
                return;
            }
            // update task status
            this.tasks.put(ts.documentSelfLink, ts);
        }
    }
}


