package com.vmware.xenon.services.common;

import com.vmware.xenon.common.NodeSelectorService;
import com.vmware.xenon.common.NodeSelectorState;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationContext;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

/**
 * task scheduler
 */
public class TaskScheduleService extends StatelessService {

    private TaskService.TaskServiceState taskTemplate;
    private String taskFactoryLink;
    private String nodeSelectorLink = ServiceUriPaths.DEFAULT_NODE_SELECTOR;

    public TaskScheduleService(TaskService.TaskServiceState taskTemplate,
                               String taskFactoryLink, long period) {
        super();
        toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        this.setMaintenanceIntervalMicros(period);
        this.taskTemplate = taskTemplate;
        this.taskFactoryLink = taskFactoryLink;
    }

    @Override
    public void handlePeriodicMaintenance(Operation maintOp) {

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
                    logInfo("task %s in progress", s.documentSelfLink);
                    maintOp.complete();
                }).sendWith(this);
    }
}


