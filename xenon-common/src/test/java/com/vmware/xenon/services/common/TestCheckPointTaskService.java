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

import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.logging.Level;

import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestCheckPointTaskService extends BasicTestCase {

    private static final String EXAMPLE_SYNCH_CHECKPOINT_SELF_LINK = ServiceUriPaths.CHECKPOINT + "/example";
    private Comparator<ExampleServiceState> documentComparator = (d0, d1) -> {
        if (d0.documentUpdateTimeMicros > d1.documentUpdateTimeMicros) {
            return 1;
        }
        return -1;
    };
    
    public long serviceCount = 10;
    public int updateCount = 1;
    public int nodeCount = 3;
    public int iterationCount = 1;
    public long checkPointLag = TimeUnit.MILLISECONDS.toMicros(10);
    public int checkPointScanWindowSize = 1000;
    public String exampleFactoryLink = ExampleService.FACTORY_LINK;
    public Class<? extends ServiceDocument> exampleKind = ExampleServiceState.class;

    public void setUp(int nodeCount) throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        if (this.host.getInProcessHostMap().isEmpty()) {
            this.host.setStressTest(this.host.isStressTest);
            this.host.setPeerSynchronizationEnabled(true);
            this.host.setUpPeerHosts(nodeCount);
            this.host.joinNodesAndVerifyConvergence(nodeCount, true);
            this.host.setNodeGroupQuorum(nodeCount);

            for (VerificationHost host : this.host.getInProcessHostMap().values()) {
                host.startFactory(new CheckPointService());
                host.waitForServiceAvailable(CheckPointService.FACTORY_LINK);
                CheckPointService.CheckPointState s
                        = new CheckPointService.CheckPointState();
                s.checkPoint = 0L;
                s.documentSelfLink = EXAMPLE_SYNCH_CHECKPOINT_SELF_LINK;
                Operation post = Operation.createPost(UriUtils.buildUri(host, ServiceUriPaths.CHECKPOINT))
                        .setBody(s);
                host.getTestRequestSender().sendAndWait(post);
                host.waitForServiceAvailable(EXAMPLE_SYNCH_CHECKPOINT_SELF_LINK);
                host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
            }
        }
    }

    /**
     * update check point by manually triggered check point task
     * @throws Throwable
     */
    @Test
    public void verifyCheckPointConvergence() throws Throwable {
        setUp(this.nodeCount);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        this.host.waitForNodeGroupConvergence();

        // create example services
        List<ExampleServiceState> exampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, this.exampleFactoryLink);
        ExampleService.ExampleServiceState lastUpdateExampleState;

        for (int i = 0; i < this.updateCount; i++) {
            // update
            updateExampleServices(exampleStates);
            lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
            // check whether check point catch up the latest update
            updateAndVerifyCheckPoints(lastUpdateExampleState.documentUpdateTimeMicros);
        }

        for (int i = 0; i < this.updateCount; i++) {
            // update
            updateExampleServices(exampleStates);
        }

        lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
        updateAndVerifyCheckPoints(lastUpdateExampleState.documentUpdateTimeMicros);
    }

    /**
     * trigger a check point task, update check point
     * and wait for check point converge as expected value
     * @param expectedCheckPoint
     * @throws Throwable
     */
    private void updateAndVerifyCheckPoints(long expectedCheckPoint) throws Throwable {
        this.host.waitFor("check point convergence timeout", () -> {
            startAndWaitCheckPointTaskService();
            Long actualCheckPoint = checkPointsConverged(EXAMPLE_SYNCH_CHECKPOINT_SELF_LINK);
            if (actualCheckPoint == null) {
                // non unique check point
                return false;
            }
            if (actualCheckPoint != expectedCheckPoint) {
                this.host.log(Level.INFO,"Expected check point %d\nActual check point %d", expectedCheckPoint, actualCheckPoint);
                return false;
            }
            return true;
        });
    }

    private CheckPointTaskService.State validateTaskState() {
        CheckPointTaskService.State s = new CheckPointTaskService.State();
        s.factoryLink = this.exampleFactoryLink;
        s.kind = Utils.buildKind(this.exampleKind);
        s.checkPointServiceLink = EXAMPLE_SYNCH_CHECKPOINT_SELF_LINK;
        s.nodeSelectorLink = ServiceUriPaths.DEFAULT_NODE_SELECTOR;
        s.checkPointLag = this.checkPointLag;
        s.checkPointScanWindowSize = this.checkPointScanWindowSize;
        return s;
    }

    /**
     * start a check point task
     */
    private void startAndWaitCheckPointTaskService() {
        CheckPointTaskService.State state = validateTaskState();
        VerificationHost host =
                this.host.getInProcessHostMap().values().stream().filter(h -> !h.isStopping()).iterator().next();
        Operation patch = Operation.createPatch(host, CheckPointTaskService.SELF_LINK)
                .setBody(state);
        this.host.getTestRequestSender().sendAndWait(patch, CheckPointTaskService.State.class);
    }

    /**
     * check point converge and match expected check point
     * @param expectedCheckPoint
     * @throws Throwable
     */
    private void verifyCheckPoints(Long expectedCheckPoint) throws Throwable {
        this.host.waitFor("check point convergence timeout", () -> {
            Long actualCheckPoint = checkPointsConverged(EXAMPLE_SYNCH_CHECKPOINT_SELF_LINK);
            if (actualCheckPoint == null) {
                // non unique check point
                return false;
            }
            if (!actualCheckPoint.equals(expectedCheckPoint)) {
                this.host.log(Level.INFO,"Expected check point %d\nActual check point %d", expectedCheckPoint, actualCheckPoint);
                return false;
            }
            return true;
        });
    }

    /**
     * check point convergence across peers
     * @param checkPointServiceLink
     * @return
     */
    private Long checkPointsConverged(String checkPointServiceLink) {
        Set<Long> checkPoints = new HashSet<>();
        List<Operation> ops = new ArrayList<>();
        for (ServiceHost h : this.host.getInProcessHostMap().values()) {
            ops.add(Operation.createGet(UriUtils.buildUri(h, checkPointServiceLink)));
        }
        List<CheckPointService.CheckPointState> states =
                this.host.getTestRequestSender().sendAndWait(ops, CheckPointService.CheckPointState.class);
        states.stream().forEach(s -> checkPoints.add(s.checkPoint));
        if (checkPoints.size() > 1) {
            this.host.log(Level.INFO, "check point not converged %s",
                    Utils.toJson(checkPoints));
            return null;
        }
        return checkPoints.iterator().next();
    }

    private void updateExampleServices(List<ExampleServiceState> exampleStates) {
        List<Operation> ops = new ArrayList<>();
        for (ExampleServiceState st : exampleStates) {
            ExampleServiceState s = new ExampleServiceState();
            s.counter =  ++st.counter;
            URI serviceUri = UriUtils.buildUri(this.host.getPeerHost(), st.documentSelfLink);
            Operation patch = Operation.createPatch(serviceUri).setBody(s);
            ops.add(patch);
        }
        this.host.getTestRequestSender().sendAndWait(ops);
    }

    private List<ExampleServiceState> getExampleServices(List<ExampleServiceState> exampleStates) {
        List<Operation> ops = new ArrayList<>();
        for (ExampleServiceState st : exampleStates) {
            URI serviceUri = UriUtils.buildUri(this.host.getPeerHost(), st.documentSelfLink);
            Operation get = Operation.createGet(serviceUri);
            ops.add(get);
        }
        return this.host.getTestRequestSender().sendAndWait(ops, ExampleServiceState.class);
    }
}
