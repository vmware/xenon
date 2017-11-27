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
import java.util.logging.Level;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.SynchronizationTaskService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.config.TestXenonConfiguration;
import com.vmware.xenon.common.test.VerificationHost;

public class TestCheckPointService extends BasicTestCase {

    private long checkPoint = 0;
    private long checkPointVersion = 0;
    private static final String TEST_CHECKPOINT_SELF_LINK = UriUtils.buildUriPath(
            CheckPointService.FACTORY_LINK,
            "test");

    private static final String EXAMPLE_CHECKPOINT_SELF_LINK = UriUtils.buildUriPath(
            CheckPointService.FACTORY_LINK,
            UriUtils.convertPathCharsFromLink(ExampleService.FACTORY_LINK));
    private Comparator<ExampleService.ExampleServiceState> documentComparator = (d0, d1) -> {
        if (d0.documentUpdateTimeMicros > d1.documentUpdateTimeMicros) {
            return 1;
        }
        return -1;
    };

    public long serviceCount = 10;
    public int updateCount = 1;
    public int nodeCount = 3;
    public int iterationCount = 1;
    public String exampleFactoryLink = ExampleService.FACTORY_LINK;

    @BeforeClass
    public static void setUpClass() throws Exception {
        TestXenonConfiguration.override(
                SynchronizationTaskService.class,
                "isCheckPointEnabled",
                "true"
        );
        TestXenonConfiguration.override(
                SynchronizationTaskService.class,
                "checkPointPeriod",
                String.valueOf(TimeUnit.MILLISECONDS.toMicros(1000))
        );
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        TestXenonConfiguration.restore();
    }

    public void setUp(int nodeCount) throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        if (this.host.getInProcessHostMap().isEmpty()) {
            this.host.setStressTest(this.host.isStressTest);
            this.host.setPeerSynchronizationEnabled(true);
            this.host.setUpPeerHosts(nodeCount);
            this.host.joinNodesAndVerifyConvergence(nodeCount, true);
            this.host.setNodeGroupQuorum(nodeCount);

            for (VerificationHost host : this.host.getInProcessHostMap().values()) {
                CheckPointService.CheckPointState s = new CheckPointService.CheckPointState();
                s.checkPoint = 0L;
                s.documentSelfLink = TEST_CHECKPOINT_SELF_LINK;
                Operation op = Operation.createPost(host, CheckPointService.FACTORY_LINK)
                        .setBody(s);
                this.host.getTestRequestSender().sendAndWait(op);
                host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
            }
        }
    }

    @Test
    public void testCheckPointServiceRestart() throws Throwable {
        setUp(1);
        this.host.waitForNodeGroupIsAvailableConvergence();
        VerificationHost h0 = this.host.getPeerHost();

        Operation get = Operation.createGet(UriUtils.buildUri(h0, TEST_CHECKPOINT_SELF_LINK));
        CheckPointService.CheckPointState s0 =
                this.host.getTestRequestSender().sendAndWait(get, CheckPointService.CheckPointState.class);
        Assert.assertEquals(this.checkPointVersion, s0.documentVersion);
        Assert.assertEquals(this.checkPoint, s0.checkPoint.longValue());
        // stop then restart
        this.host.stopHostAndPreserveState(h0);
        h0.setPort(0);
        VerificationHost.restartStatefulHost(h0, false);
        this.host.addPeerNode(h0);
        this.host.waitForNodeGroupIsAvailableConvergence();

        // patch, start service on demand
        CheckPointService.CheckPointState s1 = new CheckPointService.CheckPointState();
        s1.documentSelfLink = TEST_CHECKPOINT_SELF_LINK;
        s1.checkPoint = ++this.checkPoint;
        ++this.checkPointVersion;
        Operation patch = Operation.createPatch(UriUtils.buildUri(h0, TEST_CHECKPOINT_SELF_LINK))
                .setBody(s1);
        this.host.getTestRequestSender().sendAndWait(patch);

        get = Operation.createGet(UriUtils.buildUri(h0, TEST_CHECKPOINT_SELF_LINK));
        s0 = this.host.getTestRequestSender().sendAndWait(get, CheckPointService.CheckPointState.class);
        Assert.assertEquals(this.checkPointVersion, s0.documentVersion);
        Assert.assertEquals(this.checkPoint, s0.checkPoint.longValue());
    }

    @Test
    public void testCheckPointServiceUpdate() throws Throwable {
        setUp(1);
        this.host.waitForNodeGroupIsAvailableConvergence();
        VerificationHost h0 = this.host.getPeerHost();

        CheckPointService.CheckPointState state = new CheckPointService.CheckPointState();
        long oldCheckPoint = this.checkPoint++;
        state.checkPoint = oldCheckPoint;
        // update with oldCheckPoint, expect the same version and checkpoint
        Operation patch = Operation.createPatch(UriUtils.buildUri(h0, TEST_CHECKPOINT_SELF_LINK))
                .setBody(state);
        this.host.getTestRequestSender().sendAndWait(patch);
        Operation get = Operation.createGet(UriUtils.buildUri(h0, TEST_CHECKPOINT_SELF_LINK));
        CheckPointService.CheckPointState s0 =
                this.host.getTestRequestSender().sendAndWait(get, CheckPointService.CheckPointState.class);
        Assert.assertEquals(0L, s0.documentVersion);
        Assert.assertEquals(0L, s0.checkPoint.longValue());

        // update with advanced checkpoint, expect updated version and checkpoint
        state.checkPoint = this.checkPoint;
        patch = Operation.createPatch(UriUtils.buildUri(h0, TEST_CHECKPOINT_SELF_LINK))
                .setBody(state);
        this.host.getTestRequestSender().sendAndWait(patch);
        get = Operation.createGet(UriUtils.buildUri(h0, TEST_CHECKPOINT_SELF_LINK));
        s0 = this.host.getTestRequestSender().sendAndWait(get, CheckPointService.CheckPointState.class);
        Assert.assertEquals(1L, s0.documentVersion);
        Assert.assertEquals(this.checkPoint, s0.checkPoint.longValue());
    }

    @Test
    public void testCheckPointServiceMultiNode() throws Throwable {
        int nodeCount = 3;
        setUp(nodeCount);
        this.host.setNodeGroupQuorum(nodeCount - 1);
        this.host.waitForNodeGroupConvergence();
        VerificationHost h0 = this.host.getPeerHost();
        CheckPointService.CheckPointState state = new CheckPointService.CheckPointState();
        long oldCheckPoint = this.checkPoint++;
        state.checkPoint = this.checkPoint;
        // update local check point of h0, no propagation to {h1, h2}
        Operation patch = Operation.createPatch(UriUtils.buildUri(h0, TEST_CHECKPOINT_SELF_LINK))
                .setBody(state);
        this.host.getTestRequestSender().sendAndWait(patch);

        for (ServiceHost h : this.host.getInProcessHostMap().values()) {
            Operation op = Operation.createGet(UriUtils.buildUri(h, TEST_CHECKPOINT_SELF_LINK));
            CheckPointService.CheckPointState s = this.host.getTestRequestSender().sendAndWait(op, CheckPointService.CheckPointState.class);
            Assert.assertEquals(h.getId() == h0.getId() ? this.checkPoint : oldCheckPoint, s.checkPoint.longValue());
        }

        // stop h0 and preserve checkpoint state
        this.host.stopHostAndPreserveState(h0);
        this.host.waitForNodeUnavailable(ServiceUriPaths.DEFAULT_NODE_GROUP,
                this.host.getInProcessHostMap().values(), h0);
        this.host.waitForNodeGroupConvergence();

        // update checkpoint within {h1, h2}
        oldCheckPoint = this.checkPoint++;
        for (ServiceHost h : this.host.getInProcessHostMap().values()) {
            CheckPointService.CheckPointState s = new CheckPointService.CheckPointState();
            s.checkPoint = this.checkPoint;
            Operation op = Operation.createPatch(UriUtils.buildUri(h, TEST_CHECKPOINT_SELF_LINK))
                    .setBody(s);
            this.host.getTestRequestSender().sendAndWait(op);
        }
        // restart h0
        h0.start();
        //h0.startFactory(new CheckPointService());
        //h0.waitForServiceAvailable(CheckPointService.FACTORY_LINK);
        h0.waitForServiceAvailable(ExampleService.FACTORY_LINK);

        this.host.addPeerNode(h0);
        this.host.waitForNodeGroupConvergence();

        for (ServiceHost h : this.host.getInProcessHostMap().values()) {
            Operation op = Operation.createGet(UriUtils.buildUri(h, TEST_CHECKPOINT_SELF_LINK));
            CheckPointService.CheckPointState s =
                    this.host.getTestRequestSender().sendAndWait(op, CheckPointService.CheckPointState.class);
            Assert.assertEquals(h.getId() != h0.getId() ? this.checkPoint : oldCheckPoint, s.checkPoint.longValue());
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
        List<ExampleService.ExampleServiceState> exampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, this.exampleFactoryLink);
        ExampleService.ExampleServiceState lastUpdateExampleState;

        for (int i = 0; i < this.updateCount; i++) {
            // update
            updateExampleServices(exampleStates);
            lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
            verifyCheckPoints(lastUpdateExampleState.documentUpdateTimeMicros);
        }

        for (int i = 0; i < this.updateCount; i++) {
            // update
            updateExampleServices(exampleStates);
        }

        lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
        verifyCheckPoints(lastUpdateExampleState.documentUpdateTimeMicros);
    }

    /**
     * check point converge and match expected check point
     * @param expectedCheckPoint
     * @throws Throwable
     */
    private void verifyCheckPoints(Long expectedCheckPoint) throws Throwable {
        this.host.waitFor("check point convergence timeout", () -> {
            Long actualCheckPoint = checkPointsConverged(EXAMPLE_CHECKPOINT_SELF_LINK);
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

    private void updateExampleServices(List<ExampleService.ExampleServiceState> exampleStates) {
        List<Operation> ops = new ArrayList<>();
        for (ExampleService.ExampleServiceState st : exampleStates) {
            ExampleService.ExampleServiceState s = new ExampleService.ExampleServiceState();
            s.counter =  ++st.counter;
            URI serviceUri = UriUtils.buildUri(this.host.getPeerHost(), st.documentSelfLink);
            Operation patch = Operation.createPatch(serviceUri).setBody(s);
            ops.add(patch);
        }
        this.host.getTestRequestSender().sendAndWait(ops);
    }

    private List<ExampleService.ExampleServiceState> getExampleServices(List<ExampleService.ExampleServiceState> exampleStates) {
        List<Operation> ops = new ArrayList<>();
        for (ExampleService.ExampleServiceState st : exampleStates) {
            URI serviceUri = UriUtils.buildUri(this.host.getPeerHost(), st.documentSelfLink);
            Operation get = Operation.createGet(serviceUri);
            ops.add(get);
        }
        return this.host.getTestRequestSender().sendAndWait(ops, ExampleService.ExampleServiceState.class);
    }

    @After
    public void cleanUp() throws Throwable {
        this.host.tearDownInProcessPeers();
        this.host.tearDown();
    }
}