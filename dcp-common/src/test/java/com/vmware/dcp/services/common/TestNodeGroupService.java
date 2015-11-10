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

package com.vmware.dcp.services.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.dcp.common.CommandLineArgumentParser;
import com.vmware.dcp.common.FactoryService;
import com.vmware.dcp.common.NodeSelectorService.SelectAndForwardRequest;
import com.vmware.dcp.common.NodeSelectorService.SelectOwnerResponse;
import com.vmware.dcp.common.NodeSelectorState;
import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.Operation.CompletionHandler;
import com.vmware.dcp.common.OperationJoin;
import com.vmware.dcp.common.Service;
import com.vmware.dcp.common.Service.Action;
import com.vmware.dcp.common.Service.ProcessingStage;
import com.vmware.dcp.common.Service.ServiceOption;
import com.vmware.dcp.common.ServiceConfigUpdateRequest;
import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.ServiceDocumentDescription;
import com.vmware.dcp.common.ServiceDocumentQueryResult;
import com.vmware.dcp.common.ServiceErrorResponse;
import com.vmware.dcp.common.ServiceHost;
import com.vmware.dcp.common.ServiceHost.ServiceHostState;
import com.vmware.dcp.common.ServiceStats;
import com.vmware.dcp.common.ServiceStats.ServiceStat;
import com.vmware.dcp.common.StatefulService;
import com.vmware.dcp.common.UriUtils;
import com.vmware.dcp.common.Utils;
import com.vmware.dcp.common.test.MinimalTestServiceState;
import com.vmware.dcp.common.test.TestProperty;
import com.vmware.dcp.common.test.VerificationHost;
import com.vmware.dcp.services.common.ExampleService.ExampleServiceState;
import com.vmware.dcp.services.common.NodeGroupService.JoinPeerRequest;
import com.vmware.dcp.services.common.NodeGroupService.NodeGroupConfig;
import com.vmware.dcp.services.common.NodeGroupService.NodeGroupState;
import com.vmware.dcp.services.common.NodeState.NodeOption;
import com.vmware.dcp.services.common.NodeState.NodeStatus;
import com.vmware.dcp.services.common.QueryTask.Query;
import com.vmware.dcp.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.dcp.services.common.ReplicationTestService.ReplicationTestServiceErrorResponse;
import com.vmware.dcp.services.common.ReplicationTestService.ReplicationTestServiceState;
import com.vmware.dcp.services.common.ServiceHostManagementService.SynchronizeWithPeersRequest;

public class TestNodeGroupService {

    public static class CustomNodeGroupService extends StatefulService {

        public CustomNodeGroupService() {
            super(ExampleServiceState.class);
            super.toggleOption(ServiceOption.REPLICATION, true);
            super.toggleOption(ServiceOption.OWNER_SELECTION, true);
            super.toggleOption(ServiceOption.PERSISTENCE, true);
        }

    }

    public static class CustomNodeGroupFactoryService extends FactoryService {

        public CustomNodeGroupFactoryService() {
            super(ExampleServiceState.class);
            super.setPeerNodeSelectorPath(CUSTOM_GROUP_NODE_SELECTOR);
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            return new CustomNodeGroupService();
        }

    }

    private static final String CUSTOM_NODE_GROUP_NAME = "custom";
    private static final String CUSTOM_NODE_GROUP = UriUtils.buildUriPath(
            ServiceUriPaths.NODE_GROUP_FACTORY,
            CUSTOM_NODE_GROUP_NAME);
    private static final String CUSTOM_GROUP_NODE_SELECTOR = UriUtils.buildUriPath(
            ServiceUriPaths.NODE_SELECTOR_PREFIX,
            CUSTOM_NODE_GROUP_NAME);

    public static final long DEFAULT_MAINT_INTERVAL_MICROS = TimeUnit.MILLISECONDS
            .toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS);
    private VerificationHost host;

    /**
     * Command line argument specifying default number of in process service hosts
     */
    public int nodeCount = 3;

    /**
     * Command line argument specifying request count
     */
    public int updateCount = 10;

    /**
     * Command line argument specifying service instance count
     */
    public int serviceCount = 10;

    /**
     * Command line argument specifying test duration
     */
    public long testDurationSeconds;

    private NodeGroupConfig nodeGroupConfig = new NodeGroupConfig();
    private EnumSet<ServiceOption> postCreationServiceOptions = EnumSet.noneOf(ServiceOption.class);
    private boolean expectFailure;
    private long expectedFailureStartTimeMicros;
    private List<URI> expectedFailedHosts = new ArrayList<>();
    private String replicationTargetFactoryLink = ExampleFactoryService.SELF_LINK;
    private String replicationNodeSelector = ServiceUriPaths.DEFAULT_NODE_SELECTOR;
    private long replicationFactor;

    private BiPredicate<ExampleServiceState, ExampleServiceState> exampleStateConvergenceChecker = (
            initial, current) -> {
        if (current.name == null) {
            return false;
        }
        return current.name.equals(initial.name);
    };

    private Function<ExampleServiceState, Void> exampleStateUpdateBodySetter = (
            ExampleServiceState state) -> {
        state.name = Utils.getNowMicrosUtc() + "";
        return null;
    };

    private BiPredicate<ReplicationTestServiceState, ReplicationTestServiceState> replicationServiceStatePostUpdateConvergenceChecker = (
            initialState, currentState) -> {
        if (currentState.stringField == null) {
            return false;
        }
        return currentState.stringField.equals(initialState.stringField);
    };

    private Function<ReplicationTestServiceState, Void> replicationServiceStateUpdateBodySetter = (
            body) -> {
        body.stringField = ReplicationTestServiceState.CLIENT_PATCH_HINT + Utils.getNowMicrosUtc();
        return null;
    };

    private boolean isPeerSynchronizationEnabled = true;

    private void setUpPeers(int localHostCount) throws Throwable {
        if (this.host != null) {
            return;
        }
        CommandLineArgumentParser.parseFromProperties(this);
        this.host = VerificationHost.create(0, null);
        this.host.start();
        CommandLineArgumentParser.parseFromProperties(this.host);
        this.host.setStressTest(this.host.isStressTest);
        this.host.setPeerSynchronizationEnabled(this.isPeerSynchronizationEnabled);

        this.host.setUpPeerHosts(localHostCount);

        for (VerificationHost h1 : this.host.getInProcessHostMap().values()) {
            setUpPeerHostWithAdditionalServices(h1);
        }
    }

    private void setUpPeerHostWithAdditionalServices(VerificationHost h1) throws Throwable {
        h1.setStressTest(this.host.isStressTest);

        h1.waitForServiceAvailable(ExampleFactoryService.SELF_LINK);

        LimitedReplicationExampleFactoryService limitedExampleFactory = new LimitedReplicationExampleFactoryService();
        h1.startServiceAndWait(limitedExampleFactory,
                LimitedReplicationExampleFactoryService.SELF_LINK,
                null);

        // start the replication test factory service with OWNER_SELECTION
        ReplicationFactoryTestService ownerSelRplFactory = new ReplicationFactoryTestService();
        h1.startServiceAndWait(ownerSelRplFactory,
                ReplicationFactoryTestService.OWNER_SELECTION_SELF_LINK,
                null);

        // start the replication test factory service with STRICT update checking
        ReplicationFactoryTestService strictReplFactory = new ReplicationFactoryTestService();
        h1.startServiceAndWait(strictReplFactory,
                ReplicationFactoryTestService.STRICT_SELF_LINK, null);

        // start the replication test factory service with simple replication, no owner selection
        ReplicationFactoryTestService replFactory = new ReplicationFactoryTestService();
        h1.startServiceAndWait(replFactory,
                ReplicationFactoryTestService.SIMPLE_REPL_SELF_LINK, null);
    }

    private Map<URI, URI> getFactoriesPerNodeGroup(String factoryLink) {
        Map<URI, URI> map = this.host.getNodeGroupToFactoryMap(factoryLink);
        for (URI h : this.expectedFailedHosts) {
            URI e = UriUtils.buildUri(h, ServiceUriPaths.DEFAULT_NODE_GROUP);
            // do not send messages through hosts that will be stopped: this allows all messages to
            // end the node group and the succeed or fail based on the test goals. If we let messages
            // route through a host that we will abruptly stop, the message might timeout, which is
            // OK for the expected failure case when quorum is not met, but will prevent is from confirming
            // in the non eager consistency case, that all updates were written to at least one host
            map.remove(e);
        }

        return map;
    }

    @Before
    public void setUp() {
        CommandLineArgumentParser.parseFromProperties(this);
    }

    @After
    public void tearDown() throws InterruptedException {
        if (this.host == null) {
            return;
        }

        if (this.host.isRemotePeerTest()) {
            try {
                this.host.logNodeProcessLogs(this.host.getNodeGroupMap().keySet(),
                        ServiceUriPaths.PROCESS_LOG);
            } catch (Throwable e) {
                this.host.log("Failure retrieving process logs: %s", Utils.toString(e));
            }

            try {
                this.host.logNodeManagementState(this.host.getNodeGroupMap().keySet());
            } catch (Throwable e) {
                this.host.log("Failure retrieving management state: %s", Utils.toString(e));
            }
        }

        this.host.tearDownInProcessPeers();
        this.host.toggleNegativeTestMode(false);
        this.host.tearDown();
    }

    @Test
    public void customNodeGroupWithObservers() throws Throwable {
        setUpPeers(this.nodeCount);
        // on one of the hosts create the custom group but with self as an observer. That peer should
        // never receive replicated or broadcast requests
        URI observerHostUri = this.host.getPeerHostUri();
        ServiceHostState observerHostState = this.host.getServiceState(null,
                ServiceHostState.class,
                UriUtils.buildUri(observerHostUri, ServiceUriPaths.CORE_MANAGEMENT));
        Map<URI, NodeState> selfStatePerNode = new HashMap<>();
        NodeState observerSelfState = new NodeState();
        observerSelfState.id = observerHostState.id;
        observerSelfState.options = EnumSet.of(NodeOption.OBSERVER);

        selfStatePerNode.put(observerHostUri, observerSelfState);
        this.host.createCustomNodeGroupOnPeers(CUSTOM_NODE_GROUP_NAME, selfStatePerNode);

        final String customFactoryLink = "custom-factory";
        // start a node selector attached to the custom group
        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            NodeSelectorState initialState = new NodeSelectorState();
            initialState.nodeGroupLink = CUSTOM_NODE_GROUP;
            h.startServiceAndWait(new ConsistentHashingNodeSelectorService(),
                    CUSTOM_GROUP_NODE_SELECTOR, initialState);
            // start the factory that is attached to the custom group selector
            h.startServiceAndWait(CustomNodeGroupFactoryService.class, customFactoryLink);
        }

        Map<URI, EnumSet<NodeOption>> expectedOptionsPerNode = new HashMap<>();
        expectedOptionsPerNode.put(UriUtils.buildUri(observerHostUri, CUSTOM_NODE_GROUP),
                observerSelfState.options);

        this.host.joinNodesAndVerifyConvergence(CUSTOM_NODE_GROUP, this.nodeCount,
                this.nodeCount, expectedOptionsPerNode);

        int restartCount = 0;
        // verify that the observer node shows up as OBSERVER on all peers, including self
        for (URI hostUri : this.host.getNodeGroupMap().keySet()) {
            URI customNodeGroupUri = UriUtils.buildUri(hostUri, CUSTOM_NODE_GROUP);
            NodeGroupState ngs = this.host.getServiceState(null, NodeGroupState.class,
                    customNodeGroupUri);

            for (NodeState ns : ngs.nodes.values()) {
                if (ns.id.equals(observerHostState.id)) {
                    assertTrue(ns.options.contains(NodeOption.OBSERVER));
                } else {
                    assertTrue(ns.options.contains(NodeOption.PEER));
                }
            }

            ServiceStats nodeGroupStats = this.host.getServiceState(null, ServiceStats.class,
                    UriUtils.buildStatsUri(customNodeGroupUri));
            ServiceStat restartStat = nodeGroupStats.entries
                    .get(NodeGroupService.STAT_NAME_RESTARTING_SERVICES_COUNT);
            if (restartStat != null) {
                restartCount += restartStat.latestValue;
            }
        }

        assertEquals("expected different number of service restarts", restartCount, 0);

        // join all the nodes through the default group, making sure another group still works
        this.host.joinNodesAndVerifyConvergence(this.nodeCount, true);

        URI observerFactoryUri = UriUtils.buildUri(observerHostUri, customFactoryLink);
        // create N services on the custom group, verify none of them got created on the observer.
        // We actually post directly to the observer node, which should forward to the other nodes
        Map<URI, ExampleServiceState> serviceStatesOnPost = this.host.doFactoryChildServiceStart(
                null, this.serviceCount,
                ExampleServiceState.class,
                (o) -> {
                    ExampleServiceState body = new ExampleServiceState();
                    body.name = Utils.getNowMicrosUtc() + "";
                    o.setBody(body);
                },
                observerFactoryUri);

        ServiceDocumentQueryResult r = this.host.getFactoryState(observerFactoryUri);
        assertEquals(0, r.documentLinks.size());

        // do a GET on each service and confirm the owner id is never that of the observer node
        Map<URI, ExampleServiceState> serviceStatesFromGet = this.host.getServiceState(
                null, ExampleServiceState.class, serviceStatesOnPost.keySet());

        for (ExampleServiceState s : serviceStatesFromGet.values()) {
            if (observerHostState.id.equals(s.documentOwner)) {
                throw new IllegalStateException("Observer node reported state for service");
            }
        }

        URI existingNodeGroup = this.host.getPeerNodeGroupUri();

        // start  more nodes, insert them to existing group, but with no synchronization required
        // start some additional nodes
        int additionalHostCount = this.nodeCount;
        List<ServiceHost> newHosts = Collections.synchronizedList(new ArrayList<>());
        this.host.testStart(additionalHostCount);
        for (int i = 0; i < additionalHostCount; i++) {
            this.host.run(() -> {
                try {
                    this.host.setUpLocalPeerHost(newHosts, DEFAULT_MAINT_INTERVAL_MICROS);
                } catch (Throwable e) {
                    this.host.failIteration(e);
                }
            });
        }
        this.host.testWait();

        expectedOptionsPerNode.clear();
        // join new nodes with existing node group.
        this.host.testStart(newHosts.size());
        for (ServiceHost h : newHosts) {
            URI newCustomNodeGroupUri = UriUtils.buildUri(h, ServiceUriPaths.DEFAULT_NODE_GROUP);

            JoinPeerRequest joinBody = JoinPeerRequest.create(existingNodeGroup, null);
            joinBody.localNodeOptions = EnumSet.of(NodeOption.PEER);
            this.host.send(Operation.createPost(newCustomNodeGroupUri)
                    .setBody(joinBody)
                    .setCompletion(this.host.getCompletion()));
            expectedOptionsPerNode.put(newCustomNodeGroupUri, joinBody.localNodeOptions);
        }

        this.host.testWait();
        this.host.waitForNodeGroupConvergence(this.host.getNodeGroupMap().values(),
                this.host.getNodeGroupMap().size(),
                this.host.getNodeGroupMap().size(),
                expectedOptionsPerNode, false);

        restartCount = 0;
        // do another restart check. None of the new nodes should have reported restarts
        for (URI hostUri : this.host.getNodeGroupMap().keySet()) {
            URI nodeGroupUri = UriUtils.buildUri(hostUri, ServiceUriPaths.DEFAULT_NODE_GROUP);
            ServiceStats nodeGroupStats = this.host.getServiceState(null, ServiceStats.class,
                    UriUtils.buildStatsUri(nodeGroupUri));
            ServiceStat restartStat = nodeGroupStats.entries
                    .get(NodeGroupService.STAT_NAME_RESTARTING_SERVICES_COUNT);
            if (restartStat != null) {
                restartCount += restartStat.latestValue;
            }
        }

        assertEquals("expected different number of service restarts", 0,
                restartCount);
    }

    @Test
    public void factorySynchronizationNoChildren() throws Throwable {
        int factoryCount = Math.max(this.serviceCount, 25);
        setUpPeers(this.nodeCount);

        // start many factories, in each host, so when the nodes join there will be a storm
        // of synchronization requests between the nodes + factory instances
        this.host.testStart(this.nodeCount * factoryCount);
        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            for (int i = 0; i < factoryCount; i++) {
                Operation startPost = Operation.createPost(
                        UriUtils.buildUri(h,
                                UriUtils.buildUriPath(ExampleFactoryService.SELF_LINK, UUID
                                        .randomUUID().toString())))
                        .setCompletion(this.host.getCompletion());
                h.startService(startPost, new ExampleFactoryService());
            }
        }
        this.host.testWait();
        this.host.joinNodesAndVerifyConvergence(this.host.getPeerCount());
    }

    @Test
    public void synchronizationWithDifferentNodeInitialStateManual() throws Throwable {
        this.isPeerSynchronizationEnabled = false;
        doSynchronizationWithDifferentNodeInitialState();
    }

    private void doSynchronizationWithDifferentNodeInitialState() throws Throwable {
        ExampleServiceHost h = null;

        if (this.serviceCount < FactoryService.SELF_QUERY_RESULT_LIMIT) {
            this.serviceCount = FactoryService.SELF_QUERY_RESULT_LIMIT + 10;
            this.updateCount = 1;
        }

        try {
            setUpPeers(this.nodeCount);

            // On one host, add some services. They exist only on this host and we expect them to synchronize
            // across all hosts once this one joins with the group
            VerificationHost hostWithInitialState = this.host.getInProcessHostMap().values()
                    .iterator().next();
            URI factoryUri = UriUtils.buildUri(hostWithInitialState,
                    ExampleFactoryService.SELF_LINK);
            this.host.log("Starting, auto synch: %s. Adding children to %s",
                    this.isPeerSynchronizationEnabled, factoryUri);
            // add some services on one of the peers, so we can verify the get synchronized after they all join
            Map<URI, ExampleServiceState> exampleStates = this.host.doFactoryChildServiceStart(
                    null,
                    this.serviceCount,
                    ExampleServiceState.class,
                    (o) -> {
                        ExampleServiceState s = new ExampleServiceState();
                        s.name = UUID.randomUUID().toString();
                        o.setBody(s);
                    }, factoryUri);

            Map<String, ExampleServiceState> exampleStatesPerSelfLink = new HashMap<>();

            for (ExampleServiceState s : exampleStates.values()) {
                exampleStatesPerSelfLink.put(s.documentSelfLink, s);
            }

            // add the *same* service instance, all *all* peers, so we force synchronization and epoch
            // change on an instance that exists everywhere

            int dupServiceCount = Math.min(10, this.serviceCount / 2);
            AtomicInteger counter = new AtomicInteger();
            for (VerificationHost v : this.host.getInProcessHostMap().values()) {
                counter.set(0);
                factoryUri = UriUtils.buildUri(v,
                        ExampleFactoryService.SELF_LINK);
                this.host.doFactoryChildServiceStart(
                        null,
                        dupServiceCount,
                        ExampleServiceState.class,
                        (o) -> {
                            ExampleServiceState s = new ExampleServiceState();
                            s.documentSelfLink = "duplicateExampleInstance-"
                                    + counter.incrementAndGet();
                            s.name = s.documentSelfLink;
                            o.setBody(s);
                        } , factoryUri);
            }

            // increment to account for link found on all nodes
            this.serviceCount = exampleStatesPerSelfLink.size();

            // create peer argument list. Include all but one node, which we will add after
            // all the nodes join.
            Collection<URI> peerNodeGroupUris = new ArrayList<>();
            StringBuilder peerNodes = new StringBuilder();
            for (VerificationHost peer : this.host.getInProcessHostMap().values()) {
                if (hostWithInitialState.equals(peer)) {
                    // skip this host, we will add it after an initial empty group has formed
                    continue;
                }
                peerNodeGroupUris.add(UriUtils.buildUri(peer, ServiceUriPaths.DEFAULT_NODE_GROUP));
                peerNodes.append(peer.getUri().toString()).append(",");
            }

            CountDownLatch notifications = new CountDownLatch(this.nodeCount);
            for (URI nodeGroup : this.host.getNodeGroupMap().values()) {
                this.host.subscribeForNodeGroupConvergence(nodeGroup, this.nodeCount + 1,
                        (o, e) -> {
                            if (e != null) {
                                this.host.log("Error in notificaiton: %s", Utils.toString(e));
                                return;
                            }
                            notifications.countDown();
                        });
            }

            // now start a new Host and supply the already created peer, then observe the automatic
            // join
            h = new ExampleServiceHost();

            String mainHostId = "main-" + VerificationHost.hostNumber.incrementAndGet();
            String[] args = {
                    "--port=0",
                    "--id=" + mainHostId,
                    "--bindAddress=127.0.0.1",
                    "--sandbox="
                            + Files.createTempDirectory(VerificationHost.TMP_PATH_PREFIX).toUri(),
                    "--peerNodes=" + peerNodes.toString()
            };

            h.initialize(args);

            h.setPeerSynchronizationEnabled(this.isPeerSynchronizationEnabled);
            h.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS
                    .toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
            h.start();

            peerNodeGroupUris.add(UriUtils.buildUri(h, ServiceUriPaths.DEFAULT_NODE_GROUP));
            this.host.waitForNodeGroupConvergence(peerNodeGroupUris, this.nodeCount,
                    this.nodeCount, true);
            scheduleSynchronizationIfAutoSyncDisabled();

            int peerNodeCount = h.getInitialPeerHosts().size();
            // include self in peers
            assertTrue(this.nodeCount >= peerNodeCount + 1);

            // add the host with state. to the already formed group
            URI existingMemberNodeGroup = UriUtils.buildUri(h.getUri(),
                    ServiceUriPaths.DEFAULT_NODE_GROUP);
            URI hostWithStateNodeGroup = UriUtils.buildUri(hostWithInitialState.getUri(),
                    ServiceUriPaths.DEFAULT_NODE_GROUP);
            // explicitly add host, joinNodeGroup will not do it.
            this.host.testStart(1);
            this.host.addPeerNode(hostWithInitialState);
            this.host
                    .joinNodeGroup(hostWithStateNodeGroup, existingMemberNodeGroup, this.nodeCount);
            this.host.testWait();

            // wait for node group convergence
            int totalNodeCount = this.nodeCount + 1;
            this.host.waitForNodeGroupConvergence(totalNodeCount);
            this.host.log("Wait for notifications on group convergence");

            if (!notifications.await(this.host.getTimeoutSeconds(), TimeUnit.SECONDS)) {
                throw new TimeoutException("Notifications on group convergence");
            }

            scheduleSynchronizationIfAutoSyncDisabled();

            // now verify all nodes synchronize and see the example service instances that existed on the single
            // host
            waitForReplicatedFactoryChildServiceConvergence(
                    exampleStatesPerSelfLink,
                    this.exampleStateConvergenceChecker,
                    this.serviceCount, 0);

            // update the quorum level on the group
            int quorum = (totalNodeCount / 2) + 1;
            this.host.setNodeGroupQuorum(quorum);

            NodeGroupState ngs = this.host.getServiceState(null, NodeGroupState.class,
                    UriUtils.buildUri(h, ServiceUriPaths.DEFAULT_NODE_GROUP));
            for (NodeState s : ngs.nodes.values()) {
                if (!s.id.equals(h.getId())) {
                    continue;
                }
                assertEquals(quorum, s.membershipQuorum);
            }

            // Send some updates after the full group has formed  and verify the updates are seen by services on all nodes

            doStateUpdateReplicationTest(Action.PATCH, this.serviceCount, this.updateCount, 0,
                    this.exampleStateUpdateBodySetter,
                    this.exampleStateConvergenceChecker,
                    exampleStatesPerSelfLink);

            for (VerificationHost peer : this.host.getInProcessHostMap().values()) {
                this.verifyPendingChildServiceSynchStats(
                        UriUtils.buildUri(peer, ExampleFactoryService.SELF_LINK), 0);
            }

            this.verifyPendingChildServiceSynchStats(
                    UriUtils.buildUri(h, ExampleFactoryService.SELF_LINK), 0);

        } finally {
            this.host.log("test finished");
            if (h != null) {
                h.stop();
                VerificationHost.cleanupStorage(h.getStorageSandbox());
            }
        }
    }

    private void scheduleSynchronizationIfAutoSyncDisabled() throws Throwable {
        if (this.isPeerSynchronizationEnabled) {
            return;
        }
        for (VerificationHost peerHost : this.host.getInProcessHostMap().values()) {
            peerHost.scheduleNodeGroupChangeMaintenance(
                    ServiceUriPaths.DEFAULT_NODE_SELECTOR);
            ServiceStats selectorStats = this.host.getServiceState(null, ServiceStats.class,
                    UriUtils.buildStatsUri(peerHost, ServiceUriPaths.DEFAULT_NODE_SELECTOR));
            ServiceStat synchStat = selectorStats.entries
                    .get(ConsistentHashingNodeSelectorService.STAT_NAME_SYNCHRONIZATION_COUNT);
            if (synchStat != null && synchStat.latestValue > 0) {
                throw new IllegalStateException("Automatic synchronization was triggered");
            }
        }

    }

    /**
     * This test validates that if a host, joined in a peer node group, stops/fails and another
     * host, listening on the same address:port, rejoins, the existing peer members will mark the
     * OLD host instance as FAILED, and mark the new instance, with the new ID as HEALTHY
     *
     * @throws Throwable
     */
    @Test
    public void nodeRestartWithSameAddressDifferentId() throws Throwable {
        int failedNodeCount = 1;

        setUpPeers(this.nodeCount);
        setOperationTimeoutMicros(TimeUnit.SECONDS.toMicros(5));

        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        this.host.log("Stopping node");

        // we should now have N nodes, that see each other. Stop one of the
        // nodes, and verify the other host's node group deletes the entry
        List<ServiceHostState> hostStates = stopHostsToSimulateFailure(failedNodeCount);
        URI remainingPeerNodeGroup = this.host.getPeerNodeGroupUri();

        // wait for convergence of the remaining peers, before restarting. The failed host
        // should be marked FAILED, otherwise we will not converge
        this.host.waitForNodeGroupConvergence(this.nodeCount - failedNodeCount);

        ServiceHostState stoppedHostState = hostStates.get(0);

        // start a new HOST, with a new ID, but with the same address:port as the one we stopped
        this.host.testStart(1);
        VerificationHost newHost = this.host.setUpLocalPeerHost(stoppedHostState.httpPort,
                VerificationHost.FAST_MAINT_INTERVAL_MILLIS, null);
        this.host.testWait();

        // re-join the remaining peers
        URI newHostNodeGroupService = UriUtils
                .buildUri(newHost.getUri(), ServiceUriPaths.DEFAULT_NODE_GROUP);
        this.host.testStart(1);
        this.host.joinNodeGroup(newHostNodeGroupService, remainingPeerNodeGroup);
        this.host.testWait();

        // now wait for convergence. If the logic is correct, the old HOST, that listened on the
        // same port as the new host, should stay in the FAILED state, but the new host should
        // be marked as HEALTHY
        this.host.waitForNodeGroupConvergence(this.nodeCount);
    }

    public void setMaintenanceIntervalMillis(long defaultMaintIntervalMillis) {
        for (VerificationHost h1 : this.host.getInProcessHostMap().values()) {
            // set short interval so failure detection and convergence happens quickly
            h1.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS
                    .toMicros(defaultMaintIntervalMillis));
        }
    }

    @Test
    public void replicationWithConcurrentNodeFailureEagerConsistency()
            throws Throwable {
        this.postCreationServiceOptions.add(ServiceOption.EAGER_CONSISTENCY);
        this.expectFailure = true;
        doNodeReplicationWithNodeStopStart(true, true);
    }

    private void doNodeReplicationWithNodeStopStart(boolean doStop,
            boolean doReplicationPostStart) throws Throwable {

        long opTimeoutMicros = TimeUnit.SECONDS.toMicros(3);
        int hostRestartCount = 2;

        Map<String, ExampleServiceState> childStates = doExampleFactoryPostReplicationTest(
                this.serviceCount, null);

        updateExampleServiceOptions(childStates);

        setOperationTimeoutMicros(opTimeoutMicros);

        this.nodeGroupConfig.nodeRemovalDelayMicros = TimeUnit.SECONDS.toMicros(10);
        this.host.setNodeGroupConfig(this.nodeGroupConfig);
        if (this.postCreationServiceOptions.contains(ServiceOption.EAGER_CONSISTENCY)) {
            this.host.setNodeGroupQuorum((this.nodeCount + 1) / 2);
        }

        if (doStop) {
            int i = 0;
            for (URI h : this.host.getInProcessHostMap().keySet()) {
                this.expectedFailedHosts.add(h);
                if (++i >= hostRestartCount) {
                    break;
                }
            }

            // stop one host right away
            stopHostsToSimulateFailure(1);

            // concurrently with the PATCH requests below, stop some of the hosts
            this.host.schedule(() -> {
                stopHostsToSimulateFailure(hostRestartCount - 1);
                // add a small bit of time slop since its feasible a host completed a operation *after* we stopped it,
                // the netty handlers are stopped in async (not forced) mode
                this.expectedFailureStartTimeMicros = Utils.getNowMicrosUtc()
                        + TimeUnit.MILLISECONDS.toMicros(250);
            }, 1, TimeUnit.MILLISECONDS);
        }

        childStates = doStateUpdateReplicationTest(Action.PATCH, this.serviceCount,
                this.updateCount,
                this.updateCount,
                this.exampleStateUpdateBodySetter,
                this.exampleStateConvergenceChecker,
                childStates);

        if (!doReplicationPostStart) {
            return;
        }

        doStateUpdateReplicationTest(Action.PATCH, childStates.size(), this.updateCount,
                this.updateCount * 2,
                this.exampleStateUpdateBodySetter,
                this.exampleStateConvergenceChecker,
                childStates);
    }

    private void setOperationTimeoutMicros(long opTimeoutMicros) {
        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            h.setOperationTimeOutMicros(opTimeoutMicros);
        }
        this.host.setOperationTimeOutMicros(opTimeoutMicros);
    }

    /**
     * This test creates N local service hosts, each with K instances of a replicated service. The
     * service will create a query task, also replicated, and self patch itself. The test makes sure
     * all K instances, on all N hosts see the self PATCHs AND that the query tasks exist on all
     * hosts
     *
     * @throws Throwable
     */
    @Test
    public void replicationWithCrossServiceDependencies() throws Throwable {
        // this.isPeerSynchronizationEnabled = false;
        setUpPeers(this.nodeCount);

        this.host.joinNodesAndVerifyConvergence(this.host.getPeerCount());

        Consumer<Operation> setBodyCallback = (o) -> {
            ReplicationTestServiceState s = new ReplicationTestServiceState();
            s.stringField = UUID.randomUUID().toString();
            o.setBody(s);
        };

        URI hostUri = this.host.getPeerServiceUri(null);

        URI factoryUri = UriUtils.buildUri(hostUri,
                ReplicationFactoryTestService.SIMPLE_REPL_SELF_LINK);
        doReplicatedServiceFactoryPost(this.serviceCount, setBodyCallback, factoryUri);

        factoryUri = UriUtils.buildUri(hostUri,
                ReplicationFactoryTestService.OWNER_SELECTION_SELF_LINK);
        Map<URI, ReplicationTestServiceState> ownerSelectedServices =
                doReplicatedServiceFactoryPost(this.serviceCount, setBodyCallback, factoryUri);

        factoryUri = UriUtils.buildUri(hostUri, ReplicationFactoryTestService.STRICT_SELF_LINK);
        doReplicatedServiceFactoryPost(this.serviceCount, setBodyCallback, factoryUri);

        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        Query kindClause = new Query();
        kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(Utils.buildKind(ReplicationTestServiceState.class));
        q.query.addBooleanClause(kindClause);

        Query nameClause = new Query();
        nameClause.setTermPropertyName("stringField")
                .setTermMatchValue("*")
                .setTermMatchType(MatchType.WILDCARD);
        q.query.addBooleanClause(nameClause);

        // expect results for strict and regular service instances
        int expectedServiceCount = this.serviceCount * 3;

        Date exp = this.host.getTestExpiration();
        while (exp.after(new Date())) {
            // create N direct query tasks. Direct tasks complete in the context of the POST to the
            // query task factory
            int count = 10;
            URI queryFactoryUri = UriUtils.extendUri(hostUri,
                    ServiceUriPaths.CORE_QUERY_TASKS);
            this.host.testStart(count);

            Map<String, QueryTask> taskResults = new ConcurrentSkipListMap<>();
            for (int i = 0; i < count; i++) {
                QueryTask qt = QueryTask.create(q);
                qt.taskInfo.isDirect = true;
                qt.documentSelfLink = UUID.randomUUID().toString();
                Operation startPost = Operation
                        .createPost(queryFactoryUri)
                        .setBody(qt)
                        .setCompletion(
                                (o, e) -> {
                                    if (e != null) {
                                        this.host.failIteration(e);
                                        return;
                                    }

                                    QueryTask rsp = o.getBody(QueryTask.class);
                                    qt.results = rsp.results;
                                    qt.documentOwner = rsp.documentOwner;
                                    taskResults.put(rsp.documentSelfLink, qt);
                                    this.host.completeIteration();
                                });

                this.host.send(startPost);
            }
            this.host.testWait();
            this.host.logThroughput();

            boolean converged = true;
            for (QueryTask qt : taskResults.values()) {
                if (qt.results == null || qt.results.documentLinks == null) {
                    throw new IllegalStateException("Missing results");
                }
                if (qt.results.documentLinks.size() != expectedServiceCount) {
                    this.host.log("%s", Utils.toJsonHtml(qt));
                    converged = false;
                    break;
                }
            }

            if (!converged) {
                Thread.sleep(250);
                continue;
            }
            break;
        }

        if (exp.before(new Date())) {
            throw new TimeoutException();
        }

        // Negative tests: Make sure custom error response body is preserved
        URI childUri = ownerSelectedServices.keySet().iterator().next();
        this.host.testStart(1);
        ReplicationTestServiceState badRequestBody = new ReplicationTestServiceState();
        this.host
                .send(Operation
                        .createPatch(childUri)
                        .setBody(badRequestBody)
                        .setCompletion(
                                (o, e) -> {
                                    if (e == null) {
                                        this.host.failIteration(new IllegalStateException(
                                                "Expected failure"));
                                        return;
                                    }

                                    ReplicationTestServiceErrorResponse rsp = o
                                            .getBody(ReplicationTestServiceErrorResponse.class);
                                    if (!ReplicationTestServiceErrorResponse.KIND
                                            .equals(rsp.documentKind)) {
                                        this.host.failIteration(new IllegalStateException(
                                                "Expected custom response body"));
                                        return;
                                    }

                                    this.host.completeIteration();
                                }));
        this.host.testWait();

        // verify that each owner selected service reports stats from the same node that reports state
        Map<URI, ReplicationTestServiceState> latestState = this.host.getServiceState(null,
                ReplicationTestServiceState.class, ownerSelectedServices.keySet());
        Map<String, String> ownerIdPerLink = new HashMap<>();
        List<URI> statsUris = new ArrayList<>();
        for (ReplicationTestServiceState state : latestState.values()) {
            URI statsUri = this.host.getPeerServiceUri(UriUtils.buildUriPath(
                    state.documentSelfLink, ServiceHost.SERVICE_URI_SUFFIX_STATS));
            ownerIdPerLink.put(state.documentSelfLink, state.documentOwner);
            statsUris.add(statsUri);
        }

        Map<URI, ServiceStats> latestStats = this.host.getServiceState(null, ServiceStats.class,
                statsUris);
        for (ServiceStats perServiceStats : latestStats.values()) {

            String serviceLink = UriUtils.getParentPath(perServiceStats.documentSelfLink);
            String expectedOwnerId = ownerIdPerLink.get(serviceLink);
            if (expectedOwnerId.equals(perServiceStats.documentOwner)) {
                continue;
            }
            throw new IllegalStateException("owner routing issue with stats: "
                    + Utils.toJsonHtml(perServiceStats));

        }
    }

    private Map<URI, ReplicationTestServiceState> doReplicatedServiceFactoryPost(int serviceCount,
            Consumer<Operation> setBodyCallback, URI factoryUri) throws Throwable,
            InterruptedException, TimeoutException {

        ServiceDocumentDescription sdd = this.host
                .buildDescription(ReplicationTestServiceState.class);
        Map<URI, ReplicationTestServiceState> serviceMap = this.host.doFactoryChildServiceStart(
                null, serviceCount, ReplicationTestServiceState.class, setBodyCallback, factoryUri);

        Date expiration = this.host.getTestExpiration();
        boolean isConverged = true;
        Map<URI, String> uriToSignature = new HashMap<>();
        while (new Date().before(expiration)) {
            isConverged = true;
            uriToSignature.clear();
            for (Entry<URI, VerificationHost> e : this.host.getInProcessHostMap().entrySet()) {
                URI baseUri = e.getKey();
                VerificationHost h = e.getValue();
                URI u = UriUtils.buildUri(baseUri, factoryUri.getPath());
                u = UriUtils.buildExpandLinksQueryUri(u);
                ServiceDocumentQueryResult r = this.host.getFactoryState(u);
                if (r.documents.size() != serviceCount) {
                    this.host.log("instance count mismatch, expected %d, got %d, from %s",
                            serviceCount, r.documents.size(), u);
                    isConverged = false;
                    break;
                }

                for (URI instanceUri : serviceMap.keySet()) {
                    ReplicationTestServiceState initialState = serviceMap.get(instanceUri);
                    ReplicationTestServiceState newState = Utils.fromJson(
                            r.documents.get(instanceUri.getPath()),
                            ReplicationTestServiceState.class);
                    if (newState.documentVersion == 0) {
                        this.host.log("version mismatch, expected %d, got %d, from %s", 1,
                                newState.documentVersion, instanceUri);
                        isConverged = false;
                        break;
                    }

                    if (initialState.stringField.equals(newState.stringField)) {
                        this.host.log("field mismatch, expected %s, got %s, from %s",
                                initialState.stringField, newState.stringField, instanceUri);
                        isConverged = false;
                        break;
                    }

                    if (newState.queryTaskLink == null) {
                        this.host.log("missing query task link from %s", instanceUri);
                        isConverged = false;
                        break;
                    }

                    // Only instances with OWNER_SELECTION patch string field with self link so bypass this check
                    if (!newState.documentSelfLink
                            .contains(ReplicationFactoryTestService.STRICT_SELF_LINK)
                            && !newState.documentSelfLink
                                    .contains(ReplicationFactoryTestService.SIMPLE_REPL_SELF_LINK)
                            && !newState.stringField.equals(newState.documentSelfLink)) {
                        this.host.log("State not in final state");
                        isConverged = false;
                        break;
                    }

                    String sig = uriToSignature.get(instanceUri);
                    if (sig == null) {
                        sig = Utils.computeSignature(newState, sdd);
                        uriToSignature.put(instanceUri, sig);
                    } else {
                        String newSig = Utils.computeSignature(newState, sdd);
                        if (!sig.equals(newSig)) {
                            isConverged = false;
                            this.host.log("signature mismatch, expected %s, got %s, from %s",
                                    sig, newSig, instanceUri);
                        }
                    }

                    ProcessingStage ps = h.getServiceStage(newState.queryTaskLink);
                    if (ps == null || ps != ProcessingStage.AVAILABLE) {
                        this.host.log("missing query task service from %s", newState.queryTaskLink,
                                instanceUri);
                        isConverged = false;
                        break;
                    }
                }

                if (isConverged == false) {
                    break;
                }
            }
            if (isConverged == true) {
                break;
            }

            Thread.sleep(100);
        }

        if (!isConverged) {
            throw new TimeoutException("States did not converge");
        }

        return serviceMap;
    }

    @Test
    public void replication() throws Throwable {
        this.isPeerSynchronizationEnabled = false;
        this.replicationTargetFactoryLink = ExampleFactoryService.SELF_LINK;
        doReplication();
    }

    @Test
    public void replicationLimitedFactor() throws Throwable {
        this.replicationFactor = 3L;
        this.replicationNodeSelector = ServiceUriPaths.SHA1_3X_NODE_SELECTOR;
        this.replicationTargetFactoryLink = LimitedReplicationExampleFactoryService.SELF_LINK;
        this.nodeCount = Math.max(5, this.nodeCount);
        doReplication();
    }

    private void doReplication() throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        Date expiration = new Date();
        if (this.testDurationSeconds > 0) {
            expiration = new Date(expiration.getTime()
                    + TimeUnit.SECONDS.toMillis(this.testDurationSeconds));
        }

        do {
            Map<String, ExampleServiceState> childStates = doExampleFactoryPostReplicationTest(
                    this.serviceCount);

            if (expiration == null) {
                expiration = this.host.getTestExpiration();
            }
            int expectedVersion = this.updateCount;

            if (!this.host.isStressTest()
                    && (this.host.getPeerCount() > 16 || this.serviceCount * this.updateCount > 100)) {
                this.host.setStressTest(true);
            }

            if (this.testDurationSeconds == 0 && this.serviceCount < 100) {
                doMissingServiceUpdatesNoQueuing();
            }

            childStates = doStateUpdateReplicationTest(Action.PATCH, this.serviceCount,
                    this.updateCount,
                    expectedVersion,
                    this.exampleStateUpdateBodySetter,
                    this.exampleStateConvergenceChecker,
                    childStates);
            expectedVersion += this.updateCount;

            childStates = doStateUpdateReplicationTest(Action.PUT, this.serviceCount,
                    this.updateCount,
                    expectedVersion,
                    this.exampleStateUpdateBodySetter,
                    this.exampleStateConvergenceChecker,
                    childStates);

            Date queryExp = this.host.getTestExpiration();
            if (expiration.after(queryExp)) {
                queryExp = expiration;
            }
            while (new Date().before(queryExp)) {
                Set<String> links = verifyReplicatedServiceCountWithBroadcastQueries();
                if (links.size() < this.serviceCount) {
                    this.host.log("Found only %d links across nodes, retrying", links.size());
                    Thread.sleep(500);
                    continue;
                }
                break;
            }

            if (queryExp.before(new Date())) {
                throw new TimeoutException();
            }

            this.scheduleSynchronizationIfAutoSyncDisabled();
            expectedVersion += 1;
            doStateUpdateReplicationTest(Action.DELETE, this.serviceCount, 1,
                    expectedVersion,
                    this.exampleStateUpdateBodySetter,
                    this.exampleStateConvergenceChecker,
                    childStates);

        } while (new Date().before(expiration));

        this.host.doNodeGroupStatsVerification(this.host.getNodeGroupMap());
    }

    private Set<String> verifyReplicatedServiceCountWithBroadcastQueries()
            throws Throwable {
        // create a query task, which will execute on a randomly selected node. Since there is no guarantee the node
        // selected to execute the query task is the one with all the replicated services, broadcast to all nodes, then
        // join the results

        URI nodeUri = this.host.getPeerHostUri();
        this.host.testStart(1);
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND).setTermMatchValue(
                Utils.buildKind(ExampleServiceState.class));
        QueryTask task = QueryTask.create(q).setDirect(true);
        URI queryTaskFactoryUri = UriUtils
                .buildUri(nodeUri, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);

        // send the POST to the forwarding service on one of the nodes, with the broadcast query parameter set
        URI forwardingService = UriUtils.buildBroadcastRequestUri(queryTaskFactoryUri,
                ServiceUriPaths.DEFAULT_NODE_SELECTOR);

        Set<String> links = new HashSet<>();

        Operation postQuery = Operation
                .createPost(forwardingService)
                .setBody(task)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }

                            NodeGroupBroadcastResponse rsp = o
                                    .getBody(NodeGroupBroadcastResponse.class);

                            if (!rsp.failures.isEmpty()) {
                                this.host.failIteration(new IllegalStateException(
                                        "Failure from query tasks: " + Utils.toJsonHtml(rsp)));
                                return;
                            }

                            // verify broadcast requests should come from all discrete nodes
                        Set<String> ownerIds = new HashSet<>();

                        for (Entry<URI, String> en : rsp.jsonResponses.entrySet()) {
                            String jsonRsp = en.getValue();
                            QueryTask qt = Utils.fromJson(jsonRsp, QueryTask.class);
                            this.host.log("Broadcast response from %s %s", qt.documentSelfLink,
                                    qt.documentOwner);
                            ownerIds.add(qt.documentOwner);
                            if (qt.results == null) {
                                this.host.log("Node %s had no results", en.getKey());
                                continue;
                            }
                            for (String l : qt.results.documentLinks) {
                                links.add(l);
                            }
                        }

                        if (ownerIds.size() != rsp.jsonResponses.size()) {
                            throw new IllegalStateException(
                                    "Number of owners in response less than node count: " +
                                            ownerIds.toString());
                        }
                        this.host.completeIteration();
                    });

        this.host.send(postQuery);
        this.host.testWait();

        return links;
    }

    private void doMissingServiceUpdatesNoQueuing() throws Throwable {
        this.host.toggleNegativeTestMode(true);
        // do a negative test: send request to a example child we know does not exist, but disable queuing
        // so we get 404 right away
        this.host.testStart(this.serviceCount);
        for (int i = 0; i < this.serviceCount; i++) {
            URI factoryURI = this.host.getNodeGroupToFactoryMap(ExampleFactoryService.SELF_LINK)
                    .values().iterator().next();
            URI bogusChild = UriUtils.extendUri(factoryURI, UUID.randomUUID().toString());
            Operation patch = Operation.createPatch(bogusChild)
                    .setCompletion(this.host.getExpectedFailureCompletion())
                    .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_QUEUING)
                    .setBody(new ExampleServiceState());

            this.host.send(patch);
        }
        this.host.testWait();
        this.host.toggleNegativeTestMode(false);
    }

    @Test
    public void broadcast() throws Throwable {
        setUpPeers(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);

        // Do a broadcast on a local, non replicated service. Replicated services can not
        // be used with broadcast since they will duplicate the update and potentially route
        // to a single node
        URI nodeGroup = this.host.getPeerNodeGroupUri();

        long c = this.updateCount * this.nodeCount;
        List<ServiceDocument> initialStates = new ArrayList<>();
        for (int i = 0; i < c; i++) {
            ServiceDocument s = this.host.buildMinimalTestState();
            s.documentSelfLink = UUID.randomUUID().toString();
            initialStates.add(s);
        }

        this.host.testStart(c * this.host.getPeerCount());
        for (VerificationHost peer : this.host.getInProcessHostMap().values()) {
            for (ServiceDocument s : initialStates) {
                Operation post = Operation.createPost(UriUtils.buildUri(peer, s.documentSelfLink))
                        .setCompletion(this.host.getCompletion())
                        .setBody(s);
                peer.startService(post, new MinimalTestService());
            }
        }
        this.host.testWait();

        // we broadcast one update, per service, through one peer. We expect to see
        // the same update across all peers, just like with replicated services
        nodeGroup = this.host.getPeerNodeGroupUri();
        this.host.testStart(initialStates.size());
        for (ServiceDocument s : initialStates) {
            URI serviceUri = UriUtils.buildUri(nodeGroup, s.documentSelfLink);
            URI u = UriUtils.buildBroadcastRequestUri(serviceUri,
                    ServiceUriPaths.DEFAULT_NODE_SELECTOR);
            MinimalTestServiceState body = (MinimalTestServiceState) this.host
                    .buildMinimalTestState();
            body.id = serviceUri.getPath();
            this.host.send(Operation.createPut(u)
                    .setCompletion(this.host.getCompletion())
                    .setBody(body));
        }
        this.host.testWait();

        for (URI baseHostUri : this.host.getNodeGroupMap().keySet()) {
            List<URI> uris = new ArrayList<>();
            for (ServiceDocument s : initialStates) {
                URI serviceUri = UriUtils.buildUri(baseHostUri, s.documentSelfLink);
                uris.add(serviceUri);
            }
            Map<URI, MinimalTestServiceState> states = this.host.getServiceState(null,
                    MinimalTestServiceState.class, uris);
            for (MinimalTestServiceState s : states.values()) {
                // the PUT we issued, should have been forwarded to this service and modified its
                // initial ID to be the same as the self link
                if (!s.id.equals(s.documentSelfLink)) {
                    throw new IllegalStateException("Service broadcast failure");
                }

            }
        }
    }

    @Test
    public void forwardingToKeyHashNode() throws Throwable {
        setUpPeers(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        long c = this.updateCount * this.nodeCount;
        Map<String, List<String>> ownersPerServiceLink = new HashMap<>();

        // 0) Create N service instances, in each peer host. Services are NOT replicated
        // 1) issue a forward request to owner, per service link
        // 2) verify the request ended up on the owner the partitioning service predicted
        List<ServiceDocument> initialStates = new ArrayList<>();
        for (int i = 0; i < c; i++) {
            ServiceDocument s = this.host.buildMinimalTestState();
            s.documentSelfLink = UUID.randomUUID().toString();
            initialStates.add(s);
        }

        this.host.testStart(c * this.host.getPeerCount());
        for (VerificationHost peer : this.host.getInProcessHostMap().values()) {
            for (ServiceDocument s : initialStates) {
                Operation post = Operation.createPost(UriUtils.buildUri(peer, s.documentSelfLink))
                        .setCompletion(this.host.getCompletion())
                        .setBody(s);
                peer.startService(post, new MinimalTestService());
            }
        }
        this.host.testWait();

        URI nodeGroup = this.host.getPeerNodeGroupUri();
        this.host.testStart(initialStates.size());
        for (ServiceDocument s : initialStates) {
            URI serviceUri = UriUtils.buildUri(nodeGroup, s.documentSelfLink);
            URI u = UriUtils.buildForwardRequestUri(serviceUri,
                    null,
                    ServiceUriPaths.DEFAULT_NODE_SELECTOR);
            MinimalTestServiceState body = (MinimalTestServiceState) this.host
                    .buildMinimalTestState();
            body.id = serviceUri.getPath();
            this.host.send(Operation.createPut(u)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        this.host.completeIteration();
                    })
                    .setBody(body));
        }
        this.host.testWait();
        this.host.logThroughput();

        AtomicInteger assignedLinks = new AtomicInteger();
        this.host.testStart(initialStates.size());
        for (ServiceDocument s : initialStates) {
            // make sure the key is the path to the service. The initial state self link is not a
            // path ...
            String key = UriUtils.normalizeUriPath(s.documentSelfLink);
            s.documentSelfLink = key;
            SelectAndForwardRequest body = new SelectAndForwardRequest();
            body.key = key;
            Operation post = Operation.createPost(UriUtils.buildUri(nodeGroup,
                    ServiceUriPaths.DEFAULT_NODE_SELECTOR))
                    .setBody(body)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        synchronized (ownersPerServiceLink) {
                            SelectOwnerResponse rsp = o.getBody(SelectOwnerResponse.class);
                            List<String> links = ownersPerServiceLink.get(rsp.ownerNodeId);
                            if (links == null) {
                                links = new ArrayList<>();
                                ownersPerServiceLink.put(rsp.ownerNodeId, links);
                            }
                            links.add(key);
                            ownersPerServiceLink.put(rsp.ownerNodeId, links);
                        }
                        assignedLinks.incrementAndGet();
                        this.host.completeIteration();
                    });
            this.host.send(post);
        }
        this.host.testWait();

        assertTrue(assignedLinks.get() == initialStates.size());

        // verify the services on the node that should be owner, has modified state
        for (Entry<String, List<String>> e : ownersPerServiceLink.entrySet()) {
            String nodeId = e.getKey();
            List<String> links = e.getValue();
            NodeState ns = this.host.getNodeStateMap().get(nodeId);
            List<URI> uris = new ArrayList<>();
            // make a list of URIs to the services assigned to this peer node
            for (String l : links) {
                uris.add(UriUtils.buildUri(ns.groupReference, l));
            }

            Map<URI, MinimalTestServiceState> states = this.host.getServiceState(null,
                    MinimalTestServiceState.class, uris);
            for (MinimalTestServiceState s : states.values()) {
                // the PUT we issued, should have been forwarded to this service and modified its
                // initial ID to be the same as the self link
                if (!s.id.equals(s.documentSelfLink)) {
                    throw new IllegalStateException("Service forwarding failure");
                } else {
                }
            }
        }
    }

    @Test
    public void duplicatePostToFactory() throws Throwable {
        setUpPeers(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        // pick one host to post to
        VerificationHost serviceHost = this.host.getPeerHost();
        Consumer<Operation> setBodyCallback = (o) -> {
            ReplicationTestServiceState s = new ReplicationTestServiceState();
            s.stringField = UUID.randomUUID().toString();
            o.setBody(s);
        };

        URI factoryUri = this.host
                .getPeerServiceUri(ReplicationFactoryTestService.OWNER_SELECTION_SELF_LINK);
        Map<URI, ReplicationTestServiceState> states = doReplicatedServiceFactoryPost(
                this.serviceCount, setBodyCallback, factoryUri);

        serviceHost.testStart(states.size());
        ReplicationTestServiceState initialState = new ReplicationTestServiceState();

        for (URI uri : states.keySet()) {
            initialState.documentSelfLink = uri.toString().substring(uri.toString()
                    .lastIndexOf(UriUtils.URI_PATH_CHAR) + 1);
            Operation createPost = Operation
                    .createPost(factoryUri)
                    .setBody(initialState)
                    .setCompletion(
                            (o, e) -> {
                                if (o.getStatusCode() != Operation.STATUS_CODE_CONFLICT) {
                                    serviceHost.failIteration(
                                            new IllegalStateException(
                                                    "Incorrect response code received"));
                                    return;
                                }
                                serviceHost.completeIteration();
                            });
            serviceHost.send(createPost);
        }
        serviceHost.testWait();
    }

    @Test
    public void forwardingToPeerId() throws Throwable {
        setUpPeers(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        long c = this.updateCount * this.nodeCount;
        Map<String, URI> nodeIdToServiceURI = new HashMap<>();

        // 0) Create N service instances, in each peer host. Services are NOT replicated
        // 1) issue a forward request to a specific peer id, per service link
        // 2) verify the request ended up on the peer we targeted
        List<ServiceDocument> initialStates = new ArrayList<>();
        for (int i = 0; i < c; i++) {
            ServiceDocument s = this.host.buildMinimalTestState();
            s.documentSelfLink = UUID.randomUUID().toString();
            initialStates.add(s);
        }

        this.host.testStart(c * this.host.getPeerCount());
        for (VerificationHost peer : this.host.getInProcessHostMap().values()) {
            for (ServiceDocument s : initialStates) {
                Operation post = Operation.createPost(UriUtils.buildUri(peer, s.documentSelfLink))
                        .setCompletion(this.host.getCompletion())
                        .setBody(s);
                peer.startService(post, new MinimalTestService());
            }
        }
        this.host.testWait();

        VerificationHost peerEntryPoint = this.host.getPeerHost();

        // add a custom header and make sure the service sees it in its handler, in the request
        // headers, and we see a service response header in our response
        String headerName = MinimalTestService.TEST_HEADER_NAME;
        UUID id = UUID.randomUUID();
        String headerRequestValue = "request-" + id;
        String headerResponseValue = "response-" + id;
        List<ServiceDocument> documentsRemaining = new ArrayList<>(initialStates);
        this.host.testStart(documentsRemaining.size());
        while (!documentsRemaining.isEmpty()) {
            // round robin the service documents between the peers

            for (VerificationHost peer : this.host.getInProcessHostMap().values()) {
                if (documentsRemaining.isEmpty()) {
                    break;
                }

                ServiceDocument s = documentsRemaining.remove(0);
                // Create a URI for the service instance we expect the PUT to land on: It will be
                // on the peer we forward to, not the peer we forward *through*
                URI serviceUri = UriUtils.buildUri(peer.getUri(), s.documentSelfLink);
                // For testing coverage, force the use of the same forwarding service instance.
                // We make all request flow from one peer to others, testing both loopback p2p
                // and true forwarding. Otherwise, the forwarding happens by directly contacting
                // peer we want to land on!
                URI localForwardingUri = UriUtils.buildUri(peerEntryPoint.getUri(),
                        s.documentSelfLink);
                // add a query to make sure it does not affect forwarding
                localForwardingUri = UriUtils.extendUriWithQuery(localForwardingUri, "k", "v",
                        "k1", "v1", "k2", "v2");
                URI u = UriUtils.buildForwardToPeerUri(localForwardingUri, peer.getId(),
                        ServiceUriPaths.DEFAULT_NODE_SELECTOR, EnumSet.noneOf(ServiceOption.class));
                MinimalTestServiceState body = (MinimalTestServiceState) this.host
                        .buildMinimalTestState();
                body.id = peer.getId();

                nodeIdToServiceURI.put(peer.getId(), serviceUri);
                this.host.send(Operation.createPut(u)
                        .addRequestHeader(headerName, headerRequestValue)
                        .setCompletion(
                                (o, e) -> {
                                    if (e != null) {
                                        this.host.failIteration(e);
                                        return;
                                    }
                                    String value = o.getResponseHeader(headerName);
                                    if (value == null || !value.equals(headerResponseValue)) {
                                        this.host.failIteration(new IllegalArgumentException(
                                                "response header not found"));
                                        return;
                                    }
                                    this.host.completeIteration();
                                })
                        .setBody(body));
            }
        }
        this.host.testWait();
        this.host.logThroughput();
        Map<URI, MinimalTestServiceState> states = this.host.getServiceState(null,
                MinimalTestServiceState.class, nodeIdToServiceURI.values());
        for (MinimalTestServiceState s : states.values()) {
            NodeState ns = this.host.getNodeStateMap().get(s.id);
            if (ns == null) {
                throw new IllegalStateException("host id not valid for " + s.documentSelfLink);
            }
            URI expectedServiceUri = nodeIdToServiceURI.get(s.id);
            if (expectedServiceUri == null
                    || !expectedServiceUri.getPath().equals(s.documentSelfLink)) {
                throw new IllegalStateException("host id not valid for " + s.documentSelfLink);
            }
        }

        // Do a negative test: Send a request that will induce failure in the service handler and
        // make sure we receive back failure, with a ServiceErrorResponse body

        this.host.toggleDebuggingMode(true);
        this.host.testStart(this.host.getInProcessHostMap().size());
        for (VerificationHost peer : this.host.getInProcessHostMap().values()) {
            ServiceDocument s = initialStates.get(0);
            URI serviceUri = UriUtils.buildUri(peerEntryPoint.getUri(), s.documentSelfLink);
            URI u = UriUtils.buildForwardToPeerUri(serviceUri, peer.getId(),
                    ServiceUriPaths.DEFAULT_NODE_SELECTOR,
                    null);
            MinimalTestServiceState body = (MinimalTestServiceState) this.host
                    .buildMinimalTestState();
            // setting id to null will cause validation failure.
            body.id = null;
            this.host.send(Operation.createPut(u)
                    .setCompletion(
                            (o, e) -> {
                                if (e == null) {
                                    this.host.failIteration(new IllegalStateException(
                                            "expected failure"));
                                    return;
                                }
                                ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                                if (rsp.message == null || rsp.message.isEmpty()) {
                                    this.host.failIteration(new IllegalStateException(
                                            "expected error response body"));
                                    return;
                                }
                                this.host.completeIteration();
                            }).setBody(body));
        }
        this.host.testWait();
        this.host.toggleDebuggingMode(false);
    }

    @Test
    public void directOwnerSelection() throws Throwable {
        setUpPeers(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        Map<URI, Map<String, Long>> keyToNodeAssignmentsPerNode = new HashMap<>();
        Set<String> keys = new HashSet<>();

        long c = this.updateCount * this.nodeCount;
        // generate N keys once, then ask each node to assign to its peers. Each node should come up
        // with the same distribution

        for (int i = 0; i < c; i++) {
            keys.add(UUID.randomUUID().toString());
        }

        for (URI nodeGroup : this.host.getNodeGroupMap().values()) {
            keyToNodeAssignmentsPerNode.put(nodeGroup, new HashMap<>());
        }

        this.host.testStart(c * this.nodeCount);
        for (URI nodeGroup : this.host.getNodeGroupMap().values()) {
            for (String key : keys) {
                SelectAndForwardRequest body = new SelectAndForwardRequest();
                body.key = key;
                Operation post = Operation
                        .createPost(UriUtils.buildUri(nodeGroup,
                                ServiceUriPaths.DEFAULT_NODE_SELECTOR))
                        .setBody(body)
                        .setCompletion(
                                (o, e) -> {
                                    try {
                                        synchronized (keyToNodeAssignmentsPerNode) {
                                            SelectOwnerResponse rsp = o
                                                    .getBody(SelectOwnerResponse.class);
                                            Map<String, Long> assignmentsPerNode = keyToNodeAssignmentsPerNode
                                                    .get(nodeGroup);
                                            Long l = assignmentsPerNode.get(rsp.ownerNodeId);
                                            if (l == null) {
                                                l = 0L;
                                            }
                                            assignmentsPerNode.put(rsp.ownerNodeId, l + 1);
                                            this.host.completeIteration();
                                        }
                                    } catch (Throwable ex) {
                                        this.host.failIteration(ex);
                                    }
                                });
                this.host.send(post);
            }
        }
        this.host.testWait();
        this.host.logThroughput();

        Map<String, Long> countPerNode = null;

        for (URI nodeGroup : this.host.getNodeGroupMap().values()) {
            Map<String, Long> assignmentsPerNode = keyToNodeAssignmentsPerNode.get(nodeGroup);
            if (countPerNode == null) {
                countPerNode = assignmentsPerNode;
            }
            for (Entry<String, Long> e : assignmentsPerNode.entrySet()) {
                // we assume that with random keys, and random node ids, each node will get at least
                // one key.
                assertTrue(e.getValue() > 0);
                this.host.log("Node group %s assigned node id %s %d keys", nodeGroup, e.getKey(),
                        e.getValue());

                if (!countPerNode.get(e.getKey()).equals(e.getValue())) {
                    throw new IllegalStateException(
                            "Node id got assigned the same key different number of times, on one of the nodes");
                }
            }

        }
    }

    @Test
    public void synchronizationWithNodeStartStopStartManualSync() throws Throwable {
        this.isPeerSynchronizationEnabled = false;
        doSynchNodeStartStopStart();
    }

    @Test
    public void synchronizationWithNodeStartStopStart() throws Throwable {
        this.isPeerSynchronizationEnabled = true;
        doSynchNodeStartStopStart();
    }

    private void doSynchNodeStartStopStart() throws Throwable {

        int additionalHostCount = Math.min(this.nodeCount, 2);
        this.serviceCount = Math.max((this.nodeCount + additionalHostCount) * 3, this.serviceCount);

        setUpPeers(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.host.getPeerCount());

        Consumer<Operation> setBodyCallback = (o) -> {
            ReplicationTestServiceState s = new ReplicationTestServiceState();
            s.stringField = UUID.randomUUID().toString();
            o.setBody(s);
        };

        URI hostUri = this.host.getPeerServiceUri(null);

        URI factoryUri = UriUtils.buildUri(hostUri,
                ReplicationFactoryTestService.OWNER_SELECTION_SELF_LINK);

        // set the factory we use across nodes, so various helpers know what factory link to use
        this.replicationTargetFactoryLink = ReplicationFactoryTestService.OWNER_SELECTION_SELF_LINK;

        Map<URI, ReplicationTestServiceState> childStates =
                doReplicatedServiceFactoryPost(this.serviceCount, setBodyCallback, factoryUri);
        Map<String, ReplicationTestServiceState> childStatesPerLink = new HashMap<>();
        for (ReplicationTestServiceState st : childStates.values()) {
            childStatesPerLink.put(st.documentSelfLink, st);
        }

        Set<String> ownerIds = new HashSet<>();

        Map<String, String> linkToOwnerInitial = verifyDocumentOwnerIdAssignment(
                childStatesPerLink, ownerIds);

        if (this.host.isRemotePeerTest()) {
            return;
        }

        verifyOperationJoinAcrossPeers(childStates);

        updateEpoch(childStates, 1L);

        Collection<VerificationHost> originalHosts = new ArrayList<>(this.host
                .getInProcessHostMap().values());
        URI existingHostNodeGroup = this.host.getPeerNodeGroupUri();
        // start some additional nodes

        List<ServiceHost> newHosts = Collections.synchronizedList(new ArrayList<>());
        this.host.testStart(additionalHostCount);
        for (int i = 0; i < additionalHostCount; i++) {
            this.host.run(() -> {
                try {
                    this.host.setUpLocalPeerHost(newHosts, DEFAULT_MAINT_INTERVAL_MICROS);
                } catch (Throwable e) {
                    this.host.failIteration(e);
                }
            });
        }
        this.host.testWait();

        Set<String> newOwnerIds = new HashSet<>();
        this.host.testStart(newHosts.size());
        for (ServiceHost h : newHosts) {
            setUpPeerHostWithAdditionalServices((VerificationHost) h);
            URI newNodeGroup = UriUtils.buildUri(h, ServiceUriPaths.DEFAULT_NODE_GROUP);
            newOwnerIds.add(h.getId());
            this.host.joinNodeGroup(newNodeGroup, existingHostNodeGroup, additionalHostCount
                    + this.nodeCount);
        }

        this.host.testWait();

        this.host.waitForNodeGroupConvergence(this.host.getNodeGroupMap().size());

        scheduleSynchronizationIfAutoSyncDisabled();

        waitForReplicatedFactoryChildServiceConvergence(childStatesPerLink,
                this.replicationServiceStatePostUpdateConvergenceChecker,
                this.serviceCount, 0);

        childStatesPerLink = doStateUpdateReplicationTest(Action.PATCH, this.serviceCount,
                this.updateCount,
                this.updateCount,
                this.replicationServiceStateUpdateBodySetter,
                this.replicationServiceStatePostUpdateConvergenceChecker,
                childStatesPerLink);

        Set<String> combinedOwnerIds = new HashSet<>();
        combinedOwnerIds.addAll(newOwnerIds);
        combinedOwnerIds.addAll(ownerIds);

        Map<String, String> linkToOwnerAfterNodeJoin = verifyDocumentOwnerIdAssignment(
                childStatesPerLink, combinedOwnerIds);

        int minOwnerChangeCount = 0;
        minOwnerChangeCount = computeOwnershipChangeCount(linkToOwnerInitial,
                linkToOwnerAfterNodeJoin);

        verifyDocumentOwnerAndEpoch(childStatesPerLink, combinedOwnerIds, 0,
                childStatesPerLink.size() * 2, minOwnerChangeCount);

        // negative tests that abruptly stop nodes should set operation timeout smaller than the test
        // timeout, so any node to node gossip I/O times out quickly and test can proceed
        long testTimeoutMicros = TimeUnit.SECONDS.toMicros(this.host.getTimeoutSeconds());
        setOperationTimeoutMicros(Math.max(testTimeoutMicros / 3, TimeUnit.SECONDS.toMicros(10)));

        // node member expiration can be set through PATCH on each node node group service
        this.nodeGroupConfig.nodeRemovalDelayMicros = TimeUnit.SECONDS.toMicros(1);
        this.host.setNodeGroupConfig(this.nodeGroupConfig);

        // Do the inverse test. Remove all of the original hosts and this time, expect all the
        // documents have owners assigned to the new hosts
        for (VerificationHost h : originalHosts) {
            this.host.stopHost(h);
        }

        // verify nodes expire and removed entirely from the group state
        this.host.waitForNodeGroupConvergence(this.host.getNodeGroupMap().size(), this.host
                .getNodeGroupMap().size());

        childStatesPerLink = doStateUpdateReplicationTest(Action.PATCH, this.serviceCount,
                this.updateCount,
                this.updateCount * 2,
                this.replicationServiceStateUpdateBodySetter,
                this.replicationServiceStatePostUpdateConvergenceChecker,
                childStatesPerLink);

        Map<String, String> linkToOwnerAfterNodeStop = verifyDocumentOwnerIdAssignment(
                childStatesPerLink, newOwnerIds);

        minOwnerChangeCount = computeOwnershipChangeCount(linkToOwnerAfterNodeJoin,
                linkToOwnerAfterNodeStop);

        verifyDocumentOwnerAndEpoch(childStatesPerLink, newOwnerIds, 0,
                childStatesPerLink.size() * 2, minOwnerChangeCount);

        this.host.log("Starting another set of new nodes");
        existingHostNodeGroup = this.host.getPeerNodeGroupUri();

        newHosts.clear();
        this.host.testStart(additionalHostCount);
        for (int i = 0; i < additionalHostCount; i++) {
            this.host.run(() -> {
                try {
                    this.host.setUpLocalPeerHost(newHosts, DEFAULT_MAINT_INTERVAL_MICROS);
                } catch (Throwable e) {
                    this.host.failIteration(e);
                }
            });
        }
        this.host.testWait();

        int synchQuorum = this.host.getPeerCount();
        this.host.testStart(newHosts.size());
        for (ServiceHost h : newHosts) {
            setUpPeerHostWithAdditionalServices((VerificationHost) h);
            newOwnerIds.add(h.getId());
            URI newNodeGroup = UriUtils.buildUri(h, ServiceUriPaths.DEFAULT_NODE_GROUP);
            this.host.joinNodeGroup(newNodeGroup, existingHostNodeGroup, synchQuorum);
        }
        this.host.testWait();
        this.host.waitForNodeGroupConvergence(this.host.getNodeGroupMap().size());

        if (!this.isPeerSynchronizationEnabled) {
            for (ServiceHost h : this.host.getInProcessHostMap().values()) {
                // for added coverage, trigger maintenance by issuing a PATCH to the management service,
                // its equivalent to calling the host method
                SynchronizeWithPeersRequest r = SynchronizeWithPeersRequest
                        .create(ServiceUriPaths.DEFAULT_NODE_SELECTOR);
                this.host.send(Operation.createPatch(
                        UriUtils.buildUri(h, ServiceHostManagementService.SELF_LINK)).setBody(r));
                verifyPendingChildServiceSynchStats(
                        UriUtils.buildUri(h.getUri(), factoryUri.getPath()),
                        0);
            }
        }

        combinedOwnerIds.clear();
        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            combinedOwnerIds.add(h.getId());
        }

        waitForReplicatedFactoryChildServiceConvergence(childStatesPerLink,
                this.replicationServiceStatePostUpdateConvergenceChecker,
                this.serviceCount, 0);

        childStatesPerLink = doStateUpdateReplicationTest(Action.PATCH, this.serviceCount,
                this.updateCount,
                this.updateCount * 3,
                this.replicationServiceStateUpdateBodySetter,
                this.replicationServiceStatePostUpdateConvergenceChecker,
                childStatesPerLink);

        Map<String, String> linkToOwnerAfterNodeJoinSecondTime = verifyDocumentOwnerIdAssignment(
                childStatesPerLink, combinedOwnerIds);

        minOwnerChangeCount = computeOwnershipChangeCount(linkToOwnerAfterNodeStop,
                linkToOwnerAfterNodeJoinSecondTime);

        verifyDocumentOwnerAndEpoch(childStatesPerLink, newOwnerIds, 0,
                childStatesPerLink.size() * 2, minOwnerChangeCount);
        this.host.logNodeGroupStats();
    }

    private int computeOwnershipChangeCount(Map<String, String> linkToOwnerAfterNodeJoin,
            Map<String, String> linkToOwnerAfterNodeStop) {
        int expectedOwnerChanges = 0;
        Map<String, String> before = linkToOwnerAfterNodeJoin;
        Map<String, String> after = linkToOwnerAfterNodeStop;
        for (Entry<String, String> e : before.entrySet()) {
            String link = e.getKey();
            String originalOwner = e.getValue();
            String newOwner = after.get(link);
            if (!newOwner.equals(originalOwner)) {
                expectedOwnerChanges++;
                this.host.log("%s changed from %s to %s", link, originalOwner, newOwner);
            }
        }
        return expectedOwnerChanges;
    }

    private void verifyPendingChildServiceSynchStats(URI factoryUri, int expectedChildrenPending)
            throws Throwable {
        URI statsUri = UriUtils.buildStatsUri(factoryUri);
        Date exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {
            ServiceStats stats = this.host.getServiceState(null, ServiceStats.class, statsUri);
            ServiceStat pendingSynchStat = stats.entries
                    .get(Service.STAT_NAME_NODE_GROUP_CHANGE_PENDING_MAINTENANCE_COUNT);
            if (pendingSynchStat == null) {
                if (expectedChildrenPending == 0) {
                    return;
                }

            }

            if (pendingSynchStat.latestValue == expectedChildrenPending) {
                return;
            }

            this.host.log("Not converged, expected %d, stat: %f", expectedChildrenPending,
                    pendingSynchStat.latestValue);

            Thread.sleep(250);
            continue;
        }
    }

    private void verifyOperationJoinAcrossPeers(Map<URI, ReplicationTestServiceState> childStates)
            throws Throwable {
        // do a OperationJoin across N nodes, making sure join works when forwarding is involved
        List<Operation> joinedOps = new ArrayList<>();
        for (ReplicationTestServiceState st : childStates.values()) {
            Operation get = Operation.createGet(
                    this.host.getPeerServiceUri(st.documentSelfLink)).setReferer(
                    this.host.getReferer());
            joinedOps.add(get);
        }

        this.host.testStart(1);
        OperationJoin
                .create(joinedOps)
                .setCompletion(
                        (ops, exc) -> {
                            if (exc != null) {
                                this.host.failIteration(exc.values().iterator().next());
                                return;
                            }

                            for (Operation o : ops.values()) {
                                ReplicationTestServiceState state = o.getBody(
                                        ReplicationTestServiceState.class);
                                if (state.stringField == null) {
                                    this.host.failIteration(new IllegalStateException());
                                    return;
                                }
                            }
                            this.host.completeIteration();
                        }).sendWith(this.host.getPeerHost());
        this.host.testWait();
    }

    private void updateEpoch(Map<URI, ReplicationTestServiceState> childStates, long newEpoch)
            throws Throwable {
        this.host.testStart(childStates.size());
        for (URI childUri : childStates.keySet()) {
            URI childServiceConfigUri = UriUtils.buildConfigUri(childUri);
            // increment epoch to 1, otherwise node synchronization will skip epoch update if all services
            // on all peers, have zero or null documentEpoch
            ServiceConfigUpdateRequest body = ServiceConfigUpdateRequest.create();
            body.epoch = newEpoch;
            URI broadcastUri = UriUtils.buildBroadcastRequestUri(childServiceConfigUri,
                    ServiceUriPaths.DEFAULT_NODE_SELECTOR);
            Operation epochUpdate = Operation
                    .createPatch(broadcastUri)
                    .setBody(body)
                    .setCompletion(this.host.getCompletion());
            this.host.send(epochUpdate);
        }
        this.host.testWait();
        this.host.log("Sending PUT to force epoch indexing");
        // Epoch updates do not cause re-indexing. So if we try add new nodes, they will broadcast GETs to the index
        // and not see the new epoch.
        // They normally happen as part of sync, which follows through with state update
        // so here we do an additional update to persist the epoch in the index as part of the new state version
        this.host.testStart(childStates.size());
        for (Entry<URI, ReplicationTestServiceState> e : childStates.entrySet()) {
            ReplicationTestServiceState body = e.getValue();
            body.stringField = UUID.randomUUID().toString();
            // increment epoch to 1, otherwise node synchronization will skip epoch update if all services
            // on all peers, have zero or null documentEpoch
            Operation put = Operation
                    .createPut(e.getKey())
                    .setBody(body)
                    .setCompletion(this.host.getCompletion());
            this.host.send(put);
        }

        this.host.testWait();
    }

    private Map<String, String> verifyDocumentOwnerIdAssignment(
            Map<String, ReplicationTestServiceState> childStates,
            Set<String> ownerIds) throws Throwable {

        Map<String, String> linkToOwner = new HashMap<>();
        for (ReplicationTestServiceState state : childStates.values()) {
            assertTrue(state.documentOwner != null);
            ownerIds.add(state.documentOwner);
            linkToOwner.put(state.documentSelfLink, state.documentOwner);
        }

        // verify the owner node IDs each document got assigned to, are healthy,
        // real, nodes
        Map<URI, NodeGroupState> nodesPerHost = new HashMap<>();
        this.host.testStart(this.host.getNodeGroupMap().size());
        assertTrue(!this.host.getNodeGroupMap().isEmpty());
        for (URI nodeGroup : this.host.getNodeGroupMap().values()) {
            this.host.getNodeState(nodeGroup, nodesPerHost);
        }
        this.host.testWait();

        for (String assignedOwnerId : ownerIds) {
            boolean foundNodeOwner = false;
            for (Entry<URI, NodeGroupState> entry : nodesPerHost
                    .entrySet()) {
                NodeGroupState rsp = entry.getValue();
                for (NodeState node : rsp.nodes.values()) {
                    if (node.status == NodeStatus.AVAILABLE
                            && node.id.equals(assignedOwnerId)) {
                        foundNodeOwner = true;
                    }
                }
            }
            assertTrue(foundNodeOwner);
        }
        return linkToOwner;
    }

    public Map<String, Set<String>> computeOwnerIdsPerLink(Collection<String> links)
            throws Throwable {
        Map<String, Set<String>> expectedOwnersPerLink = new ConcurrentSkipListMap<>();
        // we need to independently verify which host should have been assigned the service links
        VerificationHost h = this.host.getPeerHost();
        if (h == null) {
            return expectedOwnersPerLink;
        }
        CompletionHandler c = (o, e) -> {
            if (e != null) {
                this.host.failIteration(e);
                return;
            }

            SelectOwnerResponse rsp = o.getBody(SelectOwnerResponse.class);
            Set<String> eligibleNodeIds = new HashSet<>();
            for (NodeState ns : rsp.selectedNodes) {
                eligibleNodeIds.add(ns.id);
            }
            expectedOwnersPerLink.put(rsp.key, eligibleNodeIds);
            this.host.completeIteration();
        };

        this.host.testStart(links.size());
        for (String link : links) {
            Operation selectOp = Operation.createGet(null)
                    .setCompletion(c)
                    .setExpiration(this.host.getOperationTimeoutMicros() + Utils.getNowMicrosUtc());

            h.selectOwner(this.replicationNodeSelector, link, selectOp);
        }
        this.host.testWait();
        return expectedOwnersPerLink;
    }

    public <T extends ServiceDocument> void verifyDocumentOwnerAndEpoch(Map<String, T> childStates,
            Set<String> ownerIds, int minExpectedEpochRetries,
            int maxExpectedEpochRetries, int expectedOwnerChanges)
                    throws Throwable, InterruptedException, TimeoutException {

        Map<String, Set<String>> ownerIdsPerLink = computeOwnerIdsPerLink(childStates.keySet());
        Date exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {
            boolean isConverged = true;
            Map<String, Set<Long>> epochsPerLink = new HashMap<>();
            List<URI> nodeSelectorStatsUris = new ArrayList<>();

            for (URI baseNodeUri : this.host.getNodeGroupMap().keySet()) {
                nodeSelectorStatsUris.add(UriUtils.buildUri(baseNodeUri,
                        ServiceUriPaths.DEFAULT_NODE_SELECTOR,
                        ServiceHost.SERVICE_URI_SUFFIX_STATS));

                URI factoryUri = UriUtils.buildExpandLinksQueryUri(UriUtils.buildUri(
                        baseNodeUri, this.replicationTargetFactoryLink));
                ServiceDocumentQueryResult factoryRsp = this.host
                        .getFactoryState(factoryUri);
                if (factoryRsp.documents == null
                        || factoryRsp.documents.size() != childStates.size()) {
                    isConverged = false;
                    // services not all started in new nodes yet;
                    break;
                }

                List<URI> childStatUris = new ArrayList<>();
                for (Object s : factoryRsp.documents.values()) {
                    ServiceDocument state = Utils.fromJson(s, ServiceDocument.class);

                    if (state.documentOwner == null) {
                        this.host.log("Owner not set in service on new node: %s",
                                Utils.toJsonHtml(s));
                        isConverged = false;
                        break;
                    }

                    URI statUri = UriUtils.buildUri(baseNodeUri, state.documentSelfLink,
                            ServiceHost.SERVICE_URI_SUFFIX_STATS);
                    childStatUris.add(statUri);

                    Set<Long> epochs = epochsPerLink.get(state.documentEpoch);
                    if (epochs == null) {
                        epochs = new HashSet<>();
                        epochsPerLink.put(state.documentSelfLink, epochs);
                    }

                    epochs.add(state.documentEpoch);
                    Set<String> eligibleNodeIds = ownerIdsPerLink.get(state.documentSelfLink);

                    if (!ownerIds.contains(state.documentOwner)) {
                        this.host.log("Owner id for %s not expected: %s, valid ids: %s",
                                state.documentSelfLink, state.documentOwner, ownerIds);
                        isConverged = false;
                        break;
                    }

                    if (eligibleNodeIds != null && !eligibleNodeIds.contains(state.documentOwner)) {
                        this.host.log("Owner id for %s not eligible: %s, eligible ids: %s",
                                state.documentSelfLink, state.documentOwner, ownerIds);
                        isConverged = false;
                        break;
                    }
                }

                int nodeGroupMaintCount = 0;
                int missingDocOwnerToggleCount = 0;
                int docOwnerToggleCount = 0;
                // verify stats were reported by owner node, not a random peer
                Map<URI, ServiceStats> allChildStats = this.host.getServiceState(null,
                        ServiceStats.class, childStatUris);
                for (ServiceStats childStats : allChildStats.values()) {
                    String parentLink = UriUtils.getParentPath(childStats.documentSelfLink);
                    Set<String> eligibleNodes = ownerIdsPerLink.get(parentLink);
                    if (!eligibleNodes.contains(childStats.documentOwner)) {
                        this.host.log("Stats for %s owner not expected. Is %s, should be %s",
                                parentLink, childStats.documentOwner,
                                ownerIdsPerLink.get(parentLink));
                        isConverged = false;
                        break;
                    }
                    ServiceStat maintStat = childStats.entries
                            .get(Service.STAT_NAME_NODE_GROUP_CHANGE_MAINTENANCE_COUNT);
                    if (maintStat != null) {
                        nodeGroupMaintCount++;
                    }
                    ServiceStat missingToggleStat = childStats.entries
                            .get(ReplicationTestService.STAT_NAME_MISSING_SERVICE_OPTION_TOGGLE_COUNT);
                    if (missingToggleStat != null) {
                        missingDocOwnerToggleCount++;
                    }
                    ServiceStat docOwnerToggleStat = childStats.entries
                            .get(ReplicationTestService.STAT_NAME_SERVICE_OPTION_TOGGLE_COUNT);
                    if (docOwnerToggleStat != null) {
                        docOwnerToggleCount++;
                    }
                }

                this.host.log("Node group change maintenance observed: %d", nodeGroupMaintCount);
                if (nodeGroupMaintCount < expectedOwnerChanges) {
                    isConverged = false;
                }

                this.host.log("Failed doc owner count: %d, toggle count: %d",
                        missingDocOwnerToggleCount, docOwnerToggleCount);
                if (docOwnerToggleCount < expectedOwnerChanges) {
                    isConverged = false;
                }

                if (missingDocOwnerToggleCount > 0) {
                    throw new IllegalStateException(
                            "Missing service option config details in maintenance for service");
                }

                // verify epochs
                for (Set<Long> epochs : epochsPerLink.values()) {
                    if (epochs.size() > 1) {
                        this.host.log("Documents have different epochs:%s", epochs.toString());
                        isConverged = false;
                    }
                }

                if (!isConverged) {
                    break;
                }
            }

            if (!isConverged) {
                Thread.sleep(1000);
                continue;
            }

            // get stats from each node selector and verify epoch retries
            Map<URI, ServiceStats> allNodeSelectorStats = new ConcurrentSkipListMap<>();
            this.host.testStart(nodeSelectorStatsUris.size());
            for (URI statUri : nodeSelectorStatsUris) {
                Operation get = Operation.createGet(statUri)
                        .setCompletion((o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }

                            ServiceStats st = o.getBody(ServiceStats.class);
                            allNodeSelectorStats.put(o.getUri(), st);
                            this.host.completeIteration();
                        });
                this.host.send(get);
            }

            this.host.testWait();

            long totalRetries = 0;
            for (ServiceStats stats : allNodeSelectorStats.values()) {
                ServiceStat epochRetry = stats.entries.get(
                        NodeSelectorSynchronizationService.STAT_NAME_EPOCH_INCREMENT_RETRY_COUNT);
                if (epochRetry == null) {
                    continue;
                }
                totalRetries += epochRetry.latestValue;
            }

            this.host.log("Total epoch retries: %d", totalRetries);
            if (totalRetries > maxExpectedEpochRetries * 2
                    || totalRetries < minExpectedEpochRetries) {
                throw new IllegalStateException("Too many or too few epoch retries");
            }

            break;
        }

        if (new Date().after(exp)) {
            throw new TimeoutException();
        }
    }

    private <T extends ServiceDocument> Map<String, T> doStateUpdateReplicationTest(Action action,
            int childCount, int updateCount,
            long expectedVersion,
            Function<T, Void> updateBodySetter,
            BiPredicate<T, T> convergenceChecker,
            Map<String, T> initialStatesPerChild) throws Throwable {
        this.host.testStart("Replicated " + action, null, childCount * updateCount);

        AtomicInteger failedCount = new AtomicInteger();
        // issue an update to each child service and verify it was reflected
        // among
        // peers
        for (T initState : initialStatesPerChild.values()) {
            // change a field in the initial state of each service but keep it
            // the same across all updates so potential re ordering of the
            // updates does not cause spurious test breaks
            updateBodySetter.apply(initState);

            for (int i = 0; i < updateCount; i++) {

                long sentTime = Utils.getNowMicrosUtc();
                String contextId = sentTime + "";
                URI factoryOnRandomPeerUri = this.host
                        .getPeerServiceUri(this.replicationTargetFactoryLink);

                this.host.send(Operation
                        .createPatch(UriUtils.buildUri(factoryOnRandomPeerUri.getHost(),
                                factoryOnRandomPeerUri.getPort(),
                                initState.documentSelfLink, null))
                        .setAction(action)
                        .setContextId(contextId)
                        .forceRemote()
                        .setBody(initState)
                        .setCompletion(
                                (o, e) -> {
                                    if (e != null) {
                                        if (this.expectFailure) {
                                            failedCount.incrementAndGet();
                                            this.host.completeIteration();
                                            return;
                                        }
                                        this.host.failIteration(e);
                                        return;
                                    }

                                    if (this.expectFailure
                                            && this.expectedFailureStartTimeMicros > 0
                                            && sentTime > this.expectedFailureStartTimeMicros) {
                                        this.host.failIteration(new IllegalStateException(
                                                "Request should have failed: %s" + o.toString()
                                                        + " sent at " + sentTime));
                                        return;
                                    }
                                    this.host.completeIteration();
                                }));
            }
        }
        this.host.testWait();
        this.host.logThroughput();

        if (this.expectFailure) {
            this.host.log("Failed count: %d", failedCount.get());
            if (failedCount.get() == 0) {
                throw new IllegalStateException(
                        "Possible false negative but expected at least one failure");
            }
            return initialStatesPerChild;
        }

        // All updates sent to all children within one host, now wait for
        // convergence
        if (action != Action.DELETE) {
            return waitForReplicatedFactoryChildServiceConvergence(initialStatesPerChild,
                    convergenceChecker,
                    childCount,
                    expectedVersion);
        }

        // for DELETE replication, we expect the child services to be stopped
        // across hosts and their state marked "deleted". So confirm no children
        // are available
        return waitForReplicatedFactoryChildServiceConvergence(initialStatesPerChild,
                convergenceChecker,
                0,
                expectedVersion);
    }

    private Map<String, ExampleServiceState> doExampleFactoryPostReplicationTest(int childCount)
            throws Throwable, TimeoutException {
        return doExampleFactoryPostReplicationTest(childCount, null);
    }

    private Map<String, ExampleServiceState> doExampleFactoryPostReplicationTest(int childCount,
            EnumSet<TestProperty> props) throws Throwable {

        if (props == null) {
            props = EnumSet.noneOf(TestProperty.class);
        }

        if (this.host == null) {
            setUpPeers(this.nodeCount);
            this.host.joinNodesAndVerifyConvergence(this.host.getPeerCount());
        }

        if (props.contains(TestProperty.FORCE_FAILURE)) {
            this.host.toggleNegativeTestMode(true);
        }

        this.host.log("%s: Starting replication", this.host.buildTestNameFromStack());

        String factoryPath = this.replicationTargetFactoryLink;
        long[] startAndEndTime = new long[2];

        startAndEndTime[0] = System.nanoTime();
        Map<String, ExampleServiceState> serviceStates = new HashMap<>();
        this.host.testStart(childCount);
        for (int i = 0; i < childCount; i++) {
            URI factoryOnRandomPeerUri = this.host.getPeerServiceUri(factoryPath);
            Operation post = Operation
                    .createPost(factoryOnRandomPeerUri)
                    .setCompletion(this.host.getCompletion());

            ExampleServiceState initialState = new ExampleServiceState();
            initialState.name = "" + post.getId();
            initialState.counter = Long.MIN_VALUE;

            // set the self link as a hint so the child service URI is
            // predefined instead of random
            initialState.documentSelfLink = "" + post.getId();

            // factory service is started on all hosts. Now issue a POST to one,
            // to create a child service with some initial state.
            post.setReferer(this.host.getReferer());
            this.host.sendRequest(post.setBody(initialState));

            // initial state is cloned and sent, now we can change self link per
            // child to reflect its runtime URI, which will
            // contain the factory service path
            initialState.documentSelfLink = UriUtils.buildUriPath(factoryPath,
                    initialState.documentSelfLink);
            serviceStates.put(initialState.documentSelfLink, initialState);
        }

        if (props.contains(TestProperty.FORCE_FAILURE)) {
            // do not wait for convergence of the child services. Instead
            // proceed to the next test which is probably stopping hosts
            // abruptly
            return serviceStates;
        }

        this.host.testWait();

        return waitForReplicatedFactoryChildServiceConvergence(serviceStates,
                this.exampleStateConvergenceChecker,
                childCount,
                0L);
    }

    private void updateExampleServiceOptions(
            Map<String, ExampleServiceState> statesPerSelfLink) throws Throwable {
        if (this.postCreationServiceOptions == null || this.postCreationServiceOptions.isEmpty()) {
            return;
        }
        this.host.testStart(statesPerSelfLink.size());
        URI nodeGroup = this.host.getNodeGroupMap().values().iterator().next();
        for (String link : statesPerSelfLink.keySet()) {
            URI bUri = UriUtils.buildBroadcastRequestUri(
                    UriUtils.buildUri(nodeGroup, link, ServiceHost.SERVICE_URI_SUFFIX_CONFIG),
                    ServiceUriPaths.DEFAULT_NODE_SELECTOR);
            ServiceConfigUpdateRequest cfgBody = ServiceConfigUpdateRequest.create();
            cfgBody.addOptions = this.postCreationServiceOptions;
            this.host.send(Operation.createPatch(bUri)
                    .setBody(cfgBody)
                    .setCompletion(this.host.getCompletion()));

        }
        this.host.testWait();
    }

    private <T extends ServiceDocument> Map<String, T> waitForReplicatedFactoryChildServiceConvergence(
            Map<String, T> serviceStates,
            BiPredicate<T, T> stateChecker,
            int expectedChildCount, long expectedVersion)
            throws Throwable, TimeoutException {

        Class<?> stateType = serviceStates.values().iterator().next().getClass();

        // now poll all hosts until they converge: They all have a child service
        // with the expected URI and it has the same state

        Map<String, T> updatedStatesPerSelfLink = new HashMap<>();
        Date expiration = new Date(new Date().getTime()
                + TimeUnit.SECONDS.toMillis(this.host.getTimeoutSeconds()));
        do {
            Map<URI, URI> factories = getFactoriesPerNodeGroup(this.replicationTargetFactoryLink);
            URI node = factories.keySet().iterator().next();
            AtomicInteger getFailureCount = new AtomicInteger();
            if (expectedChildCount != 0) {
                // issue direct GETs to the services, we do not trust the factory
                this.host.testStart(serviceStates.size());
                for (String link : serviceStates.keySet()) {
                    this.host.send(Operation.createGet(UriUtils.buildUri(node, link))
                            .setCompletion(
                                    (o, e) -> {
                                        if (e != null) {
                                            getFailureCount.incrementAndGet();
                                        }
                                        this.host.completeIteration();
                                    }));
                }

                this.host.testWait();

            }

            if (getFailureCount.get() > 0) {
                this.host.log("Child services not propagated yet");
                Thread.sleep(500);
                continue;
            }

            this.host.testStart(factories.size());
            Map<URI, ServiceDocumentQueryResult> childServicesPerNode = new HashMap<>();
            for (URI remoteFactory : factories.values()) {
                URI factoryUriWithExpand = UriUtils.buildExpandLinksQueryUri(remoteFactory);
                Operation get = Operation.createGet(factoryUriWithExpand)
                        .setCompletion(
                                (o, e) -> {
                                    if (e != null) {
                                        this.host.completeIteration();
                                        return;
                                    }
                                    if (!o.hasBody()) {
                                        this.host.completeIteration();
                                        return;
                                    }
                                    ServiceDocumentQueryResult r = o
                                            .getBody(ServiceDocumentQueryResult.class);
                                    synchronized (childServicesPerNode) {
                                        childServicesPerNode.put(o.getUri(), r);
                                    }
                                    this.host.completeIteration();
                                });
                this.host.send(get);
            }

            this.host.testWait();

            long expectedNodeCountPerLink = this.host.getNodeGroupMap().size();
            if (this.replicationFactor != 0) {
                expectedNodeCountPerLink = this.replicationFactor;
            }

            if (expectedChildCount == 0) {
                expectedNodeCountPerLink = 0;
            }

            // build a service link to node map so we can tell on which node each service instance landed
            Map<String, Set<URI>> linkToNodeMap = new HashMap<>();

            for (Entry<URI, ServiceDocumentQueryResult> entry : childServicesPerNode.entrySet()) {
                for (String link : entry.getValue().documentLinks) {

                    if (!serviceStates.containsKey(link)) {
                        this.host.log("ignoring service %s, not part of this iteration", link);
                        continue;
                    }

                    Set<URI> hostsPerLink = linkToNodeMap.get(link);
                    if (hostsPerLink == null) {
                        hostsPerLink = new HashSet<>();
                    }
                    hostsPerLink.add(entry.getKey());
                    linkToNodeMap.put(link, hostsPerLink);
                }
            }

            boolean isConverged = true;
            // each link must exist on N hosts, where N is either the replication factor, or, if not used, all nodes
            for (Entry<String, Set<URI>> e : linkToNodeMap.entrySet()) {
                if (e.getValue() == null && this.replicationFactor == 0) {
                    this.host.log("Service %s not found on any nodes", e.getKey());
                    isConverged = false;
                    continue;
                }

                if (e.getValue().size() != expectedNodeCountPerLink) {
                    this.host.log("Service %s found on %d nodes, expected %d", e.getKey(), e
                            .getValue().size(), expectedNodeCountPerLink);
                    isConverged = false;
                    continue;
                }
            }

            if (!isConverged) {
                Thread.sleep(500);
                continue;
            }

            if (expectedChildCount == 0) {
                // DELETE test, all children removed from all hosts, we are done
                return updatedStatesPerSelfLink;
            }

            // we have the correct number of services on all hosts. Now verify
            // the state of each service matches what we expect

            isConverged = true;

            for (Entry<String, Set<URI>> entry : linkToNodeMap.entrySet()) {
                String selfLink = entry.getKey();
                int convergedNodeCount = 0;
                for (URI nodeUri : entry.getValue()) {
                    ServiceDocumentQueryResult childLinksAndDocsPerHost = childServicesPerNode
                            .get(nodeUri);
                    Object jsonState = childLinksAndDocsPerHost.documents.get(selfLink);
                    if (jsonState == null && this.replicationFactor == 0) {
                        this.host
                                .log("Service %s not present on host %s", selfLink, entry.getKey());
                        continue;
                    }

                    if (jsonState == null) {
                        continue;
                    }

                    T initialState = serviceStates.get(selfLink);

                    if (initialState == null) {
                        continue;
                    }

                    @SuppressWarnings("unchecked")
                    T stateOnNode = (T) Utils.fromJson(jsonState, stateType);
                    if (!stateChecker.test(initialState, stateOnNode)) {
                        this.host
                                .log("State for %s not converged on node %s. Current state:%s, Initial: %s",
                                        selfLink, nodeUri, Utils.toJsonHtml(stateOnNode),
                                        Utils.toJsonHtml(initialState));
                        break;
                    }

                    if (stateOnNode.documentVersion < expectedVersion) {
                        this.host
                                .log("Version (%d, expected %d) not converged, state: %s",
                                        stateOnNode.documentVersion,
                                        expectedVersion,
                                        Utils.toJsonHtml(stateOnNode));
                        break;
                    }

                    if (stateOnNode.documentEpoch == null) {
                        this.host.log("Epoch is missing, state: %s",
                                Utils.toJsonHtml(stateOnNode));
                        break;
                    }

                    // Do not check exampleState.counter, in this validation loop.
                    // We can not compare the counter since the replication test sends the updates
                    // in parallel, meaning some of them will get re-ordered and ignored due to
                    // version being out of date.

                    updatedStatesPerSelfLink.put(selfLink, stateOnNode);
                    convergedNodeCount++;
                }

                if (convergedNodeCount != expectedNodeCountPerLink) {
                    isConverged = false;
                    break;
                }
            }

            if (isConverged) {
                return updatedStatesPerSelfLink;
            }

            Thread.sleep(500);
        } while (new Date().before(expiration));

        throw new TimeoutException();
    }

    private List<ServiceHostState> stopHostsToSimulateFailure(int failedNodeCount) {
        int k = 0;
        List<ServiceHostState> stoppedHosts = new ArrayList<>();
        for (VerificationHost hostToStop : this.host.getInProcessHostMap().values()) {
            this.host.log("Stopping host %s", hostToStop);
            stoppedHosts.add(hostToStop.getState());
            this.host.stopHost(hostToStop);

            k++;
            if (k >= failedNodeCount) {
                break;
            }
        }
        return stoppedHosts;
    }
}
