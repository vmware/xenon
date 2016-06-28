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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.NodeSelectorService.SelectAndForwardRequest;
import com.vmware.xenon.common.NodeSelectorService.SelectOwnerResponse;
import com.vmware.xenon.common.NodeSelectorState;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ProcessingStage;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceConfigUpdateRequest;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceHost.HttpScheme;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.AuthorizationHelper;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.common.test.RoundRobinIterator;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestProperty;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.common.test.VerificationHost.WaitHandler;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.ExampleTaskService.ExampleTaskServiceState;
import com.vmware.xenon.services.common.MinimalTestService.MinimalTestServiceErrorResponse;
import com.vmware.xenon.services.common.NodeGroupService.JoinPeerRequest;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupConfig;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.NodeState.NodeOption;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Builder;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.xenon.services.common.ReplicationTestService.ReplicationTestServiceErrorResponse;
import com.vmware.xenon.services.common.ReplicationTestService.ReplicationTestServiceState;
import com.vmware.xenon.services.common.ResourceGroupService.ResourceGroupState;
import com.vmware.xenon.services.common.RoleService.RoleState;
import com.vmware.xenon.services.common.UserService.UserState;

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

    private static final String CUSTOM_EXAMPLE_SERVICE_KIND = "xenon:examplestate";
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
     * Command line argument specifying number of times to run the same test method.
     */
    public int testIterationCount = 1;

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

    /**
     * Command line argument specifying iterations per test method
     */
    public long iterationCount = 1;

    /**
     * Command line argument used by replication long running tests
     */
    public long totalOperationLimit = Long.MAX_VALUE;

    private NodeGroupConfig nodeGroupConfig = new NodeGroupConfig();
    private EnumSet<ServiceOption> postCreationServiceOptions = EnumSet.noneOf(ServiceOption.class);
    private boolean expectFailure;
    private long expectedFailureStartTimeMicros;
    private List<URI> expectedFailedHosts = new ArrayList<>();
    private String replicationTargetFactoryLink = ExampleService.FACTORY_LINK;
    private String replicationNodeSelector = ServiceUriPaths.DEFAULT_NODE_SELECTOR;
    private long replicationFactor;

    private BiPredicate<ExampleServiceState, ExampleServiceState> exampleStateConvergenceChecker = (
            initial, current) -> {
        if (current.name == null) {
            return false;
        }
        if (!this.host.isRemotePeerTest() &&
                !CUSTOM_EXAMPLE_SERVICE_KIND.equals(current.documentKind)) {
            return false;
        }
        return current.name.equals(initial.name);
    };

    private Function<ExampleServiceState, Void> exampleStateUpdateBodySetter = (
            ExampleServiceState state) -> {
        state.name = Utils.getNowMicrosUtc() + "";
        return null;
    };

    private boolean isPeerSynchronizationEnabled = true;
    private boolean isAuthorizationEnabled = false;
    private HttpScheme replicationUriScheme;

    private void setUp(int localHostCount) throws Throwable {
        if (this.host != null) {
            return;
        }
        CommandLineArgumentParser.parseFromProperties(this);
        this.host = VerificationHost.create(0);
        this.host.setAuthorizationEnabled(this.isAuthorizationEnabled);

        VerificationHost.createAndAttachSSLClient(this.host);

        if (this.replicationUriScheme == HttpScheme.HTTPS_ONLY) {
            // disable HTTP, forcing host.getPublicUri() to return a HTTPS schemed URI. This in
            // turn forces the node group to use HTTPS for join, replication, etc
            this.host.setPort(ServiceHost.PORT_VALUE_LISTENER_DISABLED);
            // the default is disable (-1) so we must set port to 0, to enable SSL and make the
            // runtime pick a random HTTPS port
            this.host.setSecurePort(0);
        }

        if (this.testDurationSeconds > 0) {
            // for long running tests use the default interval to match production code
            this.host.maintenanceIntervalMillis = TimeUnit.MICROSECONDS.toMillis(
                    ServiceHostState.DEFAULT_MAINTENANCE_INTERVAL_MICROS);
        }

        this.host.start();

        if (this.host.isAuthorizationEnabled()) {
            this.host.setSystemAuthorizationContext();
        }

        CommandLineArgumentParser.parseFromProperties(this.host);
        this.host.setStressTest(this.host.isStressTest);
        this.host.setPeerSynchronizationEnabled(this.isPeerSynchronizationEnabled);
        this.host.setUpPeerHosts(localHostCount);

        for (VerificationHost h1 : this.host.getInProcessHostMap().values()) {
            setUpPeerHostWithAdditionalServices(h1);
        }

        // If the peer hosts are remote, then we undo CUSTOM_EXAMPLE_SERVICE_KIND
        // from the KINDS cache and use the real documentKind of ExampleService.
        if (this.host.isRemotePeerTest()) {
            Utils.registerKind(ExampleServiceState.class,
                    Utils.toDocumentKind(ExampleServiceState.class));
        }
    }

    private void setUpPeerHostWithAdditionalServices(VerificationHost h1) throws Throwable {
        h1.setStressTest(this.host.isStressTest);

        h1.waitForServiceAvailable(ExampleService.FACTORY_LINK);

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
        Utils.registerKind(ExampleServiceState.class, CUSTOM_EXAMPLE_SERVICE_KIND);
    }

    @After
    public void tearDown() throws InterruptedException {
        Utils.registerKind(ExampleServiceState.class,
                Utils.toDocumentKind(ExampleServiceState.class));
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
        this.host = null;
    }

    @Test
    public void commandLineJoinRetries() throws Throwable {
        this.host = VerificationHost.create(0);
        this.host.start();

        ExampleServiceHost nodeA = null;
        TemporaryFolder tmpFolderA = new TemporaryFolder();
        tmpFolderA.create();
        this.setUp(1);
        try {
            // start a node, supplying a bogus peer. Verify we retry, up to expiration which is
            // the operation timeout
            nodeA = new ExampleServiceHost();

            String id = "nodeA-" + VerificationHost.hostNumber.incrementAndGet();
            int bogusPort = 1;
            String[] args = {
                    "--port=0",
                    "--id=" + id,
                    "--bindAddress=127.0.0.1",
                    "--sandbox="
                            + tmpFolderA.getRoot().getAbsolutePath(),
                    "--peerNodes=" + "http://127.0.0.1:" + bogusPort
            };

            nodeA.initialize(args);
            nodeA.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS
                    .toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
            nodeA.start();

            // verify we see a specific retry stat
            URI nodeGroupUri = UriUtils.buildUri(nodeA, ServiceUriPaths.DEFAULT_NODE_GROUP);
            URI statsUri = UriUtils.buildStatsUri(nodeGroupUri);
            this.host.waitFor("expected stat did not converge", () -> {
                ServiceStats stats = this.host.getServiceState(null, ServiceStats.class, statsUri);
                ServiceStat st = stats.entries.get(NodeGroupService.STAT_NAME_JOIN_RETRY_COUNT);
                if (st == null || st.latestValue < 1) {
                    return false;
                }
                return true;
            });

        } finally {
            if (nodeA != null) {
                nodeA.stop();
                tmpFolderA.delete();
            }
        }
    }

    @Test
    public void customNodeGroupWithObservers() throws Throwable {
        for (int i = 0; i < this.iterationCount; i++) {
            Logger.getAnonymousLogger().info("Iteration: " + i);
            verifyCustomNodeGroupWithObservers();
            tearDown();
        }
    }

    private void verifyCustomNodeGroupWithObservers() throws Throwable {
        setUp(this.nodeCount);
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

        URI customNodeGroupServiceOnObserver = UriUtils
                .buildUri(observerHostUri, CUSTOM_NODE_GROUP);
        Map<URI, EnumSet<NodeOption>> expectedOptionsPerNode = new HashMap<>();
        expectedOptionsPerNode.put(customNodeGroupServiceOnObserver,
                observerSelfState.options);

        this.host.joinNodesAndVerifyConvergence(CUSTOM_NODE_GROUP, this.nodeCount,
                this.nodeCount, expectedOptionsPerNode);
        // one of the nodes is observer, so we must set quorum to 2 explicitly
        this.host.setNodeGroupQuorum(2, customNodeGroupServiceOnObserver);
        this.host.waitForNodeSelectorQuorumConvergence(CUSTOM_GROUP_NODE_SELECTOR, 2);
        this.host.waitForNodeGroupIsAvailableConvergence(CUSTOM_NODE_GROUP);

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

        this.host.waitForReplicatedFactoryServiceAvailable(observerFactoryUri,
                CUSTOM_GROUP_NODE_SELECTOR);

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

        // start more nodes, insert them to existing group, but with no synchronization required
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
    public void synchronizationOneByOneWithAbruptNodeShutdown() throws Throwable {
        setUp(this.nodeCount);

        // On one host, add some services. They exist only on this host and we expect them to synchronize
        // across all hosts once this one joins with the group
        URI hostUriWithInitialState = this.host.getPeerHostUri();
        Map<String, ExampleServiceState> exampleStatesPerSelfLink =
                createExampleServices(hostUriWithInitialState);

        URI hostWithStateNodeGroup = UriUtils.buildUri(hostUriWithInitialState,
                ServiceUriPaths.DEFAULT_NODE_GROUP);

        // before start joins, verify isolated factory synchronization is done
        for (URI hostUri : this.host.getNodeGroupMap().keySet()) {
            this.host.waitForReplicatedFactoryServiceAvailable(UriUtils.buildUri(hostUri,
                    ExampleService.FACTORY_LINK));
        }

        // join a node, with no state, one by one, to the host with state.
        // The steps are:
        // 1) set quorum to node group size + 1
        // 2) Join new empty node with existing node group
        // 3) verify convergence of factory state
        // 4) repeat

        List<URI> joinedHosts = new ArrayList<>();
        Map<URI, URI> factories = new HashMap<>();
        factories.put(hostWithStateNodeGroup, UriUtils.buildUri(hostWithStateNodeGroup,
                ExampleService.FACTORY_LINK));
        joinedHosts.add(hostWithStateNodeGroup);
        int fullQuorum = 1;

        for (URI nodeGroupUri : this.host.getNodeGroupMap().values()) {
            // skip host with state
            if (hostWithStateNodeGroup.equals(nodeGroupUri)) {
                continue;
            }

            this.host.log("Setting quorum to %d, already joined: %d",
                    fullQuorum + 1, joinedHosts.size());
            // set quorum to expected full node group size, for the setup hosts
            this.host.setNodeGroupQuorum(++fullQuorum);

            this.host.testStart(1);
            // join empty node, with node with state
            this.host.joinNodeGroup(hostWithStateNodeGroup, nodeGroupUri, fullQuorum);
            this.host.testWait();
            joinedHosts.add(nodeGroupUri);
            factories.put(nodeGroupUri, UriUtils.buildUri(nodeGroupUri,
                    ExampleService.FACTORY_LINK));
            this.host.waitForNodeGroupConvergence(joinedHosts, fullQuorum, fullQuorum, true);
            this.host.waitForNodeGroupIsAvailableConvergence(nodeGroupUri.getPath(), joinedHosts);

            this.waitForReplicatedFactoryChildServiceConvergence(
                    factories,
                    exampleStatesPerSelfLink,
                    this.exampleStateConvergenceChecker, exampleStatesPerSelfLink.size(),
                    0);

            doExampleServicePatch(exampleStatesPerSelfLink,
                    joinedHosts.get(0));

        }

        doNodeStopWithUpdates(exampleStatesPerSelfLink);
    }

    private void doExampleServicePatch(Map<String, ExampleServiceState> states,
            URI nodeGroupOnSomeHost) throws Throwable {
        this.host.log("Starting PATCH to %d example services", states.size());
        TestContext ctx = this.host
                .testCreate(this.updateCount * states.size());

        this.setOperationTimeoutMicros(TimeUnit.SECONDS.toMicros(this.host.getTimeoutSeconds()));

        for (int i = 0; i < this.updateCount; i++) {
            for (Entry<String, ExampleServiceState> e : states.entrySet()) {
                ExampleServiceState st = Utils.clone(e.getValue());
                st.counter = (long) i;
                Operation patch = Operation
                        .createPatch(UriUtils.buildUri(nodeGroupOnSomeHost, e.getKey()))
                        .setCompletion(ctx.getCompletion())
                        .setBody(st);
                this.host.send(patch);
            }
        }
        this.host.testWait(ctx);
        this.host.log("Done with PATCH to %d example services", states.size());
    }

    public void doNodeStopWithUpdates(Map<String, ExampleServiceState> exampleStatesPerSelfLink)
            throws Throwable {
        this.host.log("Starting to stop nodes and send updates");
        VerificationHost remainingHost = this.host.getPeerHost();
        Collection<VerificationHost> hostsToStop = new ArrayList<>(this.host.getInProcessHostMap()
                .values());
        hostsToStop.remove(remainingHost);
        List<URI> targetServices = new ArrayList<>();
        for (String link : exampleStatesPerSelfLink.keySet()) {
            targetServices.add(UriUtils.buildUri(remainingHost, link));
        }

        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            h.setPeerSynchronizationTimeLimitSeconds(this.host.getTimeoutSeconds() / 3);
        }

        stopHostsAndVerifyQueuing(hostsToStop, remainingHost, targetServices);

        // nodes are stopped, do updates again, quorum is relaxed, they should work
        doExampleServicePatch(exampleStatesPerSelfLink, remainingHost.getUri());
        this.host.log("Done with stop nodes and send updates");
    }

    private Map<String, ExampleServiceState> createExampleServices(URI hostUri) throws Throwable {
        URI factoryUri = UriUtils.buildUri(hostUri, ExampleService.FACTORY_LINK);
        this.host.log("POSTing children to %s", hostUri);

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
        return exampleStatesPerSelfLink;
    }

    @Test
    public void synchronizationWithPeerNodeListAndDuplicates()
            throws Throwable {

        ExampleServiceHost h = null;

        TemporaryFolder tmpFolder = new TemporaryFolder();
        tmpFolder.create();

        try {
            setUp(this.nodeCount);

            // the hosts are started, but not joined. We need to relax the quorum for any updates
            // to go through
            this.host.setNodeGroupQuorum(1);

            Map<String, ExampleServiceState> exampleStatesPerSelfLink = new HashMap<>();

            // add the *same* service instance, all *all* peers, so we force synchronization and epoch
            // change on an instance that exists everywhere

            int dupServiceCount = this.serviceCount;
            AtomicInteger counter = new AtomicInteger();
            Map<URI, ExampleServiceState> dupStates = new HashMap<>();
            for (VerificationHost v : this.host.getInProcessHostMap().values()) {
                counter.set(0);
                URI factoryUri = UriUtils.buildFactoryUri(v,
                        ExampleService.class);
                dupStates = this.host.doFactoryChildServiceStart(
                        null,
                        dupServiceCount,
                        ExampleServiceState.class,
                        (o) -> {
                            ExampleServiceState s = new ExampleServiceState();
                            s.documentSelfLink = "duplicateExampleInstance-"
                                    + counter.incrementAndGet();
                            s.name = s.documentSelfLink;
                            o.setBody(s);
                        }, factoryUri);
            }

            for (ExampleServiceState s : dupStates.values()) {
                exampleStatesPerSelfLink.put(s.documentSelfLink, s);
            }

            // increment to account for link found on all nodes
            this.serviceCount = exampleStatesPerSelfLink.size();

            // create peer argument list, all the nodes join.
            Collection<URI> peerNodeGroupUris = new ArrayList<>();
            StringBuilder peerNodes = new StringBuilder();
            for (VerificationHost peer : this.host.getInProcessHostMap().values()) {
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
            int quorum = this.host.getPeerCount();

            String mainHostId = "main-" + VerificationHost.hostNumber.incrementAndGet();
            String[] args = {
                    "--port=0",
                    "--id=" + mainHostId,
                    "--bindAddress=127.0.0.1",
                    "--sandbox="
                            + tmpFolder.getRoot().getAbsolutePath(),
                    "--peerNodes=" + peerNodes.toString()
            };

            h.initialize(args);

            h.setPeerSynchronizationEnabled(this.isPeerSynchronizationEnabled);
            h.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS
                    .toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));

            h.start();
            URI mainHostNodeGroupUri = UriUtils.buildUri(h, ServiceUriPaths.DEFAULT_NODE_GROUP);

            int totalCount = this.nodeCount + 1;
            peerNodeGroupUris.add(mainHostNodeGroupUri);
            this.host.waitForNodeGroupIsAvailableConvergence();
            this.host.waitForNodeGroupConvergence(peerNodeGroupUris, totalCount,
                    totalCount, true);

            this.host.setNodeGroupQuorum(quorum, mainHostNodeGroupUri);
            this.host.setNodeGroupQuorum(quorum);

            this.host.scheduleSynchronizationIfAutoSyncDisabled(this.replicationNodeSelector);

            int peerNodeCount = h.getInitialPeerHosts().size();
            // include self in peers
            assertTrue(totalCount >= peerNodeCount + 1);

            // Before factory synch is complete, make sure POSTs to existing services fail,
            // since they are not marked idempotent.
            verifyReplicatedInConflictPost(dupStates);

            // now verify all nodes synchronize and see the example service instances that existed on the single
            // host
            waitForReplicatedFactoryChildServiceConvergence(
                    exampleStatesPerSelfLink,
                    this.exampleStateConvergenceChecker,
                    this.serviceCount, 0);

            // Send some updates after the full group has formed  and verify the updates are seen by services on all nodes

            doStateUpdateReplicationTest(Action.PATCH, this.serviceCount, this.updateCount, 0,
                    this.exampleStateUpdateBodySetter,
                    this.exampleStateConvergenceChecker,
                    exampleStatesPerSelfLink);

            URI exampleFactoryUri = this.host.getPeerServiceUri(ExampleService.FACTORY_LINK);
            this.host.waitForReplicatedFactoryServiceAvailable(exampleFactoryUri);
        } finally {
            this.host.log("test finished");
            if (h != null) {
                h.stop();
                tmpFolder.delete();
            }
        }
    }

    private void verifyReplicatedInConflictPost(Map<URI, ExampleServiceState> dupStates)
            throws Throwable {
        // Its impossible to guarantee that this runs during factory synch. It might run before,
        // it might run during, it might run after. Since we runs 1000s of tests per day, CI
        // will let us know if the production code works. Here, we add a small sleep so we increase
        // chance we overlap with factory synchronization.
        Thread.sleep(TimeUnit.MICROSECONDS.toMillis(
                this.host.getPeerHost().getMaintenanceIntervalMicros()));
        // Issue a POST for a service we know exists and expect failure, since the example service
        // is not marked IDEMPOTENT. We expect CONFLICT error code, but if synchronization is active
        // we want to confirm we dont get 500, but the 409 is preserved
        TestContext ctx = this.host.testCreate(dupStates.size());
        for (ExampleServiceState st : dupStates.values()) {
            URI factoryUri = this.host.getPeerServiceUri(ExampleService.FACTORY_LINK);
            Operation post = Operation.createPost(factoryUri).setBody(st)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            if (o.getStatusCode() != Operation.STATUS_CODE_CONFLICT) {
                                ctx.failIteration(new IllegalStateException(
                                        "Expected conflict status, got " + o.getStatusCode()));
                                return;
                            }
                            ctx.completeIteration();
                            return;
                        }
                        ctx.failIteration(new IllegalStateException(
                                "Expected failure on duplicate POST"));
                    });
            this.host.send(post);
        }
        this.host.testWait(ctx);
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

        setUp(this.nodeCount);
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
    public void enforceHighQuorumWithNodeConcurrentStop()
            throws Throwable {
        int hostRestartCount = 2;

        Map<String, ExampleServiceState> childStates = doExampleFactoryPostReplicationTest(
                this.serviceCount, null, null);

        updateExampleServiceOptions(childStates);

        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            h.setPeerSynchronizationTimeLimitSeconds(1);
        }

        this.host.setNodeGroupConfig(this.nodeGroupConfig);
        this.host.setNodeGroupQuorum((this.nodeCount + 1) / 2);

        // do some replication with strong quorum enabled
        childStates = doStateUpdateReplicationTest(Action.PATCH, this.serviceCount,
                this.updateCount,
                0,
                this.exampleStateUpdateBodySetter,
                this.exampleStateConvergenceChecker,
                childStates);

        long now = Utils.getNowMicrosUtc();

        validatePerOperationReplicationQuorum(childStates, now);

        // expect failure, since we will stop some hosts, break quorum
        this.expectFailure = true;

        // when quorum is not met the runtime will just queue requests until expiration, so
        // we set expiration to something quick. Some requests will make it past queuing
        // and will fail because replication quorum is not met
        long opTimeoutMicros = TimeUnit.MILLISECONDS.toMicros(500);
        setOperationTimeoutMicros(opTimeoutMicros);

        int i = 0;
        for (URI h : this.host.getInProcessHostMap().keySet()) {
            this.expectedFailedHosts.add(h);
            if (++i >= hostRestartCount) {
                break;
            }
        }

        // stop one host right away
        stopHostsToSimulateFailure(1);

        // concurrently with the PATCH requests below, stop another host
        Runnable r = () -> {
            stopHostsToSimulateFailure(hostRestartCount - 1);
            // add a small bit of time slop since its feasible a host completed a operation *after* we stopped it,
            // the netty handlers are stopped in async (not forced) mode
            this.expectedFailureStartTimeMicros = Utils.getNowMicrosUtc()
                    + TimeUnit.MILLISECONDS.toMicros(250);

        };
        this.host.schedule(r, 1, TimeUnit.MILLISECONDS);

        childStates = doStateUpdateReplicationTest(Action.PATCH, this.serviceCount,
                this.updateCount,
                this.updateCount,
                this.exampleStateUpdateBodySetter,
                this.exampleStateConvergenceChecker,
                childStates);

        doStateUpdateReplicationTest(Action.PATCH, childStates.size(), this.updateCount,
                this.updateCount * 2,
                this.exampleStateUpdateBodySetter,
                this.exampleStateConvergenceChecker,
                childStates);

        doStateUpdateReplicationTest(Action.PATCH, childStates.size(), 1,
                this.updateCount * 2,
                this.exampleStateUpdateBodySetter,
                this.exampleStateConvergenceChecker,
                childStates);
    }

    private void validatePerOperationReplicationQuorum(Map<String, ExampleServiceState> childStates,
            long now) throws Throwable {
        Random r = new Random();
        // issue a patch, with per operation quorum set, verify it applied
        for (Entry<String, ExampleServiceState> e : childStates.entrySet()) {
            TestContext ctx = this.host.testCreate(1);
            ExampleServiceState body = e.getValue();
            body.counter = now;
            Operation patch = Operation.createPatch(this.host.getPeerServiceUri(e.getKey()))
                    .setCompletion(ctx.getCompletion())
                    .setBody(body);

            // add an explicit replication count header, using either the "all" value, or an
            // explicit node count
            if (r.nextBoolean()) {
                patch.addRequestHeader(Operation.REPLICATION_QUORUM_HEADER,
                        Operation.REPLICATION_QUORUM_HEADER_VALUE_ALL);
            } else {
                patch.addRequestHeader(Operation.REPLICATION_QUORUM_HEADER,
                        this.nodeCount + "");
            }

            this.host.send(patch);
            this.host.testWait(ctx);
            // Go to each peer, directly to their index, and verify update is present. This is not
            // proof the per operation quorum was applied "synchronously", before the response
            // was sent, but over many runs, if there is a race or its applied asynchronously,
            // we will see failures
            for (URI hostBaseUri : this.host.getNodeGroupMap().keySet()) {
                URI indexUri = UriUtils.buildUri(hostBaseUri, ServiceUriPaths.CORE_DOCUMENT_INDEX);
                indexUri = UriUtils.buildIndexQueryUri(indexUri,
                        e.getKey(), true, false, ServiceOption.PERSISTENCE);

                ExampleServiceState afterState = this.host.getServiceState(null,
                        ExampleServiceState.class, indexUri);
                assertEquals(body.counter, afterState.counter);
            }
        }

        this.host.toggleNegativeTestMode(true);
        // verify that if we try to set per operation quorum too high, request will fail
        for (Entry<String, ExampleServiceState> e : childStates.entrySet()) {
            TestContext ctx = this.host.testCreate(1);
            ExampleServiceState body = e.getValue();
            body.counter = now;
            Operation patch = Operation.createPatch(this.host.getPeerServiceUri(e.getKey()))
                    .addRequestHeader(Operation.REPLICATION_QUORUM_HEADER,
                            (this.nodeCount * 2) + "")
                    .setCompletion(ctx.getExpectedFailureCompletion())
                    .setBody(body);
            this.host.send(patch);
            this.host.testWait(ctx);
            break;
        }
        this.host.toggleNegativeTestMode(false);
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
        this.isPeerSynchronizationEnabled = false;
        setUp(this.nodeCount);

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

        exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {
            boolean isConverged = true;
            // verify all factories report same number of children
            for (VerificationHost peerHost : this.host.getInProcessHostMap().values()) {
                factoryUri = UriUtils.buildUri(peerHost,
                        ReplicationFactoryTestService.SIMPLE_REPL_SELF_LINK);
                ServiceDocumentQueryResult rsp = this.host.getFactoryState(factoryUri);
                if (rsp.documentLinks.size() != latestState.size()) {
                    this.host.log("Factory %s reporting %d children, expected %d", factoryUri,
                            rsp.documentLinks.size(), latestState.size());
                    isConverged = false;
                    break;
                }
            }
            if (!isConverged) {
                Thread.sleep(250);
                continue;
            }
            break;
        }

        if (new Date().after(exp)) {
            throw new TimeoutException("factories did not converge");
        }

        this.host.log("Inducing synchronization");
        // Induce synchronization on stable node group. No changes should be observed since
        // all nodes should have identical state
        this.host.scheduleSynchronizationIfAutoSyncDisabled(this.replicationNodeSelector);
        // give synchronization a chance to run, its 100% asynchronous so we can't really tell when each
        // child is done, but a small delay should be sufficient for 99.9% of test environments, even under
        // load
        Thread.sleep(2000);

        // verify that example states did not change due to the induced synchronization
        Map<URI, ReplicationTestServiceState> latestStateAfter = this.host.getServiceState(null,
                ReplicationTestServiceState.class, ownerSelectedServices.keySet());
        for (Entry<URI, ReplicationTestServiceState> afterEntry : latestStateAfter.entrySet()) {
            ReplicationTestServiceState beforeState = latestState.get(afterEntry.getKey());
            ReplicationTestServiceState afterState = afterEntry.getValue();
            assertEquals(beforeState.documentVersion, afterState.documentVersion);
        }

        verifyOperationJoinAcrossPeers(latestStateAfter);
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
                        this.host.log("version mismatch, expected %d, got %d, from %s", 0,
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
        this.replicationTargetFactoryLink = ExampleService.FACTORY_LINK;
        doReplication();
    }

    @Test
    public void replicationSsl() throws Throwable {
        this.replicationUriScheme = ServiceHost.HttpScheme.HTTPS_ONLY;
        this.replicationTargetFactoryLink = ExampleService.FACTORY_LINK;
        doReplication();
    }

    @Test
    public void replicationLimitedFactor() throws Throwable {
        this.replicationFactor = 3L;
        this.replicationNodeSelector = ServiceUriPaths.DEFAULT_3X_NODE_SELECTOR;
        this.replicationTargetFactoryLink = LimitedReplicationExampleFactoryService.SELF_LINK;
        this.nodeCount = Math.max(5, this.nodeCount);
        doReplication();
    }

    private void doReplication() throws Throwable {
        this.isPeerSynchronizationEnabled = false;
        CommandLineArgumentParser.parseFromProperties(this);
        Date expiration = new Date();
        if (this.testDurationSeconds > 0) {
            expiration = new Date(expiration.getTime()
                    + TimeUnit.SECONDS.toMillis(this.testDurationSeconds));
        }

        Map<Action, Long> elapsedTimePerAction = new HashMap<>();
        Map<Action, Long> countPerAction = new HashMap<>();

        long totalOperations = 0;
        boolean isFirstRun = true;
        do {
            if (this.host == null) {
                setUp(this.nodeCount);
                this.host.joinNodesAndVerifyConvergence(this.host.getPeerCount());
                // for limited replication factor, we will still set the quorum high, and expect
                // the limited replication selector to use the minimum between majority of replication
                // factor, versus node group membership quorum
                this.host.setNodeGroupQuorum(this.nodeCount);
                // since we have disabled peer synch, trigger it explicitly so factories become available
                this.host.scheduleSynchronizationIfAutoSyncDisabled(this.replicationNodeSelector);
                this.host.waitForReplicatedFactoryServiceAvailable(
                        this.host.getPeerServiceUri(this.replicationTargetFactoryLink));

                waitForReplicationFactoryConvergence();
                if (this.replicationUriScheme == ServiceHost.HttpScheme.HTTPS_ONLY) {
                    // confirm nodes are joined using HTTPS group references
                    for (URI nodeGroup : this.host.getNodeGroupMap().values()) {
                        assertTrue(UriUtils.HTTPS_SCHEME.equals(nodeGroup.getScheme()));
                    }
                }

            }

            Map<String, ExampleServiceState> childStates = doExampleFactoryPostReplicationTest(
                    this.serviceCount, countPerAction, elapsedTimePerAction);
            totalOperations += this.serviceCount;

            if (this.testDurationSeconds == 0) {
                // various validation tests, executed just once, ignored in long running test
                this.host.doExampleServiceUpdateAndQueryByVersion(this.host.getPeerHostUri(),
                        this.serviceCount);
                verifyReplicatedForcedPostAfterDelete(childStates);
                verifyInstantNotFoundFailureOnBadLinks();
                verifyReplicatedIdempotentPost(childStates);
            }

            totalOperations += this.serviceCount;

            if (expiration == null) {
                expiration = this.host.getTestExpiration();
            }
            int expectedVersion = this.updateCount;

            if (!this.host.isStressTest()
                    && (this.host.getPeerCount() > 16 || this.serviceCount * this.updateCount > 100)) {
                this.host.setStressTest(true);
            }


            long opCount = this.serviceCount * this.updateCount;
            childStates = doStateUpdateReplicationTest(Action.PATCH, this.serviceCount,
                    this.updateCount,
                    expectedVersion,
                    this.exampleStateUpdateBodySetter,
                    this.exampleStateConvergenceChecker,
                    childStates,
                    countPerAction,
                    elapsedTimePerAction);
            expectedVersion += this.updateCount;

            totalOperations += opCount;

            childStates = doStateUpdateReplicationTest(Action.PUT, this.serviceCount,
                    this.updateCount,
                    expectedVersion,
                    this.exampleStateUpdateBodySetter,
                    this.exampleStateConvergenceChecker,
                    childStates,
                    countPerAction,
                    elapsedTimePerAction);

            totalOperations += opCount;

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

            totalOperations += this.serviceCount;

            if (queryExp.before(new Date())) {
                throw new TimeoutException();
            }

            expectedVersion += 1;
            doStateUpdateReplicationTest(Action.DELETE, this.serviceCount, 1,
                    expectedVersion,
                    this.exampleStateUpdateBodySetter,
                    this.exampleStateConvergenceChecker,
                    childStates,
                    countPerAction,
                    elapsedTimePerAction);

            totalOperations += this.serviceCount;

            this.host.log("Total operations: %d", totalOperations);

            if (isFirstRun && this.testDurationSeconds > 0) {
                // ignore data during JVM warm-up
                countPerAction.clear();
                elapsedTimePerAction.clear();
                isFirstRun = false;
            }

        } while (new Date().before(expiration) && this.totalOperationLimit > totalOperations);

        logPerActionThroughput(elapsedTimePerAction, countPerAction);

        this.host.doNodeGroupStatsVerification(this.host.getNodeGroupMap());
    }

    private void logPerActionThroughput(Map<Action, Long> elapsedTimePerAction,
            Map<Action, Long> countPerAction) {
        for (Action a : EnumSet.allOf(Action.class)) {
            Long count = countPerAction.get(a);
            if (count == null) {
                continue;
            }
            Long elapsedMicros = elapsedTimePerAction.get(a);

            double thpt = (count * 1.0) / (1.0 * elapsedMicros);
            thpt *= 1000000;
            this.host.log("Total ops for %s: %d, Throughput (ops/sec): %f", a, count, thpt);
        }
    }

    private void updatePerfDataPerAction(Action a, Long startTime, Long opCount,
            Map<Action, Long> countPerAction, Map<Action, Long> elapsedTime) {
        if (opCount == null || countPerAction != null) {
            countPerAction.merge(a, opCount, (e, n) -> {
                if (e == null) {
                    return n;
                }
                return e + n;
            });
        }

        if (startTime == null || elapsedTime == null) {
            return;
        }

        long delta = Utils.getNowMicrosUtc() - startTime;
        elapsedTime.merge(a, delta, (e, n) -> {
            if (e == null) {
                return n;
            }
            return e + n;
        });
    }

    private void verifyReplicatedIdempotentPost(Map<String, ExampleServiceState> childStates)
            throws Throwable {
        // verify IDEMPOTENT POST conversion to PUT, with replication
        // Since the factory is not idempotent by default, enable the option dynamically
        Map<URI, URI> exampleFactoryUris = this.host
                .getNodeGroupToFactoryMap(ExampleService.FACTORY_LINK);
        for (URI factoryUri : exampleFactoryUris.values()) {
            this.host.toggleServiceOptions(factoryUri,
                    EnumSet.of(ServiceOption.IDEMPOTENT_POST), null);
        }
        TestContext ctx = this.host.testCreate(childStates.size());
        for (Entry<String, ExampleServiceState> entry : childStates.entrySet()) {
            Operation post = Operation
                    .createPost(this.host.getPeerServiceUri(ExampleService.FACTORY_LINK))
                    .setBody(entry.getValue())
                    .setCompletion(ctx.getCompletion());
            this.host.send(post);
        }
        ctx.await();
    }

    /**
     * Verifies that DELETE actions propagate and commit, and, that forced POST actions succeed
     */
    private void verifyReplicatedForcedPostAfterDelete(Map<String, ExampleServiceState> childStates)
            throws Throwable {
        // delete one of the children, then re-create but with a zero version, using a special
        // directive that forces creation
        Entry<String, ExampleServiceState> childEntry = childStates.entrySet().iterator().next();
        TestContext ctx = this.host.testCreate(1);
        Operation delete = Operation
                .createDelete(this.host.getPeerServiceUri(childEntry.getKey()))
                .setCompletion(ctx.getCompletion());
        this.host.send(delete);
        ctx.await();

        if (!this.host.isRemotePeerTest()) {
            this.host.waitFor("services not deleted", () -> {
                for (VerificationHost h : this.host.getInProcessHostMap().values()) {
                    ProcessingStage stg = h.getServiceStage(childEntry.getKey());
                    if (stg != null) {
                        this.host.log("Service exists %s on host %s, stage %s",
                                childEntry.getKey(), h.toString(), stg);
                        return false;
                    }
                }
                return true;
            });
        }

        TestContext postCtx = this.host.testCreate(1);
        Operation opPost = Operation
                .createPost(this.host.getPeerServiceUri(this.replicationTargetFactoryLink))
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)
                .setBody(childEntry.getValue())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        postCtx.failIteration(e);
                    } else {
                        postCtx.completeIteration();
                    }
                });
        this.host.send(opPost);
        this.host.testWait(postCtx);
    }

    private void waitForReplicationFactoryConvergence() throws Throwable {

        // for code coverage, verify the convenience method on the host also reports available
        WaitHandler wh = () -> {
            TestContext ctx = this.host.testCreate(1);
            boolean[] isReady = new boolean[1];
            CompletionHandler ch = (o, e) -> {
                if (e != null) {
                    isReady[0] = false;
                } else {
                    isReady[0] = true;
                }
                ctx.completeIteration();
            };

            VerificationHost peerHost = this.host.getPeerHost();
            if (peerHost == null) {
                NodeGroupUtils.checkServiceAvailability(ch, this.host,
                        this.host.getPeerServiceUri(this.replicationTargetFactoryLink),
                        this.replicationNodeSelector);
            } else {
                peerHost.checkReplicatedServiceAvailable(ch, this.replicationTargetFactoryLink);
            }
            ctx.await();
            return isReady[0];
        };

        this.host.waitFor("available check timeout for " + this.replicationTargetFactoryLink, wh);
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

    private void verifyInstantNotFoundFailureOnBadLinks() throws Throwable {
        this.host.toggleNegativeTestMode(true);

        CompletionHandler c = (o, e) -> {
            if (e != null) {
                this.host.completeIteration();
                return;
            }
            // strange, service exists, lets verify
            for (VerificationHost h : this.host.getInProcessHostMap().values()) {
                ProcessingStage stg = h.getServiceStage(o.getUri().getPath());
                if (stg != null) {
                    this.host.log("Service exists %s on host %s, stage %s",
                            o.getUri().getPath(), h.toString(), stg);
                }
            }
            this.host.failIteration(new Throwable("Expected service to not exist:"
                    + o.toString()));
        };

        // do a negative test: send request to a example child we know does not exist, but disable queuing
        // so we get 404 right away
        this.host.testStart(this.serviceCount);
        for (int i = 0; i < this.serviceCount; i++) {
            URI factoryURI = this.host.getNodeGroupToFactoryMap(ExampleService.FACTORY_LINK)
                    .values().iterator().next();
            URI bogusChild = UriUtils.extendUri(factoryURI,
                    Utils.getNowMicrosUtc() + UUID.randomUUID().toString());
            Operation patch = Operation.createPatch(bogusChild)
                    .setCompletion(c)
                    .setBody(new ExampleServiceState());

            this.host.send(patch);
        }
        this.host.testWait();
        this.host.toggleNegativeTestMode(false);
    }

    @Test
    public void factorySynchronization() throws Throwable {

        setUp(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);

        factorySynchronizationNoChildren();

        factoryDuplicatePost();
    }

    @Test
    public void replicationWithAuthzCacheClear() throws Throwable {
        // create users and roles for a users 'foo@foo.com' and 'bar@foo.com', 'foobar@bar.com'
        this.isAuthorizationEnabled = true;
        setUp(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        this.host.setNodeGroupQuorum(this.nodeCount);

        VerificationHost groupHost = this.host.getPeerHost();
        groupHost.setSystemAuthorizationContext();
        AuthorizationHelper authHelperForFoo = new AuthorizationHelper(groupHost);
        String email = "foo@foo.com";
        String fooUserLink = authHelperForFoo.createUserService(groupHost, email);
        UserState patchState = new UserState();
        patchState.userGroupLinks = new HashSet<String>();
        patchState.userGroupLinks.add(UriUtils.buildUriPath(
                UserGroupService.FACTORY_LINK, authHelperForFoo.getUserGroupName(email)));
        authHelperForFoo.patchUserService(groupHost, fooUserLink, patchState);
        // create a user group based on a query for userGroupLink
        authHelperForFoo.createRoles(groupHost, email, false);
        AuthorizationHelper authHelperForBar = new AuthorizationHelper(groupHost);
        email = "bar@foo.com";
        String barUserLink = authHelperForBar.createUserService(groupHost, email);
        // create a user group based on the UserService email field
        authHelperForBar.createRoles(groupHost, email);
        AuthorizationHelper authHelperForFooBar = new AuthorizationHelper(groupHost);
        email = "foobar@foo.com";
        authHelperForFooBar.createUserService(groupHost, email);
        authHelperForFooBar.createRoles(groupHost, email);
        groupHost.resetSystemAuthorizationContext();
        // delete the role associated with user foo; this should clear out the authz cache entry
        Collection<VerificationHost> peerHosts = this.host.getInProcessHostMap().values();
        invokeOperation(peerHosts, fooUserLink,
                (opHost) -> {
                    try {
                    opHost.sendAndWaitExpectSuccess(
                            Operation.createDelete(UriUtils.buildUri(opHost, authHelperForFoo.getRoleLink())));
                    } catch (Throwable t) {
                        return t;
                    }
                    return null;
                });
        // delete the user group associated with the user
        invokeOperation(peerHosts, fooUserLink,
                (opHost) -> {
                    try {
                    opHost.sendAndWaitExpectSuccess(
                            Operation.createDelete(UriUtils.buildUri(opHost, authHelperForFoo.getUserGroupLink())));
                    } catch (Throwable t) {
                        return t;
                    }
                    return null;
                });

        // next, assume identity as bar@foo.com, create a new role and associate it with the user bar
        invokeOperation(peerHosts, barUserLink,
                (opHost) -> {
                    try {
                        Query q = Builder.create()
                                .addFieldClause(
                                        ExampleServiceState.FIELD_NAME_KIND,
                                        Utils.buildKind(ExampleServiceState.class))
                                .build();
                        String newResourceGroupLink = authHelperForBar.createResourceGroup(opHost, "new-rg", q);
                        Set<Service.Action> actions = new HashSet<Service.Action>(Arrays.asList(Action.GET, Action.POST));
                        authHelperForBar.createRole(opHost, authHelperForBar.getUserGroupLink(), newResourceGroupLink, actions);
                        authHelperForBar.createRole(opHost, authHelperForFooBar.getUserGroupLink(), newResourceGroupLink, actions);
                    } catch (Throwable t) {
                        return t;
                    }
                    return null;
                });
        // delete the user group for foobar@foo.com.
        // Updating the resource group should be able to handle the fact that the user group does not exist
        invokeOperation(peerHosts, barUserLink,
                (opHost) -> {
                    try {
                        String newResourceGroupLink = UriUtils.buildUriPath(ResourceGroupService.FACTORY_LINK, "new-rg");
                        Query updateResourceGroupQuery = Builder.create()
                                .addFieldClause(
                                        ExampleServiceState.FIELD_NAME_NAME,
                                        "bar")
                                .build();
                        opHost.sendAndWaitExpectSuccess(
                                Operation.createDelete(UriUtils.buildUri(opHost, authHelperForFooBar.getUserGroupLink())));
                        ResourceGroupState resourceGroupState = new ResourceGroupState();
                        resourceGroupState.query = updateResourceGroupQuery;
                        opHost.sendAndWaitExpectSuccess(
                                Operation.createPut(UriUtils.buildUri(opHost, newResourceGroupLink))
                                .setBody(resourceGroupState));
                        opHost.sendAndWaitExpectSuccess(
                                Operation.createDelete(UriUtils.buildUri(opHost, newResourceGroupLink)));
                    } catch (Throwable t) {
                        return t;
                    }
                    return null;
                });
        // patch the userservice
        invokeOperation(peerHosts, fooUserLink,
                (opHost) -> {
                    try {
                        UserState userState = new UserState();
                        userState.userGroupLinks = new HashSet<String>();
                        userState.userGroupLinks.add("foo");
                        opHost.sendAndWaitExpectSuccess(
                                Operation.createPatch(UriUtils.buildUri(groupHost, fooUserLink))
                                .setBody(userState));
                    } catch (Throwable t) {
                        return t;
                    }
                    return null;
                });
        // finally, delete the user service; the authz cache should be cleared
        invokeOperation(peerHosts, fooUserLink,
                (opHost) -> {
                    try {
                        opHost.sendAndWaitExpectSuccess(
                                Operation.createDelete(UriUtils.buildUri(groupHost, fooUserLink)));
                    } catch (Throwable t) {
                        return t;
                    }
                    return null;
                });
    }

    // helper method to invoke the specified Function object as the system user and
    // validate if the authz caches have been invalidated after
    private void invokeOperation(Collection<VerificationHost> peerHosts, String userLink,
            Function<VerificationHost, Throwable> func) throws Throwable {
        VerificationHost groupHost = peerHosts.iterator().next();
        AuthorizationContext authContext = groupHost.assumeIdentity(userLink);
        groupHost.sendAndWaitExpectSuccess(
                Operation.createGet(UriUtils.buildUri(groupHost, ExampleService.FACTORY_LINK)));
        checkCache(peerHosts, authContext.getToken(), true);
        groupHost.setSystemAuthorizationContext();
        Throwable result = func.apply(groupHost);
        if (result != null) {
            throw result;
        }
        groupHost.resetSystemAuthorizationContext();
        checkCache(this.host.getInProcessHostMap().values(), authContext.getToken(), false);
    }

    // helper method to check if the authz cache is in the expected state
    private void checkCache(Collection<VerificationHost> peerHosts, String token, boolean expectEntries) throws Throwable {
        boolean contextFound = false;
        for (VerificationHost host : peerHosts) {
            host.setSystemAuthorizationContext();
            MinimalTestService s = new MinimalTestService();
            host.addPrivilegedService(MinimalTestService.class);
            host.startServiceAndWait(s, UUID.randomUUID().toString(), null);
            host.resetSystemAuthorizationContext();
            if (host.getAuthorizationContext(s, token) != null) {
                contextFound = true;
            }
        }
        if (expectEntries) {
            assertTrue(contextFound);
        } else {
            assertTrue(!contextFound);
        }
    }

    private void factoryDuplicatePost() throws Throwable, InterruptedException, TimeoutException {
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

    private void factorySynchronizationNoChildren() throws Throwable {
        int factoryCount = Math.max(this.serviceCount, 25);
        setUp(this.nodeCount);

        // start many factories, in each host, so when the nodes join there will be a storm
        // of synchronization requests between the nodes + factory instances
        this.host.testStart(this.nodeCount * factoryCount);
        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            for (int i = 0; i < factoryCount; i++) {
                Operation startPost = Operation.createPost(
                        UriUtils.buildUri(h,
                                UriUtils.buildUriPath(ExampleService.FACTORY_LINK, UUID
                                        .randomUUID().toString())))
                        .setCompletion(this.host.getCompletion());
                h.startService(startPost, ExampleService.createFactory());
            }
        }
        this.host.testWait();
        this.host.joinNodesAndVerifyConvergence(this.host.getPeerCount());
    }

    @Test
    public void forwardingAndSelection() throws Throwable {
        this.isPeerSynchronizationEnabled = false;
        setUp(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        for (int i = 0; i < this.iterationCount; i++) {
            directOwnerSelection();
            forwardingToPeerId();
            forwardingToKeyHashNode();
            broadcast();
        }
    }

    public void broadcast() throws Throwable {
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

    public void forwardingToKeyHashNode() throws Throwable {
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

    public void forwardingToPeerId() throws Throwable {
        long c = this.updateCount * this.nodeCount;
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
                s = Utils.clone(s);
                // set the owner to be the target node. we will use this to verify it matches
                // the id in the state, which is set through a forwarded PATCH
                s.documentOwner = peer.getId();
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
        String headerName = MinimalTestService.TEST_HEADER_NAME.toLowerCase();
        UUID id = UUID.randomUUID();
        String headerRequestValue = "request-" + id;
        String headerResponseValue = "response-" + id;
        this.host.testStart(initialStates.size() * this.nodeCount);
        for (ServiceDocument s : initialStates) {
            // send a PATCH the id for each document, to each peer. If it routes to the proper peer
            // the initial state.documentOwner, will match the state.id

            for (VerificationHost peer : this.host.getInProcessHostMap().values()) {
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

        TestContext ctx = this.host.testCreate(c * this.host.getPeerCount());
        for (VerificationHost peer : this.host.getInProcessHostMap().values()) {
            for (ServiceDocument s : initialStates) {
                Operation get = Operation.createGet(UriUtils.buildUri(peer, s.documentSelfLink))
                        .setCompletion(
                                (o, e) -> {
                                    if (e != null) {
                                        ctx.failIteration(e);
                                        return;
                                    }
                                    MinimalTestServiceState rsp = o
                                            .getBody(MinimalTestServiceState.class);
                                    if (!rsp.id.equals(rsp.documentOwner)) {
                                        ctx.failIteration(
                                                new IllegalStateException("Expected: "
                                                        + rsp.documentOwner + " was: " + rsp.id));
                                    } else {
                                        ctx.completeIteration();
                                    }
                                });
                this.host.send(get);
            }
        }
        this.host.testWait(ctx);

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
                                MinimalTestServiceErrorResponse rsp = o
                                        .getBody(MinimalTestServiceErrorResponse.class);
                                if (rsp.message == null || rsp.message.isEmpty()) {
                                    this.host.failIteration(new IllegalStateException(
                                            "expected error response message"));
                                    return;
                                }

                                if (!MinimalTestServiceErrorResponse.KIND.equals(rsp.documentKind)
                                        || 0 != Double.compare(Math.PI, rsp.customErrorField)) {
                                    this.host.failIteration(new IllegalStateException(
                                            "expected custom error fields"));
                                    return;
                                }
                                this.host.completeIteration();
                            }).setBody(body));
        }
        this.host.testWait();
        this.host.toggleDebuggingMode(false);
    }

    private void directOwnerSelection() throws Throwable {
        Map<URI, Map<String, Long>> keyToNodeAssignmentsPerNode = new HashMap<>();
        List<String> keys = new ArrayList<>();

        long c = this.updateCount * this.nodeCount;
        // generate N keys once, then ask each node to assign to its peers. Each node should come up
        // with the same distribution

        for (int i = 0; i < c; i++) {
            keys.add(Utils.getNowMicrosUtc() + "");
        }

        for (URI nodeGroup : this.host.getNodeGroupMap().values()) {
            keyToNodeAssignmentsPerNode.put(nodeGroup, new HashMap<>());
        }

        this.host.waitForNodeGroupConvergence(this.nodeCount);

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

            this.host.log("Node group %s assignments: %s", nodeGroup, assignmentsPerNode);

            for (Entry<String, Long> e : assignmentsPerNode.entrySet()) {
                // we assume that with random keys, and random node ids, each node will get at least
                // one key.
                assertTrue(e.getValue() > 0);
                Long count = countPerNode.get(e.getKey());
                if (count == null) {
                    continue;
                }
                if (!count.equals(e.getValue())) {
                    this.host.logNodeGroupState();
                    throw new IllegalStateException(
                            "Node id got assigned the same key different number of times, on one of the nodes");
                }
            }

        }
    }

    @Test
    public void replicationWithAuthAndNodeRestart() throws Throwable {
        AuthorizationHelper authHelper;

        this.isAuthorizationEnabled = true;
        setUp(this.nodeCount);

        authHelper = new AuthorizationHelper(this.host);

        // relax quorum to allow for divergent writes, on independent nodes (not yet joined)

        this.host.setSystemAuthorizationContext();

        // Create the same users and roles on every peer independently
        Map<ServiceHost, Collection<String>> roleLinksByHost = new HashMap<>();
        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            String email = "jane@doe.com";
            authHelper.createUserService(h, email);
            authHelper.createRoles(h, email);
        }

        // Get roles from all nodes
        Map<ServiceHost, Map<URI, RoleState>> roleStateByHost = getRolesByHost(roleLinksByHost);

        // Join nodes to force synchronization and convergence
        this.host.joinNodesAndVerifyConvergence(this.host.getPeerCount());

        // Get roles from all nodes
        Map<ServiceHost, Map<URI, RoleState>> newRoleStateByHost = getRolesByHost(roleLinksByHost);

        // Verify that every host independently advances their version & epoch
        for (ServiceHost h : roleStateByHost.keySet()) {
            Map<URI, RoleState> roleState = roleStateByHost.get(h);
            for (URI u : roleState.keySet()) {
                RoleState oldRole = roleState.get(u);
                RoleState newRole = newRoleStateByHost.get(h).get(u);
                assertTrue("version should have advanced",
                        newRole.documentVersion > oldRole.documentVersion);
                assertTrue("epoch should have advanced",
                        newRole.documentEpoch > oldRole.documentEpoch);
            }
        }

        // Verify that every host converged to the same version, epoch, and owner
        Map<String, Long> versions = new HashMap<>();
        Map<String, Long> epochs = new HashMap<>();
        Map<String, String> owners = new HashMap<>();
        for (ServiceHost h : newRoleStateByHost.keySet()) {
            Map<URI, RoleState> roleState = newRoleStateByHost.get(h);
            for (URI u : roleState.keySet()) {
                RoleState newRole = roleState.get(u);

                if (versions.containsKey(newRole.documentSelfLink)) {
                    assertTrue(versions.get(newRole.documentSelfLink) == newRole.documentVersion);
                } else {
                    versions.put(newRole.documentSelfLink, newRole.documentVersion);
                }

                if (epochs.containsKey(newRole.documentSelfLink)) {
                    assertTrue(Objects.equals(epochs.get(newRole.documentSelfLink),
                            newRole.documentEpoch));
                } else {
                    epochs.put(newRole.documentSelfLink, newRole.documentEpoch);
                }

                if (owners.containsKey(newRole.documentSelfLink)) {
                    assertEquals(owners.get(newRole.documentSelfLink), newRole.documentOwner);
                } else {
                    owners.put(newRole.documentSelfLink, newRole.documentOwner);
                }
            }
        }

        // create some example tasks, which delete example services. We dont have any
        // examples services created, which is good, since we just want these tasks to
        // go to finished state, then verify, after node restart, they all start
        Set<String> exampleTaskLinks = new ConcurrentSkipListSet<>();
        createReplicatedExampleTasks(exampleTaskLinks, null);

        Set<String> exampleLinks = new ConcurrentSkipListSet<>();
        verifyReplicatedAuthorizedPost(exampleLinks);

        // verify restart, with authorization.
        // stop one host
        VerificationHost hostToStop = this.host.getInProcessHostMap().values().iterator().next();
        stopAndRestartHost(exampleLinks, exampleTaskLinks, hostToStop);
    }

    private void createReplicatedExampleTasks(Set<String> exampleTaskLinks, String name)
            throws Throwable {
        URI factoryUri = UriUtils.buildFactoryUri(this.host.getPeerHost(),
                ExampleTaskService.class);
        this.host.setSystemAuthorizationContext();
        // Sample body that this user is authorized to create
        ExampleTaskServiceState exampleServiceState = new ExampleTaskServiceState();
        if (name != null) {
            exampleServiceState.customQueryClause = Query.Builder.create()
                    .addFieldClause(ExampleServiceState.FIELD_NAME_NAME, name).build();
        }
        this.host.testStart("creating example *task* instances", null, this.serviceCount);
        for (int i = 0; i < this.serviceCount; i++) {
            CompletionHandler c = (o, e) -> {
                if (e != null) {
                    this.host.failIteration(e);
                    return;
                }
                ExampleTaskServiceState rsp = o.getBody(ExampleTaskServiceState.class);
                synchronized (exampleTaskLinks) {
                    exampleTaskLinks.add(rsp.documentSelfLink);
                }
                this.host.completeIteration();
            };
            this.host.send(Operation
                    .createPost(factoryUri)
                    .setBody(exampleServiceState)
                    .setCompletion(c));
        }
        this.host.testWait();

        // ensure all tasks are in finished state
        this.host.waitFor("Example tasks did not finish", () -> {
            ServiceDocumentQueryResult rsp = this.host.getExpandedFactoryState(factoryUri);
            for (Object o : rsp.documents.values()) {
                ExampleTaskServiceState doc = Utils.fromJson(o, ExampleTaskServiceState.class);
                if (TaskState.isFailed(doc.taskInfo)) {
                    this.host.log("task %s failed: %s", doc.documentSelfLink, doc.failureMessage);
                    throw new IllegalStateException("task failed");
                }
                if (!TaskState.isFinished(doc.taskInfo)) {
                    return false;
                }
            }
            return true;
        });
    }

    private void verifyReplicatedAuthorizedPost(Set<String> exampleLinks)
            throws Throwable {
        Collection<VerificationHost> hosts = this.host.getInProcessHostMap().values();
        RoundRobinIterator<VerificationHost> it = new RoundRobinIterator<>(hosts);
        int exampleServiceCount = this.serviceCount;

        String userLink = UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHZ_USERS, "jane@doe.com");
        // Verify we can assert identity and make a request to every host
        this.host.assumeIdentity(userLink);

        // Sample body that this user is authorized to create
        ExampleServiceState exampleServiceState = new ExampleServiceState();
        exampleServiceState.name = "jane";


        this.host.testStart("creating example instances", null, exampleServiceCount);
        for (int i = 0; i < exampleServiceCount; i++) {
            CompletionHandler c = (o, e) -> {
                if (e != null) {
                    this.host.failIteration(e);
                    return;
                }

                try {
                    // Verify the user is set as principal
                    ExampleServiceState state = o.getBody(ExampleServiceState.class);
                    assertEquals(state.documentAuthPrincipalLink,
                            userLink);
                    exampleLinks.add(state.documentSelfLink);
                    this.host.completeIteration();
                } catch (Throwable e2) {
                    this.host.failIteration(e2);
                }
            };
            this.host.send(Operation
                    .createPost(UriUtils.buildFactoryUri(it.next(), ExampleService.class))
                    .setBody(exampleServiceState)
                    .setCompletion(c));
        }
        this.host.testWait();


        this.host.toggleNegativeTestMode(true);
        // Sample body that this user is NOT authorized to create
        ExampleServiceState invalidExampleServiceState = new ExampleServiceState();
        invalidExampleServiceState.name = "somebody other than jane";

        this.host.testStart("issuing non authorized request", null, exampleServiceCount);
        for (int i = 0; i < exampleServiceCount; i++) {
            this.host.send(Operation
                    .createPost(UriUtils.buildFactoryUri(it.next(), ExampleService.class))
                    .setBody(invalidExampleServiceState)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            assertEquals(Operation.STATUS_CODE_FORBIDDEN, o.getStatusCode());
                            this.host.completeIteration();
                            return;
                        }

                        this.host.failIteration(new IllegalStateException("expected failure"));
                    }));
        }
        this.host.testWait();
        this.host.toggleNegativeTestMode(false);
    }

    private void stopAndRestartHost(Set<String> exampleLinks, Set<String> exampleTaskLinks,
            VerificationHost hostToStop)
            throws Throwable, InterruptedException {
        // relax quorum
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        // expire node that went away quickly to avoid alot of log spam from gossip failures
        NodeGroupConfig cfg = new NodeGroupConfig();
        cfg.nodeRemovalDelayMicros = TimeUnit.SECONDS.toMicros(1);
        this.host.setNodeGroupConfig(cfg);

        this.host.stopHostAndPreserveState(hostToStop);
        this.host.waitForNodeGroupConvergence(2, 2);
        VerificationHost existingHost = this.host.getInProcessHostMap().values().iterator().next();

        this.host.waitForReplicatedFactoryServiceAvailable(
                this.host.getPeerServiceUri(ExampleTaskService.FACTORY_LINK));

        this.host.waitForReplicatedFactoryServiceAvailable(
                this.host.getPeerServiceUri(ExampleService.FACTORY_LINK));

        // create some additional tasks on the remaining two hosts, but make sure they don't delete
        // any example service instances, by specifying a name value we know will not match anything
        createReplicatedExampleTasks(exampleTaskLinks, UUID.randomUUID().toString());

        // delete some of the task links, to test synchronization of deleted entries on the restarted
        // host
        Set<String> deletedExampleLinks = deleteSomeServices(exampleLinks);

        // increase quorum on existing nodes, so they wait for new node
        this.host.setNodeGroupQuorum(this.nodeCount);

        hostToStop.setPort(0);
        hostToStop.setSecurePort(0);
        VerificationHost.restartStatefulHost(hostToStop);

        // restart host, rejoin it
        URI nodeGroupU = UriUtils.buildUri(hostToStop, ServiceUriPaths.DEFAULT_NODE_GROUP);
        URI eNodeGroupU = UriUtils.buildUri(existingHost, ServiceUriPaths.DEFAULT_NODE_GROUP);
        this.host.testStart(1);
        this.host.setSystemAuthorizationContext();
        this.host.joinNodeGroup(nodeGroupU, eNodeGroupU, this.nodeCount);
        this.host.testWait();
        this.host.addPeerNode(hostToStop);
        this.host.waitForNodeGroupConvergence(this.nodeCount);
        // set quorum on new node as well
        this.host.setNodeGroupQuorum(this.nodeCount);

        this.host.resetAuthorizationContext();

        this.host.waitFor("Task services not started in restarted host:" + exampleTaskLinks, () -> {
            return checkChildServicesIfStarted(exampleTaskLinks, hostToStop) == 0;
        });

        // verify all services, not previously deleted, are restarted
        this.host.waitFor("Services not started in restarted host:" + exampleLinks, () -> {
            return checkChildServicesIfStarted(exampleLinks, hostToStop) == 0;
        });

        int deletedCount = deletedExampleLinks.size();
        this.host.waitFor("Deleted services still present in restarted host", () -> {
            return checkChildServicesIfStarted(deletedExampleLinks, hostToStop) == deletedCount;
        });
    }

    private Set<String> deleteSomeServices(Set<String> exampleLinks)
            throws Throwable {
        int deleteCount = exampleLinks.size() / 3;
        Iterator<String> itLinks = exampleLinks.iterator();
        Set<String> deletedExampleLinks = new HashSet<>();
        this.host.testStart(deleteCount);
        for (int i = 0; i < deleteCount; i++) {
            String link = itLinks.next();
            deletedExampleLinks.add(link);
            exampleLinks.remove(link);
            Operation delete = Operation.createDelete(this.host.getPeerServiceUri(link))
                    .setCompletion(this.host.getCompletion());
            this.host.send(delete);
        }
        this.host.testWait();
        this.host.log("Deleted links: %s", deletedExampleLinks);
        return deletedExampleLinks;
    }

    private int checkChildServicesIfStarted(Set<String> exampleTaskLinks,
            VerificationHost host) {
        this.host.setSystemAuthorizationContext();
        int notStartedCount = 0;
        for (String s : exampleTaskLinks) {
            ProcessingStage st = host.getServiceStage(s);
            if (st == null) {
                notStartedCount++;
            }
        }
        this.host.resetAuthorizationContext();
        if (notStartedCount > 0) {
            this.host.log("%d services not started on %s (%s)", notStartedCount,
                    host.getPublicUri(), host.getId());
        }
        return notStartedCount;
    }

    private Map<ServiceHost, Map<URI, RoleState>> getRolesByHost(
            Map<ServiceHost, Collection<String>> roleLinksByHost) throws Throwable {
        Map<ServiceHost, Map<URI, RoleState>> roleStateByHost = new HashMap<>();
        for (ServiceHost h : roleLinksByHost.keySet()) {
            Collection<String> roleLinks = roleLinksByHost.get(h);
            Collection<URI> roleURIs = new ArrayList<>();
            for (String link : roleLinks) {
                roleURIs.add(UriUtils.buildUri(h, link));
            }

            Map<URI, RoleState> serviceState = this.host.getServiceState(null, RoleState.class,
                    roleURIs);
            roleStateByHost.put(h, serviceState);
        }
        return roleStateByHost;
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
                    this.host.log("Node %s does not have all services: %s", baseNodeUri,
                            Utils.toJsonHtml(factoryRsp));
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
                            .get(ReplicationTestService.STAT_NAME_HANDLE_NODE_GROUP_MAINTENANCE_COUNT);
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
        return doStateUpdateReplicationTest(action, childCount, updateCount, expectedVersion,
                updateBodySetter, convergenceChecker, initialStatesPerChild, null, null);
    }

    private <T extends ServiceDocument> Map<String, T> doStateUpdateReplicationTest(Action action,
            int childCount, int updateCount,
            long expectedVersion,
            Function<T, Void> updateBodySetter,
            BiPredicate<T, T> convergenceChecker,
            Map<String, T> initialStatesPerChild,
            Map<Action, Long> countsPerAction,
            Map<Action, Long> elapsedTimePerAction) throws Throwable {
        this.host.testStart("Replicated " + action, null, childCount * updateCount);

        if (!this.expectFailure) {
            // Before we do the replication test, wait for factory availability.
            for (URI fu : this.host.getNodeGroupToFactoryMap(this.replicationTargetFactoryLink)
                    .values()) {
                // confirm that /factory/available returns 200 across all nodes
                this.host.waitForReplicatedFactoryServiceAvailable(fu);
            }
        }

        long before = Utils.getNowMicrosUtc();
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

                long sentTime = 0;
                if (this.expectFailure) {
                    sentTime = Utils.getNowMicrosUtc();
                }
                URI factoryOnRandomPeerUri = this.host
                        .getPeerServiceUri(this.replicationTargetFactoryLink);

                long finalSentTime = sentTime;
                this.host
                        .send(Operation
                                .createPatch(UriUtils.buildUri(factoryOnRandomPeerUri,
                                        initState.documentSelfLink))
                                .setAction(action)
                                .forceRemote()
                                .setBodyNoCloning(initState)
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
                                                    && finalSentTime > this.expectedFailureStartTimeMicros) {
                                                this.host.failIteration(new IllegalStateException(
                                                        "Request should have failed: %s"
                                                                + o.toString()
                                                                + " sent at " + finalSentTime));
                                                return;
                                            }
                                            this.host.completeIteration();
                                        }));
            }
        }
        this.host.testWait();
        this.host.logThroughput();

        updatePerfDataPerAction(action, before, (long) (childCount * updateCount), countsPerAction,
                elapsedTimePerAction);

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

    private Map<String, ExampleServiceState> doExampleFactoryPostReplicationTest(int childCount,
            Map<Action, Long> countPerAction,
            Map<Action, Long> elapsedTimePerAction)
            throws Throwable, TimeoutException {
        return doExampleFactoryPostReplicationTest(childCount, null,
                countPerAction, elapsedTimePerAction);
    }

    private Map<String, ExampleServiceState> doExampleFactoryPostReplicationTest(int childCount,
            EnumSet<TestProperty> props,
            Map<Action, Long> countPerAction,
            Map<Action, Long> elapsedTimePerAction) throws Throwable {

        if (props == null) {
            props = EnumSet.noneOf(TestProperty.class);
        }

        if (this.host == null) {
            setUp(this.nodeCount);
            this.host.joinNodesAndVerifyConvergence(this.host.getPeerCount());
        }

        if (props.contains(TestProperty.FORCE_FAILURE)) {
            this.host.toggleNegativeTestMode(true);
        }

        this.host.log("%s: Starting replication", this.host.buildTestNameFromStack());

        String factoryPath = this.replicationTargetFactoryLink;
        Map<String, ExampleServiceState> serviceStates = new HashMap<>();
        long before = Utils.getNowMicrosUtc();
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
        updatePerfDataPerAction(Action.POST, before, (long) this.serviceCount, countPerAction,
                elapsedTimePerAction);

        this.host.logThroughput();

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
        return waitForReplicatedFactoryChildServiceConvergence(
                getFactoriesPerNodeGroup(this.replicationTargetFactoryLink),
                serviceStates,
                stateChecker,
                expectedChildCount,
                expectedVersion);
    }

    private <T extends ServiceDocument> Map<String, T> waitForReplicatedFactoryChildServiceConvergence(
            Map<URI, URI> factories,
            Map<String, T> serviceStates,
            BiPredicate<T, T> stateChecker,
            int expectedChildCount, long expectedVersion)
            throws Throwable, TimeoutException {

        // now poll all hosts until they converge: They all have a child service
        // with the expected URI and it has the same state

        Map<String, T> updatedStatesPerSelfLink = new HashMap<>();
        Date expiration = new Date(new Date().getTime()
                + TimeUnit.SECONDS.toMillis(this.host.getTimeoutSeconds()));
        do {

            URI node = factories.keySet().iterator().next();
            AtomicInteger getFailureCount = new AtomicInteger();
            if (expectedChildCount != 0) {
                // issue direct GETs to the services, we do not trust the factory

                for (String link : serviceStates.keySet()) {
                    TestContext ctx = this.host.testCreate(1);
                    Operation get = Operation.createGet(UriUtils.buildUri(node, link))
                            .setReferer(this.host.getReferer())
                            .setExpiration(Utils.getNowMicrosUtc() + TimeUnit.SECONDS.toMicros(5))
                            .setCompletion(
                                    (o, e) -> {
                                        if (e != null) {
                                            getFailureCount.incrementAndGet();
                                        }
                                        ctx.completeIteration();
                                    });
                    this.host.sendRequest(get);
                    this.host.testWait(ctx);
                }

            }

            if (getFailureCount.get() > 0) {
                this.host.log("Child services not propagated yet. Failure count: %d",
                        getFailureCount.get());
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

            long expectedNodeCountPerLinkMax = factories.size();
            long expectedNodeCountPerLinkMin = expectedNodeCountPerLinkMax;
            if (this.replicationFactor != 0) {
                // We expect services to end up either on K nodes, or K + 1 nodes,
                // if limited replication is enabled. The reason we might end up with services on
                // an additional node, is because we elect an owner to synchronize an entire factory,
                // using the factory's link, and that might end up on a node not used for any child.
                // This will produce children on that node, giving us K+1 replication, which is acceptable
                // given K (replication factor) << N (total nodes in group)
                expectedNodeCountPerLinkMax = this.replicationFactor + 1;
                expectedNodeCountPerLinkMin = this.replicationFactor;
            }

            if (expectedChildCount == 0) {
                expectedNodeCountPerLinkMax = 0;
                expectedNodeCountPerLinkMin = 0;
            }

            // build a service link to node map so we can tell on which node each service instance landed
            Map<String, Set<URI>> linkToNodeMap = new HashMap<>();

            boolean isConverged = true;
            for (Entry<URI, ServiceDocumentQueryResult> entry : childServicesPerNode.entrySet()) {
                for (String link : entry.getValue().documentLinks) {
                    if (!serviceStates.containsKey(link)) {
                        this.host.log("service %s not expected, node: %s", link, entry.getKey());
                        isConverged = false;
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

            if (!isConverged) {
                Thread.sleep(500);
                continue;
            }

            // each link must exist on N hosts, where N is either the replication factor, or, if not used, all nodes
            for (Entry<String, Set<URI>> e : linkToNodeMap.entrySet()) {
                if (e.getValue() == null && this.replicationFactor == 0) {
                    this.host.log("Service %s not found on any nodes", e.getKey());
                    isConverged = false;
                    continue;
                }

                if (e.getValue().size() < expectedNodeCountPerLinkMin
                        || e.getValue().size() > expectedNodeCountPerLinkMax) {
                    this.host.log("Service %s found on %d nodes, expected %d -> %d", e.getKey(), e
                            .getValue().size(), expectedNodeCountPerLinkMin,
                            expectedNodeCountPerLinkMax);
                    isConverged = false;
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

            // verify /available reports correct results on the factory.
            URI factoryUri = factories.values().iterator().next();
            Class<?> stateType = serviceStates.values().iterator().next().getClass();
            this.host.waitForReplicatedFactoryServiceAvailable(factoryUri);

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
                                .log("State for %s not converged on node %s. Current state: %s, Initial: %s",
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

                if (convergedNodeCount < expectedNodeCountPerLinkMin
                        || convergedNodeCount > expectedNodeCountPerLinkMax) {
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

    public static class StopVerificationTestService extends StatefulService {

        public Collection<URI> serviceTargets;

        public AtomicInteger outboundRequestCompletion = new AtomicInteger();
        public AtomicInteger outboundRequestFailureCompletion = new AtomicInteger();

        public StopVerificationTestService() {
            super(MinimalTestServiceState.class);
        }

        @Override
        public void handleStop(Operation deleteForStop) {
            // send requests to replicated services, during stop to verify that the
            // runtime prevents any outbound requests from making it out
            for (URI uri : this.serviceTargets) {
                ReplicationTestServiceState body = new ReplicationTestServiceState();
                body.stringField = ReplicationTestServiceState.CLIENT_PATCH_HINT;
                for (int i = 0; i < 10; i++) {
                    // send patch to self, so the select owner logic gets invoked and in theory
                    // queues or cancels the request
                    Operation op = Operation.createPatch(this, uri.getPath()).setBody(body)
                            .setTargetReplicated(true)
                            .setCompletion((o, e) -> {
                                if (e != null) {
                                    this.outboundRequestFailureCompletion.incrementAndGet();
                                } else {
                                    this.outboundRequestCompletion.incrementAndGet();
                                }
                            });
                    sendRequest(op);
                }
            }
        }

    }

    private void stopHostsAndVerifyQueuing(Collection<VerificationHost> hostsToStop,
            VerificationHost remainingHost,
            Collection<URI> serviceTargets) throws Throwable {
        this.host.log("Starting to stop hosts and verify queuing");

        // reduce node expiration for unavailable hosts so gossip warning
        // messages do not flood the logs
        this.nodeGroupConfig.nodeRemovalDelayMicros = remainingHost.getMaintenanceIntervalMicros();
        this.host.setNodeGroupConfig(this.nodeGroupConfig);
        this.setOperationTimeoutMicros(TimeUnit.SECONDS.toMicros(10));

        // relax quorum to single remaining host
        this.host.setNodeGroupQuorum(1);

        // start a special test service that will attempt to send messages when it sees
        // handleStop(). This is not expected of production code, since service authors
        // should never have to worry about handleStop(). We rely on the fact that handleStop
        // will be called due to node shutdown, and issue requests to replicated targets,
        // then check if anyone of them actually made it out (they should not have)

        List<StopVerificationTestService> verificationServices = new ArrayList<>();

        // Do the inverse test. Remove all of the original hosts and this time, expect all the
        // documents have owners assigned to the new hosts
        for (VerificationHost h : hostsToStop) {
            StopVerificationTestService s = new StopVerificationTestService();
            verificationServices.add(s);
            s.serviceTargets = serviceTargets;
            h.startServiceAndWait(s, UUID.randomUUID().toString(), null);
            this.host.stopHost(h);
        }

        Date exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {
            boolean isConverged = true;
            for (StopVerificationTestService s : verificationServices) {
                if (s.outboundRequestCompletion.get() > 0) {
                    throw new IllegalStateException("Replicated request succeeded");
                }
                if (s.outboundRequestFailureCompletion.get() < serviceTargets.size()) {
                    // We expect at least one failure per service target, if we have less
                    // keep polling.
                    this.host.log("Not converged yet: service %s on host %s has %d outbound request failures, which is lower than %d",
                            s.getSelfLink(), s.getHost().getId(), s.outboundRequestFailureCompletion.get(), serviceTargets.size());
                    isConverged = false;
                    break;
                }
            }

            if (isConverged) {
                this.host.log("Done with stop hosts and verify queuing");
                return;
            }

            Thread.sleep(250);
        }

        throw new TimeoutException();
    }
}
