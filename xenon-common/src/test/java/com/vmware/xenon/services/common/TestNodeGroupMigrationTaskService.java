/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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

import static java.util.stream.Collectors.toSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.NodeGroupMigrationTaskService.NodeGroupMigrationState;
import com.vmware.xenon.services.common.NodeGroupMigrationTaskService.NodeGroupMigrationState.MigrationRequest;
import com.vmware.xenon.services.common.NodeGroupMigrationTaskService.NodeGroupMigrationState.ResultReport;

/**
 *
 */
public class TestNodeGroupMigrationTaskService {

    public static class FailingFactory extends FactoryService {
        public static final String SELF_LINK = "/failing";

        public FailingFactory() {
            super(ExampleServiceState.class);
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            return new ExampleService();
        }

        @Override
        public void handlePost(Operation op) {
            if (op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_FROM_MIGRATION_TASK)) {
                op.fail(new RuntimeException("Intentionally fail for migration POST"));
                return;
            }
            super.handlePost(op);
        }
    }

    private VerificationHost sourceClusterHolder;
    private VerificationHost destinationClusterHolder;
    private Set<VerificationHost> sourceNodes = new HashSet<>();
    private Set<VerificationHost> destNodes = new HashSet<>();

    @Before
    public void setUp() throws Throwable {

        int sourceNodeCount = 3;
        int destNodeCount = 3;


        this.sourceClusterHolder = VerificationHost.create(0);
        this.destinationClusterHolder = VerificationHost.create(0);

        this.sourceClusterHolder.start();
        this.destinationClusterHolder.start();

        this.sourceClusterHolder.setUpPeerHosts(sourceNodeCount);
        this.destinationClusterHolder.setUpPeerHosts(destNodeCount);

        this.sourceClusterHolder.joinNodesAndVerifyConvergence(sourceNodeCount, true);
        this.destinationClusterHolder.joinNodesAndVerifyConvergence(destNodeCount, true);

        this.sourceClusterHolder.setNodeGroupQuorum(sourceNodeCount);
        this.destinationClusterHolder.setNodeGroupQuorum(destNodeCount);

        this.sourceNodes.addAll(this.sourceClusterHolder.getInProcessHostMap().values());
        this.destNodes.addAll(this.destinationClusterHolder.getInProcessHostMap().values());

        Stream.concat(this.sourceNodes.stream(), this.destNodes.stream()).forEach(node -> {
            node.startFactory(new MigrationTaskService());
            node.startFactory(new NodeGroupMigrationTaskService());
            node.waitForServiceAvailable(MigrationTaskService.FACTORY_LINK);
            node.waitForServiceAvailable(NodeGroupMigrationTaskService.FACTORY_LINK);
            node.waitForServiceAvailable(ServiceUriPaths.DEFAULT_NODE_GROUP);
        });
    }

    @After
    public void tearDown() {
        this.sourceClusterHolder.tearDownInProcessPeers();
        this.destinationClusterHolder.tearDownInProcessPeers();

        this.sourceClusterHolder.tearDown();
        this.destinationClusterHolder.tearDown();
    }

    @Test
    public void factoryDiscovery() {

        VerificationHost sourceNode = this.sourceNodes.iterator().next();
        VerificationHost destNode = this.destNodes.iterator().next();

        // request without body.batches
        NodeGroupMigrationState body = new NodeGroupMigrationState();
        body.sourceNodeReference = sourceNode.getUri();
        body.destinationNodeReference = destNode.getUri();

        NodeGroupMigrationState result = postNodeGroupMigrationTaskAndWaitFinish(body);
        assertEquals(TaskStage.FINISHED, result.taskInfo.stage);

        // check resolved factories
        Set<String> factoryPaths = result.results.stream().map(report -> report.factoryPath).collect(toSet());
        assertThat(factoryPaths).containsOnly(
                ServiceUriPaths.CORE_AUTHZ_USERS, ServiceUriPaths.CORE_AUTHZ_USER_GROUPS,
                ServiceUriPaths.CORE_AUTHZ_RESOURCE_GROUPS, ServiceUriPaths.CORE_AUTHZ_ROLES,
                ServiceUriPaths.CORE_CREDENTIALS, TenantService.FACTORY_LINK,
                ServiceUriPaths.SHARDS_MANAGEMENT_FACTORY,
                ExampleService.FACTORY_LINK, ExampleTaskService.FACTORY_LINK);
    }

    @Test
    public void requestWithFactoryLink() {

        VerificationHost sourceNode = this.sourceNodes.iterator().next();
        VerificationHost destNode = this.destNodes.iterator().next();
        TestRequestSender sender = sourceNode.getTestRequestSender();

        // populate data
        int count = 30;
        List<Operation> posts = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ExampleServiceState body = new ExampleServiceState();
            body.name = "foo-" + i;
            body.documentSelfLink = body.name;
            Operation post = Operation.createPost(sourceNode, ExampleService.FACTORY_LINK).setBody(body);
            posts.add(post);
        }
        sender.sendAndWait(posts);

        NodeGroupMigrationState body = new NodeGroupMigrationState();
        body.sourceNodeReference = sourceNode.getUri();
        body.destinationNodeReference = destNode.getUri();

        // create migration request by specifying factory-link
        MigrationRequest entry = new MigrationRequest();
        entry.factoryLink = ExampleService.FACTORY_LINK;
        List<MigrationRequest> batchEntry = new ArrayList<>();
        batchEntry.add(entry);
        body.batches.add(batchEntry);


        NodeGroupMigrationState result = postNodeGroupMigrationTaskAndWaitFinish(body);
        assertEquals(TaskStage.FINISHED, result.taskInfo.stage);
    }

    @Test
    public void requestWithMigrationTaskServiceState() {

        VerificationHost sourceNode = this.sourceNodes.iterator().next();
        VerificationHost destNode = this.destNodes.iterator().next();
        TestRequestSender sender = sourceNode.getTestRequestSender();

        // populate data
        int count = 30;
        List<Operation> posts = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ExampleServiceState body = new ExampleServiceState();
            body.name = "foo-" + i;
            body.documentSelfLink = body.name;
            Operation post = Operation.createPost(sourceNode, ExampleService.FACTORY_LINK).setBody(body);
            posts.add(post);
        }
        sender.sendAndWait(posts);

        NodeGroupMigrationState body = new NodeGroupMigrationState();
        body.sourceNodeReference = sourceNode.getUri();
        body.destinationNodeReference = destNode.getUri();

        // create request by specifying MigrationTaskService.State

        URI sourceNodeGroupReference = UriUtils.buildUri(sourceNode.getUri(), ServiceUriPaths.DEFAULT_NODE_GROUP);
        URI destNodeGroupReference = UriUtils.buildUri(destNode.getUri(), ServiceUriPaths.DEFAULT_NODE_GROUP);
        MigrationTaskService.State state = new MigrationTaskService.State();
        state.destinationFactoryLink = ExampleService.FACTORY_LINK;
        state.destinationNodeGroupReference = destNodeGroupReference;
        state.sourceFactoryLink = ExampleService.FACTORY_LINK;
        state.sourceNodeGroupReference = sourceNodeGroupReference;

        MigrationRequest entry = new MigrationRequest();
        entry.request = state;

        List<MigrationRequest> batchEntry = new ArrayList<>();
        batchEntry.add(entry);
        body.batches.add(batchEntry);


        NodeGroupMigrationState result = postNodeGroupMigrationTaskAndWaitFinish(body);
        assertEquals(TaskStage.FINISHED, result.taskInfo.stage);
    }

    private NodeGroupMigrationState postNodeGroupMigrationTaskAndWaitFinish(NodeGroupMigrationState requestBody) {
        VerificationHost destNode = this.destNodes.iterator().next();
        TestRequestSender sender = destNode.getTestRequestSender();

        Operation post = Operation.createPost(destNode, NodeGroupMigrationTaskService.FACTORY_LINK).setBody(requestBody);

        NodeGroupMigrationState response = sender.sendAndWait(post, NodeGroupMigrationState.class);
        String taskPath = response.documentSelfLink;

        Set<TaskStage> finalStages = EnumSet.of(TaskStage.CANCELLED, TaskStage.FAILED, TaskStage.FINISHED);

        AtomicReference<NodeGroupMigrationState> state = new AtomicReference<>();
        // when conversion check fails, migration task schedule next check with this interval.
        // To avoid timeout in test, set timeout longer than the interval.
        long waitSecond = TimeUnit.MICROSECONDS.toSeconds(MigrationTaskService.DEFAULT_MAINTENANCE_INTERVAL_MILLIS) * 2;
        TestContext.waitFor(Duration.ofSeconds(waitSecond), () -> {
                    Operation get = Operation.createGet(destNode, taskPath);
                    NodeGroupMigrationState result = sender.sendAndWait(get, NodeGroupMigrationState.class);
                    state.set(result);

                    if (result.taskInfo == null) {
                        // it is possible that taskinfo is not yet ready
                        return false;
                    }
                    return finalStages.contains(result.taskInfo.stage);
                }, "waiting for MigrationService To Finish"
        );
        return state.get();
    }

    @Test
    public void maxRetry() {
        // start failing factory
        Stream.concat(this.sourceNodes.stream(), this.destNodes.stream()).forEach(node -> {
            node.startService(new FailingFactory());
            node.waitForServiceAvailable(FailingFactory.SELF_LINK);
        });

        VerificationHost sourceNode = this.sourceNodes.iterator().next();
        VerificationHost destNode = this.destNodes.iterator().next();
        TestRequestSender sender = sourceNode.getTestRequestSender();

        URI uri = UriUtils.buildUri(sourceNode, FailingFactory.SELF_LINK);
        this.sourceClusterHolder.waitForReplicatedFactoryServiceAvailable(uri);

        // populate data on source
        int count = 3;
        List<Operation> posts = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ExampleServiceState body = new ExampleServiceState();
            body.name = "foo-" + i;
            body.documentSelfLink = body.name;
            Operation post = Operation.createPost(sourceNode, FailingFactory.SELF_LINK).setBody(body);
            posts.add(post);
        }
        sender.sendAndWait(posts);


        // create migration request with maxRetry
        NodeGroupMigrationState body = new NodeGroupMigrationState();
        body.sourceNodeReference = sourceNode.getUri();
        body.destinationNodeReference = destNode.getUri();

        MigrationRequest entry = new MigrationRequest();
        entry.factoryLink = FailingFactory.SELF_LINK;
        List<MigrationRequest> batchEntry = new ArrayList<>();
        batchEntry.add(entry);
        body.batches.add(batchEntry);
        body.maxRetry = 3;


        NodeGroupMigrationState result = postNodeGroupMigrationTaskAndWaitFinish(body);
        assertEquals(TaskStage.FAILED, result.taskInfo.stage);

        List<ResultReport> reports = result.results;
        assertThat(reports).hasSize(3);

        assertEquals(TaskStage.FAILED, reports.get(0).resultState);
        assertEquals(TaskStage.FAILED, reports.get(1).resultState);
        assertEquals(TaskStage.FAILED, reports.get(2).resultState);
        assertEquals(0, reports.get(0).retryCount);
        assertEquals(1, reports.get(1).retryCount);
        assertEquals(2, reports.get(2).retryCount);
    }
}
