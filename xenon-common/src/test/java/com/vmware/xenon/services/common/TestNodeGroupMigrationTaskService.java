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

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.NodeGroupMigrationTaskService.MigrationState;
import com.vmware.xenon.services.common.NodeGroupMigrationTaskService.MigrationState.MigrationRequest;

/**
 *
 */
public class TestNodeGroupMigrationTaskService {

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
        this.destinationClusterHolder.setUpPeerHosts(sourceNodeCount);

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


            node.setTimeoutSeconds((int)Duration.ofHours(1).getSeconds());
        });
    }

    @After
    public void tearDown() throws Throwable {
        this.sourceClusterHolder.tearDownInProcessPeers();
        this.destinationClusterHolder.tearDownInProcessPeers();

        this.sourceClusterHolder.tearDown();
        this.destinationClusterHolder.tearDown();
    }

    @Test
    public void test() {
        // populate data

        VerificationHost sourceNode = this.sourceNodes.iterator().next();
        VerificationHost destNode = this.destNodes.iterator().next();
        TestRequestSender sender = sourceNode.getTestRequestSender();

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

        MigrationState body = new MigrationState();
        body.sourceNodeReference = sourceNode.getUri();
        body.destinationNodeReference = destNode.getUri();
        body.sourceNodeGroupPath = ServiceUriPaths.DEFAULT_NODE_GROUP;
        body.destinationNodeGroupPath = ServiceUriPaths.DEFAULT_NODE_GROUP;

        MigrationRequest entry = new MigrationRequest();
        entry.factoryLink = ExampleService.FACTORY_LINK;
        List<MigrationRequest> batchEntry = new ArrayList<>();
        batchEntry.add(entry);
        body.batches.add(batchEntry);


        MigrationState result = postNodeGroupMigrationTaskAndWaitFinish(body);
        assertEquals(TaskStage.FINISHED, result.taskInfo.stage);
    }

    private MigrationState postNodeGroupMigrationTaskAndWaitFinish(MigrationState requestBody) {
        VerificationHost destNode = this.destNodes.iterator().next();
        TestRequestSender sender = destNode.getTestRequestSender();

        Operation post = Operation.createPost(destNode, NodeGroupMigrationTaskService.FACTORY_LINK).setBody(requestBody);

        MigrationState response = sender.sendAndWait(post, MigrationState.class);
        String taskPath = response.documentSelfLink;

        Set<TaskStage> finalStages = EnumSet.of(TaskStage.CANCELLED, TaskStage.FAILED, TaskStage.FINISHED);


        AtomicReference<MigrationState> state = new AtomicReference<>();
        destNode.waitFor("waiting for MigrationService To Finish", () -> {
                    Operation get = Operation.createGet(destNode, taskPath);
                    MigrationState result = sender.sendAndWait(get, MigrationState.class);
                    state.set(result);

                    if (result.taskInfo == null) {
                        // it is possible that taskinfo is not yet ready
                        return false;
                    }
                    return finalStages.contains(result.taskInfo.stage);
                }
        );
        return state.get();
    }
}
