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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import org.assertj.core.data.Percentage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.config.TestXenonConfiguration;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupConfig;
import com.vmware.xenon.services.common.ShardsManagementService.CreateOrGetShardInfoRequest;
import com.vmware.xenon.services.common.ShardsManagementService.ShardInfo;
import com.vmware.xenon.services.common.ShardsManagementService.ShardInfoResponse;

public class TestShardsManagementService extends BasicReusableHostTestCase {

    /**
     * Parameter that specifies the number of key-values to create
     */
    public int keyValueCount = 90;

    /**
     * Parameter that specifies the number of nodes in the nodegroup
     */
    public int nodeCount = 9;

    /**
     * Parameter that specifies how many nodes each document needs
     * to be replicated to
     */
    public int replicationFactor = 3;

    @Before
    public void prepare() throws Throwable {
        this.host.setUpPeerHosts(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
    }

    @After
    public void tearDown() {
        this.host.tearDownInProcessPeers();
        TestXenonConfiguration.restore();
    }

    private TestRequestSender getTestRequestSender() {
        return this.host.getTestRequestSender();
    }

    private <T> T sendAndWait(Operation op, Class<T> bodyType) {
        Operation response = getTestRequestSender().sendAndWait(op);
        return response.getBody(bodyType);
    }

    private ShardInfo createOrGetShard(String shardKeyValue) {
        return createOrGetShard(shardKeyValue, false);
    }

    private ShardInfo createOrGetShard(String shardKeyValue, boolean expectFailure) {
        CreateOrGetShardInfoRequest createOrGetShardRequest = new CreateOrGetShardInfoRequest(shardKeyValue);
        URI shardsManagerUri = this.host.getPeerServiceUri(ServiceUriPaths.SHARDS_MANAGER);

        Operation patch = Operation.createPatch(shardsManagerUri)
                .setBody(createOrGetShardRequest);

        if (expectFailure) {
            getTestRequestSender().sendAndWaitFailure(patch);
            return null;
        }

        ShardInfoResponse res = sendAndWait(patch, ShardInfoResponse.class);
        return res.shardInfo;
    }

    @Test
    public void uniformDistribution() throws Throwable {
        // In this test we create many random key values and ask the shard manager to create
        // or get a shard for each key value. We then verify that the shard's nodes are
        // valid nodes within the node group, and that the distribution of shard key values
        // to nodes is roughly uniform

        this.host.getPeerHost().createOrStartShardsManagerSynchronously(this.replicationFactor);

        Map<String, Integer> nodeIdToKeyValueCount = new HashMap<>();
        Map<String, Set<String>> nodeIdToShardIds = new HashMap<>();
        Set<String> shardIds = new HashSet<>();
        for (VerificationHost node : this.host.getInProcessHostMap().values()) {
            nodeIdToKeyValueCount.put(node.getId(), 0);
            nodeIdToShardIds.put(node.getId(), new HashSet<>());
        }

        // generate key values and create shards
        for (int i = 0; i < this.keyValueCount; i++) {
            String shardKeyValue = UUID.randomUUID().toString();
            ShardInfo shardInfo = createOrGetShard(shardKeyValue);

            // verify that the created shard's nodes are nodegroup members
            assertThat(nodeIdToKeyValueCount.keySet()).containsAll(shardInfo.shardNodes);

            // verify that the number of shard's nodes is equal to replication factor
            assertEquals(this.replicationFactor, shardInfo.shardNodes.size());

            for (String nodeId : shardInfo.shardNodes) {
                // increment per node key-value counter
                nodeIdToKeyValueCount.compute(nodeId, (k, count) -> {
                    return count + 1;
                });

                // update per node shard id set
                nodeIdToShardIds.compute(nodeId, (k, shards) -> {
                    shards.add(shardInfo.shardId);
                    return shards;
                });
            }

            shardIds.add(shardInfo.shardId);
        }

        int shardCount = shardIds.size();
        assertThat(shardCount).isLessThanOrEqualTo(keyValueCount);

        // verify roughly uniform distribution: we expect each node:
        // 1) to contain roughly keyValueCount / replicationFactor key values
        // 2) to contain roughly shardCount / replicationFactor shards
        int expectedKeyValueCountPerNode = this.keyValueCount / this.replicationFactor;
        int expectedShardCountPerNode = shardCount / this.replicationFactor;
        for (Integer keyValueCount : nodeIdToKeyValueCount.values()) {
            assertThat(keyValueCount).isCloseTo (expectedKeyValueCountPerNode, Percentage.withPercentage(34.0));
        }
        for (Set<String> shards : nodeIdToShardIds.values()) {
            assertThat(shards.size()).isCloseTo (expectedShardCountPerNode, Percentage.withPercentage(34.0));
        }
    }

    @Test
    public void consistentShardId() throws Throwable {
        // This test verifies a client gets the same shard id for a given
        // key-value, even in the face of nodegroup changes

        this.host.getPeerHost().createOrStartShardsManagerSynchronously(this.replicationFactor);

        Map<String, String> keyValueToShardId = new HashMap<>();

        // create some shards
        for (int i = 0; i < this.keyValueCount; i++) {
            String keyValue = UUID.randomUUID().toString();
            ShardInfo shardInfo = createOrGetShard(keyValue);

            keyValueToShardId.put(keyValue, shardInfo.shardId);
        }

        // re-create with the same key values, and verify we get the same shard ids
        for (Entry<String, String> e : keyValueToShardId.entrySet()) {
            String keyValue = e.getKey();
            String shardId = e.getValue();

            ShardInfo shardInfo = createOrGetShard(keyValue);
            assertEquals(shardId, shardInfo.shardId);
        }

        // remove a node from the group, verify we get consistent shard ids
        // after removal
        int newNodeCount = this.nodeCount - 1;

        // relax quorum
        this.host.setNodeGroupQuorum(newNodeCount);

        // expire node quickly to avoid a lot of log spam from gossip failures
        NodeGroupConfig cfg = new NodeGroupConfig();
        cfg.nodeRemovalDelayMicros = TimeUnit.SECONDS.toMicros(1);
        this.host.setNodeGroupConfig(cfg);

        // stop one of the hosts, preserve its index
        VerificationHost stoppedHost = this.host.getPeerHost();
        this.host.stopHostAndPreserveState(stoppedHost);

        // wait for stopped host to be removed from node group
        this.host.waitForNodeGroupConvergence(newNodeCount, newNodeCount);

        // re-create with the same key values, and verify we get the same shard ids
        for (Entry<String, String> e : keyValueToShardId.entrySet()) {
            String keyValue = e.getKey();
            String shardId = e.getValue();

            ShardInfo shardInfo = createOrGetShard(keyValue);
            assertEquals(shardId, shardInfo.shardId);
        }
    }

    @Test
    public void NoShardSharing() throws Throwable {
        // This test verifies that two different key values are mapped to
        // different shards with non-overlapping node members, in case
        // ALLOW_SHARD_SHARING is disabled

        // we need more key values than number of nodes for this test
        assertThat(this.keyValueCount).isGreaterThan(this.nodeCount);

        TestXenonConfiguration.override(
                ShardsManagementService.class,
                "ALLOW_SHARD_SHARING",
                "false"
        );

        this.host.getPeerHost().createOrStartShardsManagerSynchronously(this.replicationFactor);

        Set<String> shardIds = new HashSet<>();
        Map<String, Set<String>> nodeIdToShardIds = new HashMap<>();
        for (VerificationHost node : this.host.getInProcessHostMap().values()) {
            nodeIdToShardIds.put(node.getId(), new HashSet<>());
        }

        int shardCount = this.nodeCount / this.replicationFactor;

        for (int i = 0; i < shardCount; i++) {
            String shardKeyValue = UUID.randomUUID().toString();
            ShardInfo shardInfo = createOrGetShard(shardKeyValue);

            // verify that a new shard has been created
            assertThat(shardIds).doesNotContain(shardInfo.shardId);
            shardIds.add(shardInfo.shardId);

            // verify that the number of shard's nodes is equal to replication factor
            assertEquals(this.replicationFactor, shardInfo.shardNodes.size());

            // verify that the shard's nodes are not mapped to any other shard(s)
            for (String nodeId : shardInfo.shardNodes) {
                Set<String> shards = nodeIdToShardIds.get(nodeId);
                shards.add(shardInfo.shardId);
                assertThat(shards).containsOnly(shardInfo.shardId);
            }
        }

        // try to create an additional shard - it should fail because all nodes already
        // have shards
        String shardKeyValue = UUID.randomUUID().toString();
        createOrGetShard(shardKeyValue, true);
    }
}