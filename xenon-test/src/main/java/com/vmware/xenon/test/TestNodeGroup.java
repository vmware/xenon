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

package com.vmware.xenon.test;

import com.vmware.xenon.common.ServiceHost;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class TestNodeGroup<H extends ServiceHost> {

    private Set<H> hosts = new HashSet<>();
    private Set<NodeClient<H>> clients = new HashSet<>();

    public void add(H serviceHost) {
        clients.add(new NodeClient<>(serviceHost));
    }

    public Set<H> getAllNodes() {
        return clients.stream().map(NodeClient::getServiceHost).collect(toSet());
    }

    public Map<String, H> getAllNodesMapById() {
        return getAllNodes().stream().collect(toMap(ServiceHost::getId, identity()));
    }

    public H getRandomHost() {
        if (this.clients.isEmpty()) {
            // TODO: throw exception
        }
        return getAllNodes().stream().findAny().get();
    }

    public H getHostById(String hostId) {
        // TODO: impl
        return null;
    }

    public Set<NodeClient<H>> getAllClients() {
        // TODO: impl
        return null;
    }

    public Map<String, NodeClient<H>> getAllClientsById() {
        // TODO: impl
        return null;
    }

    public NodeClient<H> getRandomClient() {
        if (this.clients.isEmpty()) {
            // TODO: throw exception
        }
        return this.clients.stream().findAny().get();
    }

    public NodeClient<H> getClientById(String hostId) {
        // TODO: impl
        return null;
    }

    /**
     * wait until cluster is ready
     */
    public void waitConversion() {
        // TODO: impl
    }

    /**
     * wait until service is ready
     */
    public void waitService(String servicePath) {

        // TODO: have generic wait mechanism for framework internal. outside of framework is
        // recommended to use 3rd party wait library such as awaitility
        CountDownLatch latch = new CountDownLatch(1);
        getRandomHost().registerForServiceAvailability((o, e) -> {
            latch.countDown();
        }, servicePath);

        try {
            latch.await();
        } catch (InterruptedException e) {
            ExceptionTestUtils.throwAsUnchecked(e);
        }

    }
}
