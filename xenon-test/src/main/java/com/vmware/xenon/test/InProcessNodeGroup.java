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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceRequestSender;

/**
 * TODO: documentation
 *
 * @param <H>
 */
public class InProcessNodeGroup<H extends ServiceHost> implements ServiceRequestSender {

    private Set<H> hosts = new HashSet<>();

    public void addHost(H serviceHost) {
        this.hosts.add(serviceHost);
    }

    public Set<H> getAllHosts() {
        return Collections.unmodifiableSet(this.hosts);
    }

    public boolean removeHost(String hostId) {
        Optional<H> host = getHost(hostId);
        if (host.isPresent()) {
            return this.hosts.remove(host.get());
        }
        return false;
    }

    public Map<String, H> getHostMap() {
        return getAllHosts().stream().collect(toMap(ServiceHost::getId, identity()));
    }

    /**
     * Return randomly chosen {@link ServiceHost}.
     */
    public H getHost() {
        if (this.hosts.isEmpty()) {
            // TODO: throw exception
        }
        return this.hosts.stream().findAny().get();
    }

    public Optional<H> getHost(String hostId) {
        return this.hosts.stream().filter(h -> h.getId().equals(hostId)).findAny();
    }


    /**
     * Send {@link Operation} using randomly chosen {@link ServiceHost}
     */
    @Override
    public void sendRequest(Operation op) {
        getHost().sendRequest(op);
    }

    /**
     * wait until cluster is ready
     */
    public void waitForConversion() {
        // TODO: impl
    }

    /**
     * wait until service is ready
     */
    public void waitForServiceAvailable(String servicePath) {

        // TODO: have generic wait mechanism for framework internal. outside of framework is
        // recommended to use 3rd party wait library such as awaitility
        CountDownLatch latch = new CountDownLatch(1);
        getHost().registerForServiceAvailability((o, e) -> {
            latch.countDown();
        }, servicePath);

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw ExceptionTestUtils.throwAsUnchecked(e);
        }
    }

    public void joinHosts(){
        // TODO: impl
    }
}
