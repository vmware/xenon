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

package com.vmware.xenon.common.test;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;


/**
 * Provides node group related methods for test.
 */
public class TestNodeGroupManager {

    private Set<ServiceHost> hosts = new HashSet<>();

    public void addHost(ServiceHost serviceHost) {
        this.hosts.add(serviceHost);
    }

    public Set<ServiceHost> getAllHosts() {
        return Collections.unmodifiableSet(this.hosts);
    }

    public boolean removeHost(String hostId) {
        Optional<ServiceHost> host = getHost(hostId);
        if (host.isPresent()) {
            return this.hosts.remove(host.get());
        }
        return false;
    }

    public Map<String, ServiceHost> getHostMap() {
        return getAllHosts().stream().collect(toMap(ServiceHost::getId, identity()));
    }

    /**
     * Return randomly chosen {@link ServiceHost}.
     */
    public ServiceHost getHost() {
        if (this.hosts.isEmpty()) {
            // TODO: throw exception
        }
        return this.hosts.stream().findAny().get();
    }

    public Optional<ServiceHost> getHost(String hostId) {
        return this.hosts.stream().filter(h -> h.getId().equals(hostId)).findAny();
    }



    /**
     * Send {@link Operation} using randomly chosen {@link ServiceHost}
     */
    public void sendRequest(Operation op) {
        getHost().sendRequest(op);
    }

    /**
     * Make all hosts join the node group and wait for convergence
     */
    public void joinNodeGroupAndWaitForConvergence(String nodeGroupName){
        // TODO: impl
    }


    /**
     * wait until cluster is ready
     */
    public void waitForConvergence() {
        // TODO: impl - check "membershipUpdateTimeMicros" in all node
    }

    /**
     * wait until service is ready
     */
    public void waitForServiceAvailable(String servicePath) {
        // TODO: impl - check factory owner stats isAvailable
    }
}
