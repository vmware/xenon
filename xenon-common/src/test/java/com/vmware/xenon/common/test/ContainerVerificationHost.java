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

package com.vmware.xenon.common.test;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Binds;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.InternetProtocol;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.NodeGroupService;
import com.vmware.xenon.services.common.NodeState;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class ContainerVerificationHost extends VerificationHost {

    private static final long MEGABYTE = 1024 * 1024;
    private static final String BRIDGE_NETWORK = "bridge";
    private static DockerClient dockerClient;

    // container configuration
    public String containerImage;// = "xenon/custom";
    public String containerBindingPath;
    public long containerMemory = 256 * MEGABYTE;
    public boolean attachContainer = true;
    public int containerBindPortLow = 8000;
    public int containerBindPortHigh = 9000;
    public int containerExposedPort;
    public InternetProtocol containerProtocol = InternetProtocol.TCP;
    public String containerNetworkName = BRIDGE_NETWORK;
    // CPU CFS scheduler period in micro-seconds
    public int containerCpuPeriod = 100000;
    // The number of microseconds per cpuPeriod that the container is guaranteed CPU access.
    public int containerCpuQuota = 100000;
    // container Environment variable
    public String[] containerEnv;
    // publicUri -> container Id
    private Map<URI, String> containerIdMapping = new HashMap<>();
    // address translation from public uri to cluster uri
    private Map<URI, URI> networkMapping = new HashMap<>();

    private URI uri;

    private boolean isStarted;

    public static ContainerVerificationHost create(int port) throws Throwable {
        return create(port, null);
    }

    public static ContainerVerificationHost create(int port, String id) throws Throwable {
        return create(port, id, null);
    }

    public static ContainerVerificationHost create(int port, String id, DockerClientConfig dockerClientConfig) throws Throwable {
        ServiceHost.Arguments args = buildDefaultServiceHostArguments(port);
        if (id != null) {
            args.id = id;
        }
        return (ContainerVerificationHost) initialize(new ContainerVerificationHost(dockerClientConfig), args);
    }

    public ContainerVerificationHost(DockerClientConfig dockerClientConfig) {
        if (this.dockerClient == null) {
            CommandLineArgumentParser.parseFromProperties(this);
            if (this.containerImage == null) {
                throw new IllegalArgumentException("null containerImage");
            }
            DockerClientConfig config = dockerClientConfig != null ?
                    dockerClientConfig : DefaultDockerClientConfig.createDefaultConfigBuilder().build();
            this.dockerClient = DockerClientBuilder.getInstance(config).build();
        }
    }

    @Override
    public void setUpPeerHosts(int localHostCount) {
        CommandLineArgumentParser.parseFromProperties(this);
        if (this.peerNodes == null) {
            this.setUpLocalPeersHosts(localHostCount, null);
        } else {
            this.setUpWithRemotePeers(this.peerNodes);
        }
    }

    /**
     * create container
     * @return
     * @throws Throwable
     */
    public CreateContainerResponse createAndStartContainer(long maintIntervalMicros, String location) throws Throwable {
        // port exposed by container
        ExposedPort port = new ExposedPort(this.containerExposedPort, this.containerProtocol);
        // mapping from container port -> port on host
        Ports ports = new Ports(port, Ports.Binding.bindPortRange(this.containerBindPortLow,this.containerBindPortHigh));

        HostConfig hostConfig = new HostConfig()
                .withCpuPeriod(this.containerCpuPeriod)
                .withCpuQuota(this.containerCpuQuota)
                .withMemory(this.containerMemory)
                .withPortBindings(ports);
        // volume mount on host
        if (this.containerBindingPath != null) {
            Binds binds = new Binds(new Bind(this.containerBindingPath, new Volume(this.containerBindingPath)));
            hostConfig.withBinds(binds);
        }

        List<String> env = new ArrayList<>();

        env.add(String.format("Dxenon.maintIntervalMicros=%d", maintIntervalMicros));
        env.add(String.format("Dxenon.location=%s", location));
        env.add(String.format("BINDING_PORT=%d", this.containerExposedPort));

        if (this.containerEnv != null) {
            env.addAll(Arrays.asList(this.containerEnv));
        }

        CreateContainerResponse container = this.dockerClient.createContainerCmd(this.containerImage)
                .withHostConfig(hostConfig)
                .withExposedPorts(port)
                .withEnv(env)
                .withAttachStdout(this.attachContainer)
                .exec();

        this.dockerClient.startContainerCmd(container.getId()).exec();
        return container;
    }

    public void setUpLocalPeerHost(String id) throws Throwable {
        InspectContainerResponse icr = this.dockerClient.inspectContainerCmd(id).exec();
        ExposedPort exposedPort = new ExposedPort(this.containerExposedPort, this.containerProtocol);
        Ports.Binding binding = icr.getNetworkSettings().getPorts().getBindings().get(exposedPort)[0];
        URI publicUri = UriUtils.buildUri(UriUtils.HTTP_SCHEME, binding.getHostIp(), Integer.valueOf(binding.getHostPortSpec()), null, null, null);

        ContainerNetwork cn = icr.getNetworkSettings().getNetworks().get(this.containerNetworkName);
        URI clusterUri = UriUtils.buildUri(UriUtils.HTTP_SCHEME, cn.getIpAddress(), this.containerExposedPort, null, null, null);
        // wait for node selector service started in container
        this.waitFor("node selector available timeout", () -> {
            Operation get = Operation.createGet(UriUtils.buildUri(publicUri, ServiceUriPaths.DEFAULT_NODE_SELECTOR))
                    .setExpiration(TimeUnit.MINUTES.toMicros(1));
            try {
                this.getTestRequestSender().sendAndWait(get);
            } catch (Exception e) {
                return false;
            }
            return true;
        });

        // build a dummy peer, though real peer run in container
        // assign xenon node id same as container id
        // one on one fixed mapping, just for testing purpose
        ContainerVerificationHost h = ContainerVerificationHost.create(0, id);
        h.setPublicUri(publicUri);
        h.uri = publicUri;
        h.isStarted = true;
        addPeerNode(h);

        this.containerIdMapping.put(publicUri, id);
        this.networkMapping.put(publicUri, clusterUri);
    }

    @Override
    public void setUpLocalPeersHosts(int localHostCount, Long maintIntervalMillis) {
        testStart(localHostCount);
        if (maintIntervalMillis == null) {
            maintIntervalMillis = this.maintenanceIntervalMillis;
        }
        final long intervalMicros = TimeUnit.MILLISECONDS.toMicros(maintIntervalMillis);
        for (int i = 0; i < localHostCount; i++) {
            String location = this.isMultiLocationTest
                    ? ((i < localHostCount / 2) ? LOCATION1 : LOCATION2)
                    : null;
            run(() -> {
                try {
                    this.setUpLocalPeerHostInContainer(null, intervalMicros, location);
                } catch (Throwable e) {
                    failIteration(e);
                }
            });
        }
        testWait();
    }

    public CreateContainerResponse setUpLocalPeerHostInContainer(Collection<String> containerIds,
                                                                 long maintIntervalMicros, String location) throws Throwable {
        CreateContainerResponse c;
        try {
            c = createAndStartContainer(maintIntervalMicros, location);
            setUpLocalPeerHost(c.getId());
            if (containerIds != null) {
                containerIds.add(c.getId());
            }
        } catch (Throwable e) {
            throw new Exception(e);
        }
        this.completeIteration();
        return c;
    }

    @Override
    public void joinNodeGroup(URI newNodeGroupService,
                              URI nodeGroup, Integer quorum) {
        if (nodeGroup.equals(newNodeGroupService)) {
            return;
        }
        // map from public to cluster space
        URI publicUri =
                UriUtils.buildUri(nodeGroup.getScheme(), nodeGroup.getHost(), nodeGroup.getPort(), null, null, null);
        String nodeGroupService = nodeGroup.getPath();
        URI clusterUri = this.networkMapping.get(publicUri);
        URI clusterNodeGroup = UriUtils.buildUri(clusterUri, nodeGroupService);
        // to become member of a group of nodes, you send a POST to self
        // (the local node group service) with the URI of the remote node
        // group you wish to join
        NodeGroupService.JoinPeerRequest joinBody = NodeGroupService.JoinPeerRequest.create(clusterNodeGroup, quorum);

        log("Joining %s through %s(%s)",
                newNodeGroupService, nodeGroup, clusterNodeGroup);
        // send the request to the node group instance we have picked as the
        // "initial" one
        send(Operation.createPost(newNodeGroupService)
                .setBody(joinBody)
                .setCompletion(getCompletion()));
    }

    @Override
    public void stopHost(VerificationHost host) {
        URI publicUri = host.getPublicUri();
        String containerId = this.containerIdMapping.get(publicUri);
        log("Stopping host %s then removing container id %s", publicUri, containerId);
        this.dockerClient.stopContainerCmd(containerId).exec();
        this.dockerClient.removeContainerCmd(containerId).exec();
        this.networkMapping.remove(publicUri);
        this.containerIdMapping.remove(containerId);
        super.stopHost(host);
    }

    public void resumeHostInContainer(ContainerVerificationHost host) {
        String id = host.getId();
        InspectContainerResponse icr = null;
        try {
            icr = this.dockerClient.inspectContainerCmd(id).exec();
        } catch (NotFoundException e) {
            return;
        }
        if (!icr.getState().getStatus().equals("paused")) {
            return;
        }
        // container is not paused from running status
        if (!icr.getState().getRunning()) {
            return;
        }
        this.dockerClient.unpauseContainerCmd(id).exec();
        // add host back to peer
        ExposedPort exposedPort = new ExposedPort(this.containerExposedPort, this.containerProtocol);
        Ports.Binding binding = icr.getNetworkSettings().getPorts().getBindings().get(exposedPort)[0];
        URI publicUri = UriUtils.buildUri(UriUtils.HTTP_SCHEME, binding.getHostIp(), Integer.valueOf(binding.getHostPortSpec()), null, null, null);

        ContainerNetwork cn = icr.getNetworkSettings().getNetworks().get(this.containerNetworkName);
        URI clusterUri = UriUtils.buildUri(UriUtils.HTTP_SCHEME, cn.getIpAddress(), this.containerExposedPort, null, null, null);
        // wait for node selector service started in container
        this.waitFor("node selector available timeout", () -> {
            Operation get = Operation.createGet(UriUtils.buildUri(publicUri, ServiceUriPaths.DEFAULT_NODE_SELECTOR))
                    .setExpiration(TimeUnit.MINUTES.toMicros(1));
            try {
                this.getTestRequestSender().sendAndWait(get);
            } catch (Exception e) {
                return false;
            }
            return true;
        });
        host.setPublicUri(publicUri);
        host.uri = publicUri;
        host.isStarted = true;
        addPeerNode(host);

        this.containerIdMapping.put(publicUri, id);
        this.networkMapping.put(publicUri, clusterUri);
        return;
    }

    @Override
    public void stopHostAndPreserveState(ServiceHost host) {
        URI publicUri = host.getPublicUri();
        String containerId = this.containerIdMapping.get(publicUri);
        log("Stopping host %s and pausing container id %s", publicUri, containerId);
        this.dockerClient.pauseContainerCmd(containerId).exec();
        this.networkMapping.remove(publicUri);
        this.containerIdMapping.remove(containerId);
        super.stopHostAndPreserveState(host);
    }

    @Override
    public void waitForNodeGroupConvergence(Collection<URI> nodeGroupUris,
                                            int healthyMemberCount,
                                            Integer totalMemberCount,
                                            Map<URI, EnumSet<NodeState.NodeOption>> expectedOptionsPerNodeGroupUri,
                                            boolean waitForTimeSync) {

        Set<String> nodeGroupNames = nodeGroupUris.stream()
                .map(URI::getPath)
                .map(UriUtils::getLastPathSegment)
                .collect(toSet());
        if (nodeGroupNames.size() != 1) {
            throw new RuntimeException("Multiple nodegroups are not supported. " + nodeGroupNames);
        }
        String nodeGroupName = nodeGroupNames.iterator().next();

        Set<URI> baseUris = nodeGroupUris.stream()
                .map(ngUri -> ngUri.toString().replace(ngUri.getPath(), ""))
                .map(URI::create)
                .collect(toSet());

        this.waitFor("Node group did not converge", () -> {
            String nodeGroupPath = ServiceUriPaths.NODE_GROUP_FACTORY + "/" + nodeGroupName;
            List<Operation> nodeGroupOps = baseUris.stream()
                    .map(u -> UriUtils.buildUri(u, nodeGroupPath))
                    .map(Operation::createGet)
                    .collect(toList());
            List<NodeGroupService.NodeGroupState> nodeGroupStates = getTestRequestSender()
                    .sendAndWait(nodeGroupOps, NodeGroupService.NodeGroupState.class);

            Set<Long> unique = new HashSet<>();
            for (NodeGroupService.NodeGroupState nodeGroupState : nodeGroupStates) {
                unique.add(nodeGroupState.membershipUpdateTimeMicros);
            }
            if (unique.size() == 1) {
                return true;
            }
            return false;
        });
    }

    @Override
    public boolean isStarted() {
        return this.isStarted;
    }

    @Override
    public URI getUri() {
        if (this.uri != null) {
            return this.uri;
        }
        return super.getUri();
    }

    @Override
    public ContainerVerificationHost getPeerHost() {
        return (ContainerVerificationHost) super.getPeerHost();
    }

    // container short id is the prefix of full id
    public boolean containPeerId(String id) {
        for (String peerId : this.containerIdMapping.values()) {
            if (peerId.startsWith(id)) {
                return true;
            }
        }
        return false;
    }
}
