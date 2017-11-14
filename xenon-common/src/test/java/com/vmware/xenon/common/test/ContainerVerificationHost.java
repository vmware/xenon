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

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UnknownFormatConversionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Binds;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.InternetProtocol;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.Statistics;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.async.ResultCallbackTemplate;
import com.github.dockerjava.core.command.LogContainerResultCallback;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.NodeGroupService;
import com.vmware.xenon.services.common.NodeState;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class ContainerVerificationHost extends VerificationHost {

    public enum ContainerStatus {
        RUNNING,
        PAUSING,
        STOPPING
    }

    private static final long MEGABYTE = 1024 * 1024;
    private static final String BRIDGE_NETWORK = "bridge";
    private static DockerClient dockerClient;

    // container configuration
    public String containerImage;
    public String containerBindingPath;
    public long containerMemory = 256 * MEGABYTE;
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
    // map for container status lookup
    private ConcurrentHashMap<String, ContainerStatus> containerStatusMapping = new ConcurrentHashMap<>();

    private URI uri;
    private boolean isStarted;

    public int stopContainerTimeout = 10;
    // logging config
    public boolean logContainer = true;
    public long logPeriod = 1;
    public long logCallBackTimeout = 10;

    // stats config
    public boolean statsContainer = true;
    public int statsBatch = 10;
    public long statsPeriod = 1;
    public long statsCallBackTimeout = 10;

    public static class LogContainerCallback extends LogContainerResultCallback {
        private static ContainerVerificationHost logHost;
        // RFC 3339, resolution to second which is limited by docker api
        public static final Pattern timestampPatternSecond = Pattern.compile("\\d+-\\d+-\\d+T\\d+:\\d+:\\d+[.]");
        public static final Pattern timestampPatternNanoSecond = Pattern.compile("\\d+-\\d+-\\d+T\\d+:\\d+:\\d+[.]\\d+[Z]");
        private final URI containerUri;
        private int latestTimestamp = 0;

        public LogContainerCallback(ContainerVerificationHost host, URI clusterUri, int timestamp) {
            if (logHost == null) {
                logHost = host;
            }
            this.containerUri = clusterUri;
            this.latestTimestamp = timestamp;
        }

        @Override
        public void onNext(Frame frame) {
            String logFrame = new String(frame.getPayload());
            try {
                int timestamp =  LogContainerCallback.parseTimeInSecond(logFrame).intValue();
                if (timestamp > this.latestTimestamp) {
                    this.latestTimestamp = timestamp;
                }
                // trim timestamp from docker, use timestamp in xenon
                logFrame = LogContainerCallback.trimTime(logFrame);
            } catch (Throwable t) {

            }

            try {
                logHost.log(Level.INFO, String.format("%s: %s", this.containerUri, logFrame));
            } catch (UnknownFormatConversionException e) {

            }
        }

        public int getLatestTimestamp() {
            return this.latestTimestamp;
        }

        public static Long parseTimeInSecond(String log) throws Throwable {
            Matcher m = timestampPatternSecond.matcher(log);
            if (m.find()) {
                String timestampStr = log.substring(m.start(), m.end() - 1);
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
                long timestamp = formatter.parse(timestampStr).getTime();
                return timestamp / 1000;
            }
            return  0L;
        }

        public static String trimTime(String log) throws Throwable {
            Matcher m = timestampPatternNanoSecond.matcher(log);
            if (m.find()) {
                return log.substring(m.end()).trim();
            }
            return log;
        }
    }

    public static class StatsContainerCallback extends ResultCallbackTemplate<StatsContainerCallback, Statistics> {

        private static ContainerVerificationHost logHost;
        private final URI containerUri;
        private final String containerId;
        private final CountDownLatch countDownLatch;
        private Statistics preStats;
        private static final String CPU_USAGE = "cpu_usage";
        private static final String TOTAL_USAGE = "total_usage";
        private static final String ONLINE_CPUS = "online_cpus";
        private static final String SYSTEM_CPU_USAGE = "system_cpu_usage";
        private static final String IO_SERVICE_BYTES_RECURSIVE = "io_service_bytes_recursive";
        private static final String RX_BYTES = "rx_bytes";
        private static final String TX_BYTES = "tx_bytes";
        private static final String MEM_LIMIT = "limit";
        private static final String MEM_USAGE = "usage";

        public StatsContainerCallback(ContainerVerificationHost host,
                                      URI clusterUri,
                                      String id,
                                      CountDownLatch countDownLatch,
                                      Statistics stats) {
            if (logHost == null) {
                logHost = host;
            }
            this.containerUri = clusterUri;
            this.countDownLatch = countDownLatch;
            this.containerId = id;
            this.preStats = stats;
        }

        @Override
        public void onNext(Statistics stats) {
            Map<String, Object> memStat = stats.getMemoryStats();
            Map<String, Object> blkioStat = stats.getBlkioStats();
            Map<String, Object> netStat = stats.getNetworks();

            double cpuPercent = 0.0;
            if (this.preStats != null) {
                cpuPercent = calculateCpuPercentUnix(this.preStats, stats);
            }
            int[] netIO = calculateNetwork(netStat);
            int[] memUsageLimit = calculateMemory(memStat);
            int[] blockIo = calculateBlock(blkioStat);
            logHost.log(Level.INFO, "\n%s(%s)\nCPU %4.3f%% | MEM %4.3f%% (%d KB / %d KB) | NET %d KB / %d KB | BLOCK %d KB / %d KB",
                    this.containerId, this.containerUri,
                    cpuPercent,
                    (memUsageLimit[0] * 1.0 / memUsageLimit[1] * 1.0) * 100, memUsageLimit[0] / 1024, memUsageLimit[1] / 1024,
                    netIO[0] / 1024, netIO[1] / 1024,
                    blockIo[0] / 1024, blockIo[1] / 1024);
            this.preStats = stats;
            this.countDownLatch.countDown();
        }

        /**
         * reference: {@Link https://github.com/moby/moby/blob/eb131c5383db8cac633919f82abad86c99bffbe5/cli/command/container/stats_helpers.go}
         * @return
         */
        public static double calculateCpuPercentUnix(Statistics preCpuStats, Statistics curCpuStats) {

            Map<String, Object> preCpuStat = preCpuStats.getCpuStats();
            long preSystemUsage = (Long) preCpuStat.get(SYSTEM_CPU_USAGE);
            Map<String, Object> preCpuUsage = (Map) preCpuStat.get(CPU_USAGE);
            long preTotalUsage = (Long) preCpuUsage.get(TOTAL_USAGE);
            int preCpuCount = (Integer) preCpuStat.get(ONLINE_CPUS);

            Map<String, Object> curCpuStat = curCpuStats.getCpuStats();
            long curSystemUsage = (Long) curCpuStat.get(SYSTEM_CPU_USAGE);
            Map<String, Object> curCpuUsage = (Map) curCpuStat.get(CPU_USAGE);
            long curTotalUsage = (Long) curCpuUsage.get(TOTAL_USAGE);
            int curCpuCount = (Integer) curCpuStat.get(ONLINE_CPUS);

            double cpuPercent = 0.0;
            if (curCpuCount != preCpuCount) {
                logHost.log(Level.INFO, String.format("cpu count miss match pre: %d, cur: %d", preCpuCount, curCpuCount));
                return cpuPercent;
            }

            double cpuDelta = curTotalUsage - preTotalUsage;
            double systemDelta = curSystemUsage - preSystemUsage;
            if (systemDelta > 0.0 && cpuDelta > 0.0) {
                cpuPercent = (cpuDelta / systemDelta) * curCpuCount * 100.0;
            }
            return cpuPercent;
        }

        public static int[] calculateNetwork(Map<String, Object> netStats) {
            int receive = 0;
            int transfer = 0;
            for (Object object : netStats.values()) {
                Map<String, Object> stat = (Map) object;
                receive += (Integer) stat.get(RX_BYTES);
                transfer += (Integer) stat.get(TX_BYTES);
            }
            return new int[]{receive, transfer};
        }

        public static int[] calculateMemory(Map<String, Object> memStat) {
            int usage = (Integer) memStat.get(MEM_USAGE);
            int limit = (Integer) memStat.get(MEM_LIMIT);
            return new int[]{usage, limit};
        }

        public static int[] calculateBlock(Map<String, Object> blockStat) {
            int read = 0;
            int write = 0;
            List<Object> blkios = (List) blockStat.get(IO_SERVICE_BYTES_RECURSIVE);
            for (Object object : blkios) {
                Map<String, Object> blkio = (Map) object;
                String op = (String) blkio.get("op");
                if (op.equals("Read")) {
                    read += (Integer) blkio.get("value");
                    continue;
                }
                if (op.equals("Write")) {
                    write += (Integer) blkio.get("value");
                    continue;
                }
            }
            return new int[]{read, write};
        }

        public Statistics getPreStat() {
            return this.preStats;
        }
    }

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
                .exec();

        this.dockerClient.startContainerCmd(container.getId()).exec();
        return container;
    }

    public ContainerVerificationHost setUpLocalPeerHost(String id) throws Throwable {
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

        this.containerStatusMapping.put(id, ContainerStatus.RUNNING);
        this.containerIdMapping.put(publicUri, id);
        this.networkMapping.put(publicUri, clusterUri);
        return h;
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
            ContainerVerificationHost h = setUpLocalPeerHost(c.getId());
            URI clusterUri = this.networkMapping.get(h.getPublicUri());
            if (this.logContainer) {
                logContainer(h.getId(), clusterUri, 0);
            }
            if (this.statsContainer) {
                statsContainer(h.getId(), clusterUri, null);
            }
            if (containerIds != null) {
                containerIds.add(c.getId());
            }
        } catch (Throwable e) {
            throw new Exception(e);
        }
        this.completeIteration();
        return c;
    }

    private void logContainer(String id, URI clusterUri, Integer since) {
        if (this.isStopping()) {
            return;
        }
        if (isStopContainer(id)) {
            return;
        }
        schedule(() -> {
            LogContainerCallback loggingCallback = new LogContainerCallback(this, clusterUri, since);
            // use run, since timestamp
            try {
                this.dockerClient.logContainerCmd(id)
                        .withStdErr(true)
                        .withStdOut(true)
                        .withTimestamps(true)
                        .withTailAll()
                        .withSince(since)
                        .exec(loggingCallback);
            } catch (com.github.dockerjava.api.exception.ConflictException |
                    com.github.dockerjava.api.exception.NotFoundException e) {
                loggingCallback.onComplete();
                return;
            }

            try {
                loggingCallback.awaitCompletion(this.logCallBackTimeout, TimeUnit.SECONDS);
            } catch (InterruptedException e) {

            } finally {
                int latestTimestamp = loggingCallback.getLatestTimestamp();
                logContainer(id, clusterUri, latestTimestamp);
            }
        }, this.logPeriod, TimeUnit.SECONDS);
    }

    private void statsContainer(String id, URI clusterUri, Statistics preStats) {
        if (this.isStopping()) {
            return;
        }
        if (isStopContainer(id)) {
            return;
        }
        schedule(() -> {
            CountDownLatch countDownLatch = new CountDownLatch(this.statsBatch);
            StatsContainerCallback statsCallback = null;
            try {
                statsCallback = dockerClient.statsCmd(id).exec(new StatsContainerCallback(this, clusterUri, id, countDownLatch, preStats));
            } catch (com.github.dockerjava.api.exception.ConflictException |
                    com.github.dockerjava.api.exception.NotFoundException e) {
                return;
            }
            try {
                countDownLatch.await(this.statsCallBackTimeout, TimeUnit.SECONDS);

            } catch (InterruptedException e) {

            } finally {
                try {
                    statsCallback.close();
                } catch (IOException e) {

                }
                statsContainer(id, clusterUri, statsCallback.getPreStat());
            }
        }, this.statsPeriod, TimeUnit.SECONDS);
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
    public void tearDownInProcessPeers() {
        for (Map.Entry<String, ContainerStatus> entry : this.containerStatusMapping.entrySet()) {
            entry.setValue(ContainerStatus.STOPPING);
        }
        super.tearDownInProcessPeers();
        // clean paused but not resumed containers
        for (String containerId : this.containerStatusMapping.keySet()) {
            stopAndRemoveContainer(containerId);
        }
    }

    @Override
    public void stopHost(VerificationHost host) {
        URI publicUri = host.getPublicUri();
        URI clusterUri = this.networkMapping.get(publicUri);
        String containerId = this.containerIdMapping.get(publicUri);
        log("Stop host %s(%s) and remove container id %s", publicUri, clusterUri, containerId);
        stopAndRemoveContainer(containerId);
        log("Host %s(%s) %s removed", publicUri, clusterUri, containerId);
        this.networkMapping.remove(publicUri);
        this.containerIdMapping.remove(publicUri);
        this.containerStatusMapping.remove(containerId);
        super.stopHost(host);
    }

    private void stopAndRemoveContainer(String containerId) {
        this.dockerClient.stopContainerCmd(containerId).withTimeout(this.stopContainerTimeout).exec();
        this.dockerClient.removeContainerCmd(containerId).withForce(true).exec();
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

        this.containerStatusMapping.put(id, ContainerStatus.RUNNING);
        this.containerIdMapping.put(publicUri, id);
        this.networkMapping.put(publicUri, clusterUri);
        return;
    }

    @Override
    public void stopHostAndPreserveState(ServiceHost host) {
        URI publicUri = host.getPublicUri();
        URI clusterUri = this.networkMapping.get(publicUri);
        String containerId = this.containerIdMapping.get(publicUri);
        log("Stopping host %s(%s) and pausing container id %s", publicUri, clusterUri, containerId);
        this.containerStatusMapping.put(containerId, ContainerStatus.PAUSING);
        this.dockerClient.pauseContainerCmd(containerId).exec();
        this.networkMapping.remove(publicUri);
        this.containerIdMapping.remove(publicUri);
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
        if (this.containerStatusMapping.keySet().contains(id)) {
            return true;
        }
        for (String peerId : this.containerStatusMapping.keySet()) {
            if (peerId.startsWith(id)) {
                return true;
            }
        }
        return false;
    }

    public boolean isStopContainer(String id) {
        ContainerStatus status = this.containerStatusMapping.get(id);
        if (status == null) {
            return true;
        }
        if (status == ContainerStatus.STOPPING) {
            return true;
        }
        return false;
    }
}
