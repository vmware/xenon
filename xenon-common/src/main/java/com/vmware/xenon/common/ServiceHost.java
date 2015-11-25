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

package com.vmware.xenon.common;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.vmware.xenon.common.FileUtils.ResourceEntry;
import com.vmware.xenon.common.NodeSelectorService.SelectAndForwardRequest;
import com.vmware.xenon.common.NodeSelectorService.SelectAndForwardRequest.ForwardingOption;
import com.vmware.xenon.common.NodeSelectorService.SelectOwnerResponse;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ProcessingStage;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceDocumentDescription.Builder;
import com.vmware.xenon.common.ServiceErrorResponse.ErrorDetail;
import com.vmware.xenon.common.ServiceHost.ServiceHostState.MemoryLimitType;
import com.vmware.xenon.common.ServiceHost.ServiceHostState.SslClientAuthMode;
import com.vmware.xenon.common.ServiceMaintenanceRequest.MaintenanceReason;
import com.vmware.xenon.common.ServiceSubscriptionState.ServiceSubscriber;
import com.vmware.xenon.common.http.netty.NettyHttpListener;
import com.vmware.xenon.common.http.netty.NettyHttpServiceClient;
import com.vmware.xenon.common.jwt.Signer;
import com.vmware.xenon.common.jwt.Verifier;
import com.vmware.xenon.common.jwt.Verifier.TokenException;
import com.vmware.xenon.services.common.AuthCredentialsFactoryService;
import com.vmware.xenon.services.common.AuthorizationContextService;
import com.vmware.xenon.services.common.ConsistentHashingNodeSelectorService;
import com.vmware.xenon.services.common.FileContentService;
import com.vmware.xenon.services.common.GuestUserService;
import com.vmware.xenon.services.common.LuceneBlobIndexService;
import com.vmware.xenon.services.common.LuceneDocumentIndexService;
import com.vmware.xenon.services.common.LuceneLocalQueryTaskFactoryService;
import com.vmware.xenon.services.common.LuceneQueryTaskFactoryService;
import com.vmware.xenon.services.common.NodeGroupFactoryService;
import com.vmware.xenon.services.common.NodeGroupService.JoinPeerRequest;
import com.vmware.xenon.services.common.NodeSelectorSynchronizationService.SynchronizePeersRequest;
import com.vmware.xenon.services.common.ODataQueryService;
import com.vmware.xenon.services.common.OperationIndexService;
import com.vmware.xenon.services.common.ProcessFactoryService;
import com.vmware.xenon.services.common.QueryFilter;
import com.vmware.xenon.services.common.ReliableSubscriptionService;
import com.vmware.xenon.services.common.ResourceGroupFactoryService;
import com.vmware.xenon.services.common.RoleFactoryService;
import com.vmware.xenon.services.common.ServiceContextIndexService;
import com.vmware.xenon.services.common.ServiceHostLogService;
import com.vmware.xenon.services.common.ServiceHostManagementService;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.SystemUserService;
import com.vmware.xenon.services.common.TenantFactoryService;
import com.vmware.xenon.services.common.TransactionFactoryService;
import com.vmware.xenon.services.common.UpdateIndexRequest;
import com.vmware.xenon.services.common.UserFactoryService;
import com.vmware.xenon.services.common.UserGroupFactoryService;
import com.vmware.xenon.services.common.authn.AuthenticationConstants;
import com.vmware.xenon.services.common.authn.BasicAuthenticationService;

/**
 * Service host manages service life cycle, delivery of operations (remote and local) and performing
 * periodic maintenance on all services.
 *
 * Service host allows the process to specify at runtime key infrastructure services such as authz
 * and document storage / indexing.
 *
 * The HTTP service host listens on HTTP URIs but shares common functionality with hosts on other
 * protocols
 */
public class ServiceHost {
    public static final String UI_DIRECTORY_NAME = "ui";

    public static class ServiceAlreadyStartedException extends IllegalStateException {
        private static final long serialVersionUID = -1444810129515584386L;

        /**
         * Constructs an instance of this class.
         */
        public ServiceAlreadyStartedException(String servicePath) {
            super(servicePath);
        }
    }

    public static class ServiceNotFoundException extends IllegalStateException {
        private static final long serialVersionUID = 663670123267539178L;
    }

    public static class Arguments {
        /**
         * Command line argument
         */
        public int port = DEFAULT_PORT;

        /**
         * Command line argument
         */
        public int securePort;

        /**
         * Command line argument
         */
        public SslClientAuthMode sslClientAuthMode = SslClientAuthMode.NONE;

        /**
         * Command line argument
         */
        public Path keyFile;

        /**
         * Command line argument
         */
        public Path certificateFile;

        /**
         * Command line argument
         */
        public Path sandbox = DEFAULT_SANDBOX;

        /**
         * Command line argument
         */
        public String bindAddress = DEFAULT_BIND_ADDRESS;

        /**
         * Command line argument. Optional public URI the host uses to advertise itself to peers. If its
         * not set, the bind address and port will be used to form the host URI
         */
        public String publicUri;

        /**
         * Command line argument. Comma separated list of one or more peer nodes to join through Nodes
         * must be defined in URI form, e.g --peerNodes=http://192.168.1.59:8000,http://192.168.1.82
         */
        public String[] peerNodes;

        /**
         * Command line argument. A stable identity associated with this host
         */
        public String id;

        /**
         * Command line argument. Value indicating whether node group changes will automatically
         * trigger replicated service state synchronization. If set to false, client can issue
         * synchronization requests through core management service
         */
        public boolean isPeerSynchronizationEnabled = true;

        /**
         * Command line argument. Mandate an auth context for all requests
         * This option will be set to true and authn/authz enabled by default after a transition period
         */
        public boolean isAuthorizationEnabled = false;
    }

    private static final LogFormatter LOG_FORMATTER = new LogFormatter();
    private static final LogFormatter COLOR_LOG_FORMATTER = new ColorLogFormatter();

    public static final String SERVICE_HOST_STATE_FILE = "serviceHostState.json";

    public static final Double DEFAULT_PCT_MEMORY_LIMIT = 0.5;
    public static final Double DEFAULT_PCT_MEMORY_LIMIT_DOCUMENT_INDEX = 0.3;
    public static final Double DEFAULT_PCT_MEMORY_LIMIT_BLOB_INDEX = 0.01;

    public static final String LOCAL_HOST = "127.0.0.1";
    public static final String LOOPBACK_ADDRESS = "127.0.0.1";
    public static final String DEFAULT_BIND_ADDRESS = ServiceHost.LOCAL_HOST;
    public static final int DEFAULT_PORT = 8000;
    public static final String ALL_INTERFACES = "0.0.0.0";

    public static final String ROOT_PATH = "";

    public static final String SERVICE_URI_SUFFIX_STATS = "/stats";
    public static final String SERVICE_URI_SUFFIX_SUBSCRIPTIONS = "/subscriptions";

    public static final String SERVICE_URI_SUFFIX_CONFIG = "/config";
    public static final String SERVICE_URI_SUFFIX_TEMPLATE = "/template";
    public static final String SERVICE_URI_SUFFIX_UI = "/ui";

    public static final String SERVICE_URI_SUFFIX_REPLICATION = "/replication";

    public static final String DCP_ENVIRONMENT_VAR_PREFIX = "XENON_";
    public static final String GIT_COMMIT_PROPERTIES_RESOURCE_NAME = "xenon.git.properties";
    public static final String GIT_COMMIT_SOURCE_PROPERTY_PREFIX = "git.commit";
    public static final String GIT_COMMIT_SOURCE_PROPERTY_COMMIT_ID = GIT_COMMIT_SOURCE_PROPERTY_PREFIX
            + ".id";
    public static final String GIT_COMMIT_SOURCE_PROPERTY_COMMIT_TIME = GIT_COMMIT_SOURCE_PROPERTY_PREFIX
            + ".time";

    public static final String[] RESERVED_SERVICE_URI_PATHS = {
            SERVICE_URI_SUFFIX_REPLICATION,
            SERVICE_URI_SUFFIX_STATS, SERVICE_URI_SUFFIX_SUBSCRIPTIONS,
            SERVICE_URI_SUFFIX_UI,
            SERVICE_URI_SUFFIX_CONFIG, SERVICE_URI_SUFFIX_TEMPLATE };

    static final Path DEFAULT_TMPDIR = Paths.get(System.getProperty("java.io.tmpdir"));
    static final Path DEFAULT_SANDBOX = DEFAULT_TMPDIR.resolve("xenon");
    static final Path DEFAULT_RESOURCE_SANDBOX_DIR = Paths.get("resources");

    /**
     * Estimate for average service state memory cost, in bytes. This can be computed per
     * state cached, estimated per kind, or made tunable in the future. Its used solely for estimating
     * host memory consumption during maintenance
     */
    public static final int DEFAULT_SERVICE_STATE_COST_BYTES = 4096;

    /**
     * Estimate for service class runtime context cost, in bytes. It takes into account:
     *
     * 1) The cost of the self link of each service instance
     * 2) The cost of the map nodes used to store the self link
     * 3) The cost of the runtime context structure, per {@code StatefulService} instance
     * 4) Estimated cost of default statistics, if service is instrumented
     * 5) Estimated cost of a small number of subscriptions
     */
    public static final int DEFAULT_SERVICE_INSTANCE_COST_BYTES = Service.MAX_SERIALIZED_SIZE_BYTES
            / 2;
    private static final long ONE_MINUTE_IN_MICROS = TimeUnit.MINUTES.toMicros(1);

    public static class RequestRateInfo {
        /**
         * Request limit (upper bound) in requests per second
         */
        public double limit;

        /**
         * Number of requests since most recent time window
         */
        public AtomicInteger count = new AtomicInteger();

        /**
         * Start time in microseconds since epoch for the timing window
         */
        public long startTimeMicros;
    }

    public static class ServiceHostState extends ServiceDocument {
        public static enum MemoryLimitType {
            LOW_WATERMARK, HIGH_WATERMARK, EXACT
        }

        public static enum SslClientAuthMode {
            NONE, WANT, NEED
        }

        public static final long DEFAULT_MAINTENANCE_INTERVAL_MICROS = TimeUnit.SECONDS
                .toMicros(1);
        public static final long DEFAULT_OPERATION_TIMEOUT_MICROS = TimeUnit.SECONDS.toMicros(60);
        public String bindAddress;
        public int httpPort;
        public int httpsPort;
        public URI publicUri;
        public long maintenanceIntervalMicros = DEFAULT_MAINTENANCE_INTERVAL_MICROS;
        public long operationTimeoutMicros = DEFAULT_OPERATION_TIMEOUT_MICROS;
        public String operationTracingLevel;
        public SslClientAuthMode sslClientAuthMode;

        public URI storageSandboxFileReference;
        public URI privateKeyFileReference;
        public URI certificateFileReference;

        public URI documentIndexReference;
        public URI authorizationServiceReference;
        public URI transactionServiceReference;
        public String id;
        public boolean isPeerSynchronizationEnabled;
        public boolean isAuthorizationEnabled;

        public transient boolean isStarted;
        public transient boolean isStopping;
        public SystemHostInfo systemInfo;
        public long lastMaintenanceTimeUtcMicros;
        public boolean isProcessOwner;
        public boolean isServiceStateCaching = true;
        public Properties codeProperties;
        public long serviceCount;

        /**
         * Relative memory limit per service path. The limit is expressed as
         * percentage (range of [0.0,1.0]) of max memory available to the java virtual machine
         *
         * The empty path, "", is reserved for the host memory limit
         */
        public Map<String, Double> relativeMemoryLimits = new ConcurrentSkipListMap<>();

        /**
         * Request limits, in operations per second. Each limit is associated with a key,
         * derived from some context (user, tenant, context id). An operation is associated with
         * a key and then service host tracks and applies the limit for each in bound request that
         * belongs to the same context.
         *
         * Rate limiting is a global back pressure mechanism that is independent of the target
         * service and any additional throttling applied during service request
         * processing
         */
        public Map<String, RequestRateInfo> requestRateLimits = new ConcurrentSkipListMap<>();

        /**
         * Infrastructure use only.
         *
         * Set of links that should be excluded from operation tracing
         */
        private transient TreeSet<String> operationTracingLinkExclusionList = new TreeSet<>(
                Arrays.asList(new String[] {
                        ServiceUriPaths.NODE_GROUP_FACTORY,
                        ServiceUriPaths.UI_SERVICE_CORE_PATH,
                        ServiceUriPaths.DEFAULT_NODE_GROUP,
                        ServiceUriPaths.DEFAULT_NODE_SELECTOR,
                        ServiceUriPaths.CORE_DOCUMENT_INDEX,
                        ServiceUriPaths.CORE_OPERATION_INDEX,
                        ServiceUriPaths.CORE_QUERY_TASKS }));
        public String[] initialPeerNodes;
    }

    private static ConcurrentSkipListSet<Operation> createOperationSet() {
        return new ConcurrentSkipListSet<>(new Comparator<Operation>() {
            @Override
            public int compare(Operation o1, Operation o2) {
                return Long.compare(o1.getExpirationMicrosUtc(),
                        o2.getExpirationMicrosUtc());
            }
        });
    }

    public static int findListenPort() {
        int port = 0;
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            port = socket.getLocalPort();
            Logger.getAnonymousLogger().info("port candidate:" + port);
        } catch (Throwable e) {
            Logger.getAnonymousLogger().severe(e.toString());
        } finally {
            try {
                if (socket != null) {
                    socket.close();
                }
            } catch (IOException e) {
            }
        }
        return port;
    }

    private Logger logger = Logger.getLogger(getClass().getName());
    private FileHandler handler;

    private final Map<String, ServiceDocumentDescription> descriptionCache = new HashMap<>();
    private final ServiceDocumentDescription.Builder descriptionBuilder = Builder.create();

    private ExecutorService executor;
    protected ScheduledExecutorService scheduledExecutor;

    private final ConcurrentSkipListMap<String, Service> attachedServices = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListSet<String> coreServices = new ConcurrentSkipListSet<>();

    private final ConcurrentSkipListSet<String> pendingNodeSelectorsForFactorySynch = new ConcurrentSkipListSet<>();
    private final SortedSet<Operation> pendingStartOperations = createOperationSet();
    private final Map<String, SortedSet<Operation>> pendingServiceAvailableCompletions = new ConcurrentSkipListMap<>();
    private final SortedSet<Operation> pendingOperationsForRetry = createOperationSet();

    private ServiceHostState state;
    private Service documentIndexService;
    private Service authorizationService;
    private Service transactionService;
    private SystemHostInfo info = new SystemHostInfo();
    private ServiceClient client;

    private ServiceRequestListener httpListener;
    private ServiceRequestListener httpsListener;

    private URI documentIndexServiceUri;
    private URI authorizationServiceUri;
    private URI transactionServiceUri;
    private ScheduledFuture<?> maintenanceTask;

    private final ConcurrentSkipListMap<String, ServiceDocument> cachedServiceStates = new ConcurrentSkipListMap<>();

    private ConcurrentSkipListSet<String> serviceFactoriesUnderMemoryPressure = new ConcurrentSkipListSet<>();
    private ConcurrentSkipListMap<String, Service> pendingPauseServices = new ConcurrentSkipListMap<>();

    private final ServiceHostMaintenanceTracker maintenanceHelper = ServiceHostMaintenanceTracker
            .create(this);

    private String logPrefix;
    private URI cachedUri;

    private Signer tokenSigner;
    private Verifier tokenVerifier;

    private AuthorizationContext systemAuthorizationContext;
    private AuthorizationContext guestAuthorizationContext;
    private ConcurrentSkipListMap<String, Class<? extends Service>> privilegedServiceList = new ConcurrentSkipListMap<>();

    protected ServiceHost() {
        this.state = new ServiceHostState();
        this.state.id = UUID.randomUUID().toString();
    }

    public ServiceHost initialize(String[] args) throws Throwable {
        Arguments hostArgs = new Arguments();
        CommandLineArgumentParser.parse(hostArgs, args);
        CommandLineArgumentParser.parse(COLOR_LOG_FORMATTER, args);
        initialize(hostArgs);
        setProcessOwner(true);
        return this;
    }

    public ServiceHost initialize(Arguments args) throws Throwable {
        setSystemProperties();

        Path sandbox = args.sandbox.resolve(Integer.toString(args.port));
        URI storageSandbox = sandbox.toFile().toURI();

        if (!Files.exists(sandbox)) {
            Files.createDirectories(sandbox);
        }

        if (args.port < 0) {
            throw new IllegalArgumentException(
                    "port: negative values not allowed");
        }

        if (this.state == null) {
            throw new IllegalStateException();
        }

        File s = null;
        if (storageSandbox == null) {
            s = new File(Utils.getCurrentFileDirectory());
            storageSandbox = s.toURI();
        } else {
            s = new File(storageSandbox);
        }

        if (false == s.exists()) {
            throw new IllegalArgumentException("storageSandbox directory does not exist: "
                    + storageSandbox);
        }

        // load configuration from disk
        this.state.storageSandboxFileReference = storageSandbox;
        loadState(storageSandbox, s);

        // apply command line arguments, potentially overriding file configuration
        initializeStateFromArguments(s, args);

        LuceneDocumentIndexService documentIndexService = new LuceneDocumentIndexService();
        setDocumentIndexingService(documentIndexService);

        this.state.codeProperties = FileUtils.readPropertiesFromResource(this.getClass(),
                GIT_COMMIT_PROPERTIES_RESOURCE_NAME);

        updateSystemInfo(false);

        // Create token signer and verifier
        this.tokenSigner = new Signer(
                AuthenticationConstants.JWT_SECRET.getBytes(Utils.CHARSET));
        this.tokenVerifier = new Verifier(
                AuthenticationConstants.JWT_SECRET.getBytes(Utils.CHARSET));

        // Set default limits for memory utilization on core services and the host
        setServiceMemoryLimit(ROOT_PATH, DEFAULT_PCT_MEMORY_LIMIT);
        setServiceMemoryLimit(ServiceUriPaths.CORE_DOCUMENT_INDEX,
                DEFAULT_PCT_MEMORY_LIMIT_DOCUMENT_INDEX);
        setServiceMemoryLimit(ServiceUriPaths.CORE_BLOB_INDEX,
                DEFAULT_PCT_MEMORY_LIMIT_BLOB_INDEX);
        setServiceMemoryLimit(ServiceUriPaths.CORE_SERVICE_CONTEXT_INDEX,
                DEFAULT_PCT_MEMORY_LIMIT_BLOB_INDEX);
        return this;
    }

    private void initializeStateFromArguments(File s, Arguments args) throws URISyntaxException {
        this.state.httpPort = args.port;
        this.state.httpsPort = args.securePort;
        this.state.sslClientAuthMode = args.sslClientAuthMode;

        if (args.keyFile != null) {
            this.state.privateKeyFileReference = args.keyFile.toUri();
        }

        if (args.certificateFile != null) {
            this.state.certificateFileReference = args.certificateFile.toUri();
        }

        if (args.id != null) {
            this.state.id = args.id;
        }

        this.state.isPeerSynchronizationEnabled = args.isPeerSynchronizationEnabled;

        this.state.isAuthorizationEnabled = args.isAuthorizationEnabled;

        File hostStateFile = new File(s, SERVICE_HOST_STATE_FILE);
        String errorFmt = hostStateFile.getPath()
                + " conflicts with command line argument %s. Argument: %s, in file: %s";
        String argumentName = "bindAddress";
        if (args.bindAddress != null && this.state.bindAddress != null
                && !args.bindAddress.equals(this.state.bindAddress)) {
            log(Level.WARNING, errorFmt, argumentName, args.bindAddress,
                    this.state.bindAddress);
        }

        setBindAddress(args.bindAddress);
        if (args.publicUri != null) {
            setPublicUri(new URI(args.publicUri));
        }

        this.state.initialPeerNodes = args.peerNodes;
    }

    private void configureLogging(File storageSandboxDir) throws IOException {
        String logConfigFile = System.getProperty("java.util.logging.config.file");
        String logConfigClass = System.getProperty("java.util.logging.config.class");
        if (logConfigFile == null && logConfigClass == null) {
            File logFile = new File(storageSandboxDir, this.getClass().getSimpleName() + "."
                    + getPort() + ".%g.log");
            this.handler = new FileHandler(logFile.getAbsolutePath(), 1024 * 1024 * 10, 1);
            this.handler.setFormatter(LOG_FORMATTER);
            this.logger.getParent().addHandler(this.handler);

            String path = logFile.toString().replace("%g", "0");
            ServiceHostLogService.setProcessLogFile(path);
        }

        for (java.util.logging.Handler h : this.logger.getParent().getHandlers()) {
            if (h instanceof ConsoleHandler) {
                h.setFormatter(COLOR_LOG_FORMATTER);
            } else {
                h.setFormatter(LOG_FORMATTER);
            }
        }
        this.logPrefix = getClass().getSimpleName() + ":" + getPort();
    }

    private void removeLogging() {
        if (this.handler != null) {
            this.logger.getParent().removeHandler(this.handler);
            this.handler.close();
            this.handler = null;
        }
    }

    private void loadState(URI storageSandbox, File s) throws IOException, InterruptedException {
        File hostStateFile = new File(s, SERVICE_HOST_STATE_FILE);
        if (hostStateFile.exists()) {
            CountDownLatch l = new CountDownLatch(1);
            FileUtils.readFileAndComplete(
                    Operation.createGet(null).setCompletion(
                            (o, e) -> {
                                if (e != null) {
                                    log(Level.WARNING, "Failure loading state from %s: %s",
                                            hostStateFile, Utils.toString(e));
                                    l.countDown();
                                    return;
                                }

                                try {
                                    ServiceHostState fileState = o.getBody(ServiceHostState.class);
                                    if (fileState.id == null) {
                                        log(Level.WARNING, "Invalid state from %s: %s",
                                                hostStateFile,
                                                Utils.toJsonHtml(fileState));
                                        l.countDown();
                                        return;
                                    }
                                    fileState.isStarted = this.state.isStarted;
                                    fileState.isStopping = this.state.isStopping;
                                    this.state = fileState;
                                    l.countDown();
                                } catch (Throwable ex) {
                                    log(Level.WARNING, "Invalid state from %s: %s", hostStateFile,
                                            Utils.toJsonHtml(o.getBodyRaw()));
                                    l.countDown();
                                    return;
                                }
                            }),
                    hostStateFile);
            l.await();
        }
    }

    private void saveState() throws IOException, InterruptedException {
        saveState(new File(this.state.storageSandboxFileReference));
    }

    private void saveState(File sandboxDir) throws IOException, InterruptedException {
        File hostStateFile = new File(sandboxDir, SERVICE_HOST_STATE_FILE);
        this.state.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        byte[] serializedState = Utils.toJsonHtml(this.state).getBytes(Utils.CHARSET);
        Files.write(hostStateFile.toPath(), serializedState, StandardOpenOption.CREATE);
    }

    @Override
    public String toString() {
        return String.format("["
                + "%n isStarted: %s"
                + "%n httpPort: %d"
                + "%n httpsPort: %d"
                + "%n id: %s"
                + "%n attached services: %d"
                + "%n]",
                isStarted(),
                this.state.httpPort,
                this.state.httpsPort,
                this.state.id,
                this.attachedServices.size());
    }

    public boolean isStarted() {
        return this.state.isStarted;
    }

    public boolean isStopping() {
        return this.state.isStopping;
    }

    public boolean isServiceStateCaching() {
        return this.state.isServiceStateCaching;
    }

    public ServiceHost setServiceStateCaching(boolean enable) {
        this.state.isServiceStateCaching = enable;
        return this;
    }

    public int getPort() {
        return this.state.httpPort;
    }

    public ServiceHost setPort(int port) {
        if (isStarted()) {
            throw new IllegalStateException("Already started");
        }
        this.state.httpPort = port;
        return this;
    }

    public boolean isAuthorizationEnabled() {
        return this.state.isAuthorizationEnabled;
    }

    public void setAuthorizationEnabled(boolean isAuthorizationEnabled) {
        if (isStarted()) {
            throw new IllegalStateException("Already started");
        }
        this.state.isAuthorizationEnabled = isAuthorizationEnabled;
    }

    public boolean isPeerSynchronizationEnabled() {
        return this.state.isPeerSynchronizationEnabled;
    }

    public void setPeerSynchronizationEnabled(boolean enabled) {
        this.state.isPeerSynchronizationEnabled = enabled;
    }

    public int getSecurePort() {
        return this.state.httpsPort;
    }

    public ServiceHost setSecurePort(int port) {
        if (isStarted()) {
            throw new IllegalStateException("Already started");
        }
        this.state.httpsPort = port;
        return this;
    }

    public ServiceHost setPrivateKeyFileReference(URI fileReference) {
        this.state.privateKeyFileReference = fileReference;
        return this;
    }

    public ServiceHost setCertificateFileReference(URI fileReference) {
        this.state.certificateFileReference = fileReference;
        return this;
    }

    public ServiceHost setBindAddress(String address) {
        if (isStarted()) {
            throw new IllegalStateException("Already started");
        }
        if (address == null) {
            throw new IllegalArgumentException("address is required");
        }

        this.state.bindAddress = address;
        if (this.info.ipAddresses.isEmpty() || !this.info.ipAddresses.get(0).equals(address)) {
            // regenerate address list
            this.info.ipAddresses.clear();
            getSystemInfo();
        }

        this.cachedUri = null;

        return this;
    }

    /**
     * Sets the public URI (host name and port) the host will use to advertise itself externally.
     * The public URI is optional and should be set only when the bind address is not available
     * to external peers (due to NAT configuration, bridged networking etc).
     *
     * If the public URI is not explicitly set, the bind address and port will be used for the host's public URI
     */
    public ServiceHost setPublicUri(URI publicUri) {
        this.state.publicUri = publicUri;
        this.cachedUri = null;
        return this;
    }

    public URI getStorageSandbox() {
        return this.state.storageSandboxFileReference;
    }

    public ServiceHost setMaintenanceIntervalMicros(long micros) {
        if (micros <= 0) {
            throw new IllegalArgumentException(
                    "micros: zero or negative value not allowed");
        }

        // verify that attached services have intervals greater or equal to suggested value
        for (Service s : this.attachedServices.values()) {
            if (s.getProcessingStage() == ProcessingStage.STOPPED) {
                continue;
            }
            if (s.getMaintenanceIntervalMicros() == 0) {
                continue;
            }
            if (s.getMaintenanceIntervalMicros() < micros) {
                String error = String.format(
                        "Service %s has a small maintenance interval %d than new interval %d",
                        s.getSelfLink(), s.getMaintenanceIntervalMicros(), micros);
                log(Level.WARNING, error);
            }
        }

        this.state.maintenanceIntervalMicros = micros;

        // we need to cancel the current task and re-schedule and the new
        // interval
        ScheduledFuture<?> task = this.maintenanceTask;
        if (task == null) {
            return this;
        }
        task.cancel(true);
        scheduleMaintenance();

        return this;
    }

    public String getId() {
        return this.state.id;
    }

    public long getOperationTimeoutMicros() {
        return this.state.operationTimeoutMicros;
    }

    public ServiceHostState getState() {
        ServiceHostState s = Utils.clone(this.state);
        s.systemInfo = getSystemInfo();
        return s;
    }

    public URI getDocumentIndexServiceUri() {
        if (this.documentIndexService == null) {
            return null;
        }
        if (this.documentIndexServiceUri != null) {
            return this.documentIndexServiceUri;
        }
        this.documentIndexServiceUri = this.documentIndexService.getUri();
        return this.documentIndexServiceUri;
    }

    public URI getAuthorizationServiceUri() {
        if (this.authorizationService == null) {
            return null;
        }
        if (this.authorizationServiceUri != null) {
            return this.authorizationServiceUri;
        }
        this.authorizationServiceUri = this.authorizationService.getUri();
        return this.authorizationServiceUri;
    }

    public URI getTransactionServiceUri() {
        if (this.transactionService == null) {
            return null;
        }
        if (this.transactionServiceUri != null) {
            return this.transactionServiceUri;
        }
        this.transactionServiceUri = this.transactionService.getUri();
        return this.transactionServiceUri;
    }

    public ServiceHost setDocumentIndexingService(Service service) {
        if (this.state.isStarted) {
            throw new IllegalStateException("Host is started");
        }
        this.documentIndexService = service;
        return this;
    }

    public ServiceHost setAuthorizationService(Service service) {
        if (this.state.isStarted) {
            throw new IllegalStateException("Host is started");
        }
        this.authorizationService = service;
        return this;
    }

    public ServiceHost setTransactionService(Service service) {
        this.transactionService = service;
        return this;
    }

    ScheduledExecutorService getScheduledExecutor() {
        return this.scheduledExecutor;
    }

    ExecutorService getExecutor() {
        return this.executor;
    }

    public ExecutorService allocateExecutor(Service s) {
        return allocateExecutor(s, Utils.DEFAULT_THREAD_COUNT);
    }

    public ExecutorService allocateExecutor(Service s, int threadCount) {
        return Executors.newFixedThreadPool(threadCount, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, s.getUri() + "/" + Utils.getNowMicrosUtc());
            }
        });
    }

    public ServiceHost start() throws Throwable {
        return startImpl();
    }

    private void setSystemProperties() throws Throwable {
        Properties props = System.getProperties();

        // Prefer IPv4 by default.
        // Note that this property must be set before java.net's JNI_OnLoad
        // is called, otherwise setting this property has no effect.
        final String preferIPv4 = "java.net.preferIPv4Stack";
        if (props.getProperty(preferIPv4) == null) {
            props.setProperty(preferIPv4, "true");
        }
    }

    private ServiceHost startImpl() throws Throwable {

        synchronized (this.state) {
            if (isStarted()) {
                return this;
            }
            this.state.isStarted = true;
            this.state.isStopping = false;
        }

        if (this.isAuthorizationEnabled() && this.authorizationService == null) {
            this.authorizationService = new AuthorizationContextService();
        }

        this.executor = Executors.newWorkStealingPool(Utils.DEFAULT_THREAD_COUNT);
        this.scheduledExecutor = Executors.newScheduledThreadPool(Utils.DEFAULT_THREAD_COUNT,
                r -> new Thread(r, getUri().toString() + "/scheduled/" + this.state.id));

        if (this.httpListener == null) {
            this.httpListener = new NettyHttpListener(this);
        }

        this.httpListener.start(getPort(), this.state.bindAddress);

        if ((this.state.certificateFileReference != null
                || this.state.privateKeyFileReference != null)
                && this.httpsListener == null) {
            this.httpsListener = new NettyHttpListener(this);
        }

        if (this.httpsListener != null) {
            if (!this.httpsListener.isSSLConfigured()) {
                this.httpsListener.setSSLContextFiles(this.state.certificateFileReference,
                        this.state.privateKeyFileReference);
            }
            this.httpsListener.start(getSecurePort(), this.state.bindAddress);
        }

        // Update the state JSON file if the port was chosen by the httpListener.
        // An external process can then get the port from the state file.
        if (this.state.httpPort == 0) {
            this.state.httpPort = this.httpListener.getPort();
        }

        if (this.state.httpsPort == 0 && this.httpsListener != null) {
            this.state.httpsPort = this.httpsListener.getPort();
        }

        saveState();

        this.documentIndexServiceUri = UriUtils.updateUriPort(this.documentIndexServiceUri,
                this.state.httpPort);
        this.authorizationServiceUri = UriUtils.updateUriPort(this.authorizationServiceUri,
                this.state.httpPort);
        this.transactionServiceUri = UriUtils.updateUriPort(this.transactionServiceUri,
                this.state.httpPort);

        configureLogging(new File(getStorageSandbox()));

        // Use the class name and prefix of GIT commit ID as the user agent name and version
        String commitID = (String) this.state.codeProperties
                .get(GIT_COMMIT_SOURCE_PROPERTY_COMMIT_ID);
        if (commitID == null) {
            throw new IllegalStateException("CommitID code property not found!");
        }
        commitID = commitID.substring(0, 8);
        String userAgent = ServiceHost.class.getSimpleName() + "/" + commitID;

        if (this.client == null) {
            this.client = NettyHttpServiceClient.create(userAgent, this.executor,
                    this.scheduledExecutor,
                    this);
            SSLContext clientContext = SSLContext.getInstance(ServiceClient.TLS_PROTOCOL_NAME);
            TrustManagerFactory trustManagerFactory = TrustManagerFactory
                    .getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init((KeyStore) null);
            clientContext.init(null, trustManagerFactory.getTrustManagers(), null);
            this.client.setSSLContext(clientContext);
        }

        // Start client as system user; it starts a callback service
        AuthorizationContext ctx = OperationContext.getAuthorizationContext();
        OperationContext.setAuthorizationContext(getSystemAuthorizationContext());
        this.client.start();
        OperationContext.setAuthorizationContext(ctx);

        scheduleMaintenance();

        log(Level.INFO, "%s listening on %s:%d", userAgent, getPreferredAddress(), getPort());

        this.cachedUri = null;
        return this;
    }

    /**
     * Starts core singleton services. Should be called once from the service host entry point.
     */
    public void startDefaultCoreServicesSynchronously() throws Throwable {
        if (findService(ServiceHostManagementService.SELF_LINK) != null) {
            throw new IllegalStateException("Already started");
        }

        addPrivilegedService(ServiceHostManagementService.class);
        addPrivilegedService(OperationIndexService.class);
        addPrivilegedService(LuceneBlobIndexService.class);
        addPrivilegedService(BasicAuthenticationService.class);

        // Capture authorization context; this function executes as the system user
        AuthorizationContext ctx = OperationContext.getAuthorizationContext();
        OperationContext.setAuthorizationContext(getSystemAuthorizationContext());

        // Start authorization service first since it sits in the dispatch path
        if (this.authorizationService != null) {
            addPrivilegedService(this.authorizationService.getClass());
            startCoreServicesSynchronously(this.authorizationService);
        }

        // Normalize peer list and find our external address
        // This must be done BEFORE node group starts.
        List<URI> peers = getInitialPeerHosts();

        startDefaultReplicationAndNodeGroupServices();

        List<Service> coreServices = new ArrayList<>();
        coreServices.add(new ServiceHostManagementService());
        coreServices.add(new ProcessFactoryService());
        coreServices.add(new ServiceContextIndexService());
        coreServices.add(new LuceneBlobIndexService());
        coreServices.add(new ODataQueryService());

        // The framework supports two phase asynchronous start to avoid explicit
        // ordering of services. However, core services must be started before anyone else
        if (this.documentIndexService != null) {
            addPrivilegedService(this.documentIndexService.getClass());
            coreServices.add(this.documentIndexService);
            if (this.documentIndexService instanceof LuceneDocumentIndexService) {
                coreServices.add(new LuceneQueryTaskFactoryService());
                coreServices.add(new LuceneLocalQueryTaskFactoryService());
            }
        }

        // Start persisted factories here, after document index is added
        coreServices.add(new AuthCredentialsFactoryService());
        coreServices.add(new UserGroupFactoryService());
        coreServices.add(new ResourceGroupFactoryService());
        coreServices.add(new RoleFactoryService());
        coreServices.add(new UserFactoryService());
        coreServices.add(new SystemUserService());
        coreServices.add(new GuestUserService());
        coreServices.add(new TenantFactoryService());
        coreServices.add(new BasicAuthenticationService());

        Service transactionFactoryService = new TransactionFactoryService();
        coreServices.add(transactionFactoryService);

        Service[] coreServiceArray = new Service[coreServices.size()];
        coreServices.toArray(coreServiceArray);
        startCoreServicesSynchronously(coreServiceArray);
        this.transactionService = transactionFactoryService;

        // start the log services in parallel and asynchronously
        startService(
                Operation.createPost(UriUtils.buildUri(this, ServiceUriPaths.PROCESS_LOG)),
                new ServiceHostLogService(ServiceHostLogService.getDefaultProcessLogName()));

        startService(
                Operation.createPost(UriUtils.buildUri(this, ServiceUriPaths.GO_PROCESS_LOG)),
                new ServiceHostLogService(ServiceHostLogService.getDefaultGoDcpProcessLogName()));

        startService(
                Operation.createPost(UriUtils.buildUri(this, ServiceUriPaths.SYSTEM_LOG)),
                new ServiceHostLogService(ServiceHostLogService.DEFAULT_SYSTEM_LOG_NAME));

        // Create service without starting it.
        // Needed to start the UI resource service associated with the WebSocketService.
        Service webSocketService = new WebSocketService(null, null);
        webSocketService.setHost(this);
        startUiFileContentServices(webSocketService);

        // Restore authorization context
        OperationContext.setAuthorizationContext(ctx);

        schedule(() -> {
            joinPeers(peers, ServiceUriPaths.DEFAULT_NODE_GROUP);
        }, this.state.maintenanceIntervalMicros, TimeUnit.MICROSECONDS);
    }

    public List<URI> getInitialPeerHosts() {
        return normalizePeerNodeList(this.state.initialPeerNodes);
    }

    /**
     * Infrastructure use. Copies the specified file URL to the resource file path
     */
    public Path copyResourceToSandbox(URL url, Path resourcePath) throws URISyntaxException {
        File sandbox = new File(getStorageSandbox());
        Path outputPath = sandbox.toPath().resolve(DEFAULT_RESOURCE_SANDBOX_DIR)
                .resolve(resourcePath);

        // Return reference to file if possible.
        // This is not possible when the resource is embedded in a JAR.
        if (url.getProtocol().equals("file")) {
            log(Level.FINE, "Using resource %s", url.getPath());
            URI uri = url.toURI();
            return Paths.get(uri);
        }

        try {
            log(Level.FINE, "Copying resource %s to %s", url, outputPath);
            Path parent = outputPath.getParent();
            if (parent == null) {
                throw new IOException("No parent for output path: " + outputPath);
            }
            Files.createDirectories(parent);
            InputStream is = url.openStream();
            Files.copy(is, outputPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            log(Level.WARNING, "Unable to copy resource %s to %s: %s", url,
                    outputPath, e.toString());
            return null;
        }

        return outputPath;
    }

    private void startUiFileContentServices(Service s) throws Throwable {
        Map<Path, String> pathToURIPath = new HashMap<>();
        ServiceDocumentDescription sdd = s.getDocumentTemplate().documentDescription;

        try {
            if (sdd != null && sdd.userInterfaceResourcePath != null) {
                String customPathResources = s
                        .getDocumentTemplate().documentDescription.userInterfaceResourcePath;
                pathToURIPath = discoverUiResources(Paths.get(customPathResources), s, true);
            } else {
                Path baseResourcePath = Utils.getServiceUiResourcePath(s);
                pathToURIPath = discoverUiResources(baseResourcePath, s, false);
            }
        } catch (Throwable e) {
            log(Level.WARNING, "Error enumerating UI resources for %s: %s", s.getSelfLink(),
                    Utils.toString(e));
        }

        if (pathToURIPath.isEmpty()) {
            log(Level.WARNING, "No custom UI resources found for %s", s
                    .getClass().getName());
            return;
        }

        for (Entry<Path, String> e : pathToURIPath.entrySet()) {
            Operation post = Operation
                    .createPost(UriUtils.buildUri(this, e.getValue()))
                    .setAuthorizationContext(this.getSystemAuthorizationContext());
            FileContentService fcs = new FileContentService(e.getKey().toFile());
            startService(post, fcs);
        }
    }

    // Find UI resources for this service (e.g. html, css, js)
    private Map<Path, String> discoverUiResources(Path path, Service s, boolean hasCustomResources)
            throws Throwable {
        Map<Path, String> pathToURIPath = new HashMap<>();
        Path baseUriPath;

        if (!hasCustomResources) {
            baseUriPath = Paths.get(ServiceUriPaths.UI_RESOURCES,
                    Utils.buildServicePath(s.getClass()));
        } else {
            baseUriPath = Paths.get(ServiceUriPaths.UI_RESOURCES, path.toString());
        }

        for (ResourceEntry entry : FileUtils.findResources(s.getClass(), path.toString())) {
            Path resourcePath = path.resolve(entry.suffix);
            Path uriPath = baseUriPath.resolve(entry.suffix);
            Path outputPath = this.copyResourceToSandbox(entry.url, resourcePath);
            if (outputPath == null) {
                // Failed to copy one resource, disable user interface for this service.
                s.toggleOption(ServiceOption.HTML_USER_INTERFACE, false);
            } else {
                pathToURIPath.put(outputPath, uriPath.toString().replace('\\', '/'));
            }
        }
        return pathToURIPath;
    }

    private void startDefaultReplicationAndNodeGroupServices() throws Throwable {
        // start the node group factory allowing for N number of independent groups
        startCoreServicesSynchronously(new NodeGroupFactoryService());

        Throwable[] error = new Throwable[1];
        CountDownLatch c = new CountDownLatch(1);

        CompletionHandler comp = (o, e) -> {
            if (e != null) {
                error[0] = e;
                log(Level.SEVERE, "Node group failed start: %s:", e.toString());
                stop();
                c.countDown();
                return;
            }
            log(Level.FINE, "started %s", o.getUri().getPath());
            this.coreServices.add(o.getUri().getPath());
            c.countDown();

        };

        // create a default node group, asynchronously. Replication services
        // that depend on a node group will register availability notifications
        // before using it

        log(Level.FINE, "starting %s", ServiceUriPaths.DEFAULT_NODE_GROUP);
        this.registerForServiceAvailability(comp, ServiceUriPaths.DEFAULT_NODE_GROUP);

        Operation post = NodeGroupFactoryService.createNodeGroupPostOp(this,
                ServiceUriPaths.DEFAULT_NODE_GROUP_NAME)
                .setReferer(UriUtils.buildUri(this, ""));
        post.setAuthorizationContext(getSystemAuthorizationContext());
        sendRequest(post);

        if (!c.await(getState().operationTimeoutMicros, TimeUnit.MICROSECONDS)) {
            throw new TimeoutException();
        }
        if (error[0] != null) {
            throw error[0];
        }

        List<Operation> startNodeSelectorPosts = new ArrayList<>();
        List<Service> nodeSelectorServices = new ArrayList<>();
        Operation startPost = Operation.createPost(UriUtils.buildUri(this,
                ServiceUriPaths.DEFAULT_NODE_SELECTOR));
        startNodeSelectorPosts.add(startPost);
        nodeSelectorServices.add(new ConsistentHashingNodeSelectorService());
        startPost = Operation.createPost(UriUtils.buildUri(this,
                ServiceUriPaths.SHA1_3X_NODE_SELECTOR));
        NodeSelectorState initialState = new NodeSelectorState();
        initialState.nodeGroupLink = ServiceUriPaths.DEFAULT_NODE_GROUP;
        // we start second node selector that does 3X replication only
        initialState.replicationFactor = 3L;
        startPost.setBody(initialState);
        startNodeSelectorPosts.add(startPost);
        nodeSelectorServices.add(new ConsistentHashingNodeSelectorService());

        // start node selector before any other core service since the host APIs of forward
        // and broadcast must be ready before any I/O
        startCoreServicesSynchronously(startNodeSelectorPosts, nodeSelectorServices);
    }

    public void joinPeers(List<URI> peers, String nodeGroupUriPath) {
        if (peers == null) {
            return;
        }

        try {
            for (URI peerNodeBaseUri : peers) {
                URI localNodeGroupUri = UriUtils.buildUri(this, nodeGroupUriPath);
                JoinPeerRequest joinBody = JoinPeerRequest
                        .create(UriUtils.extendUri(peerNodeBaseUri,
                                nodeGroupUriPath), peers.size());
                boolean doRetry = true;
                sendJoinPeerRequest(joinBody, localNodeGroupUri, doRetry);
            }
        } catch (Throwable e) {
            log(Level.WARNING, "%s", Utils.toString(e));
        }
    }

    private List<URI> normalizePeerNodeList(String[] peers) {
        List<URI> peerList = new ArrayList<>();
        if (peers == null || peers.length == 0) {
            return peerList;
        }

        for (String peer : peers) {
            URI peerNodeBaseUri = null;
            if (!peer.startsWith("http")) {
                peerNodeBaseUri = UriUtils.buildUri(peer, ServiceHost.DEFAULT_PORT, "", null);
            } else {
                try {
                    peerNodeBaseUri = new URI(peer);
                } catch (URISyntaxException e) {
                    log(Level.SEVERE, "Invalid peer uri:%s", peer);
                    continue;
                }
            }
            if (checkAndSetPreferredAddress(peerNodeBaseUri.getHost())
                    && peerNodeBaseUri.getPort() == getPort()) {
                // self, skip
                log(Level.INFO, "Skipping peer %s, its us", peerNodeBaseUri);
                continue;
            }
            peerList.add(peerNodeBaseUri);
        }
        return peerList;
    }

    private void sendJoinPeerRequest(JoinPeerRequest joinBody, URI localNodeGroupUri,
            boolean doRetry) {
        if (!doRetry) {
            log(Level.WARNING, "Retrying connection to peer %s", joinBody.memberGroupReference);
        }
        ScheduledExecutorService se = this.scheduledExecutor;
        Operation peerRequestOp = Operation
                .createPost(localNodeGroupUri)
                .setReferer(UriUtils.buildUri(this, ""))
                .setBody(joinBody)
                .setCompletion((o, e) -> {
                    if (e instanceof ConnectException && doRetry && se != null) {
                        // the remote peer has likely not started, retry, once
                        se.schedule(() -> {
                            sendJoinPeerRequest(joinBody, localNodeGroupUri, false);
                        } , 15, TimeUnit.SECONDS);
                        return;
                    }

                    if (e != null) {
                        log(Level.WARNING, "Failure joining host: %s: %s",
                                joinBody.memberGroupReference,
                                e.toString());
                    } else {
                        log(Level.INFO, "Joined peer %s", joinBody.memberGroupReference);
                    }

                });
        peerRequestOp.setAuthorizationContext(getSystemAuthorizationContext());
        sendRequest(peerRequestOp);
    }

    protected void startCoreServicesSynchronously(Service... services) throws Throwable {
        List<Operation> posts = new ArrayList<>();
        for (Service s : services) {
            URI u = UriUtils.buildUri(this, s.getClass());
            Operation startPost = Operation.createPost(u);
            posts.add(startPost);
        }
        startCoreServicesSynchronously(posts, Arrays.asList(services));
    }

    protected void startCoreServicesSynchronously(List<Operation> startPosts,
            List<Service> services)
                    throws Throwable {
        CountDownLatch l = new CountDownLatch(services.size());
        Throwable[] failure = new Throwable[1];
        StringBuilder sb = new StringBuilder();

        CompletionHandler h = (o, e) -> {
            try {
                if (e != null) {
                    failure[0] = e;
                    log(Level.SEVERE, "Service %s failed start: %s", o.getUri(), e);
                    return;
                }

                log(Level.FINE, "started %s", o.getUri().getPath());
                this.coreServices.add(o.getUri().getPath());
            } finally {
                l.countDown();
            }
        };
        int index = 0;

        // start the core services as the system user
        AuthorizationContext originalContext = OperationContext.getAuthorizationContext();
        OperationContext.setAuthorizationContext(this.getSystemAuthorizationContext());

        for (Service s : services) {
            Operation startPost = startPosts.get(index++);
            startPost.setCompletion(h);
            // explicitly set the auth context for all operations as it will not be set
            startPost.setAuthorizationContext(this.getSystemAuthorizationContext());
            sb.append(startPost.getUri().toString()).append(Operation.CR_LF);
            log(Level.FINE, "starting %s", startPost.getUri());
            startService(startPost, s);
        }

        if (!l.await(this.state.operationTimeoutMicros, TimeUnit.MICROSECONDS)) {
            log(Level.SEVERE, "One of the core services failed start: %s",
                    sb.toString(),
                    new TimeoutException());
        }

        OperationContext.setAuthorizationContext(originalContext);

        if (failure[0] != null) {
            throw failure[0];
        }
    }

    protected void setAuthorizationContext(AuthorizationContext context) {
        OperationContext.setAuthorizationContext(context);
    }

    /**
     * Subscribe to the service specified in the subscribe operation URI
     */
    public URI startSubscriptionService(
            Operation subscribe,
            Consumer<Operation> notificationConsumer) {
        return startSubscriptionService(subscribe, notificationConsumer,
                ServiceSubscriber.create(false));
    }

    /**
     * Start a {@code ReliableSubscriptionService} service and using it as the target, subscribe to the
     * service specified in the subscribe operation URI
     */
    public URI startReliableSubscriptionService(
            Operation subscribe,
            Consumer<Operation> notificationConsumer) {
        ServiceSubscriber sr = ServiceSubscriber.create(false).setUsePublicUri(true);
        ReliableSubscriptionService notificationTarget = ReliableSubscriptionService.create(
                subscribe, sr, notificationConsumer);
        return startSubscriptionService(subscribe, notificationTarget, sr);
    }

    /**
     * Subscribe to the service specified in the subscribe operation URI
     */
    public URI startSubscriptionService(
            Operation subscribe,
            Consumer<Operation> notificationConsumer,
            ServiceSubscriber request) {
        if (subscribe == null) {
            throw new IllegalArgumentException("subcribe operation is required");
        }

        if (notificationConsumer == null) {
            subscribe.fail(new IllegalArgumentException("notificationConsumer is required"));
            return null;
        }

        if (request.notificationLimit != null) {
            // notification counts are kept at the publisher, here we just validate
            if (request.notificationLimit.compareTo(0L) <= 0) {
                subscribe.fail(new IllegalArgumentException(
                        "notificationCount must be greater than zero"));
                return null;
            }
        }

        Service notificationTarget = new StatelessService() {
            @Override
            public void handleRequest(Operation op) {
                if (!op.isNotification()) {
                    super.handleRequest(op);
                    return;
                }
                notificationConsumer.accept(op);
            }
        };

        return startSubscriptionService(subscribe, notificationTarget, request);
    }

    /**
     * Start the specified subscription service (if not already started) and specify it as the
     * subscriber to the service specified in the subscribe operation URI
     */
    public URI startSubscriptionService(
            Operation subscribe,
            Service notificationTarget,
            ServiceSubscriber request) {

        if (subscribe == null) {
            throw new IllegalArgumentException("subscribe operation is required");
        }

        if (subscribe.getUri() == null) {
            subscribe.fail(new IllegalArgumentException("subscribe URI is required"));
            return null;
        }

        if (!subscribe.getUri().getPath().endsWith(SERVICE_URI_SUFFIX_SUBSCRIPTIONS)) {
            subscribe.setUri(UriUtils.extendUri(subscribe.getUri(),
                    SERVICE_URI_SUFFIX_SUBSCRIPTIONS));
        }

        // After a service has been stopped it cannot be reused
        if (notificationTarget.getProcessingStage().ordinal() > ProcessingStage.AVAILABLE
                .ordinal()) {
            subscribe.fail(new IllegalArgumentException(
                    "subscription notification target cannot be reused"));
            return null;
        }

        URI subscriptionUri;
        String notificationTargetSelfLink = notificationTarget.getSelfLink();
        if (notificationTarget.getProcessingStage() == ProcessingStage.AVAILABLE) {
            // Service is already started and is being re-used
            if (request.usePublicUri) {
                subscriptionUri = UriUtils.buildPublicUri(notificationTarget.getHost(),
                        notificationTargetSelfLink);
            } else {
                subscriptionUri = notificationTarget.getUri();
            }
        } else {
            if (notificationTargetSelfLink == null) {
                notificationTargetSelfLink = UUID.randomUUID().toString();
            }
            if (request.usePublicUri) {
                subscriptionUri = UriUtils.buildPublicUri(this, notificationTargetSelfLink);
            } else {
                subscriptionUri = UriUtils.buildUri(this, notificationTargetSelfLink);
            }
        }

        if (request.documentExpirationTimeMicros != 0) {
            long delta = request.documentExpirationTimeMicros - Utils.getNowMicrosUtc();
            if (delta <= 0) {
                log(Level.WARNING, "Expiration time is in the past: %d",
                        request.documentExpirationTimeMicros);
                subscribe.fail(new CancellationException("Subscription has already expired"));
                return null;
            }

            schedule(() -> {
                sendRequest(Operation.createDelete(UriUtils.buildUri(this,
                        notificationTarget.getSelfLink())));
            } , delta, TimeUnit.MICROSECONDS);
        }

        request.reference = subscriptionUri;
        subscribe.setBody(request);
        Operation post = Operation
                .createPost(subscriptionUri)
                .setAuthorizationContext(this.getSystemAuthorizationContext())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        subscribe.fail(e);
                        return;
                    }

                    sendRequest(subscribe);
                });

        if (notificationTarget.getProcessingStage() == ProcessingStage.CREATED) {
            this.startService(post, notificationTarget);
        } else {
            post.complete();
        }
        return subscriptionUri;
    }

    /**
     * Delete subscription from publisher and stop notification target service
     */
    public void stopSubscriptionService(
            Operation unsubscribe,
            URI notificationTarget) {
        if (unsubscribe == null) {
            throw new IllegalArgumentException("unsubscribe operation is required");
        }

        if (unsubscribe.getUri() == null) {
            unsubscribe.fail(new IllegalArgumentException("unsubscribe URI is required"));
            return;
        }

        if (!unsubscribe.getUri().getPath().endsWith(SERVICE_URI_SUFFIX_SUBSCRIPTIONS)) {
            unsubscribe.setUri(UriUtils.extendUri(unsubscribe.getUri(),
                    SERVICE_URI_SUFFIX_SUBSCRIPTIONS));
        }

        unsubscribe.setAction(Action.DELETE);

        ServiceSubscriber unSubscribeBody = new ServiceSubscriber();
        unSubscribeBody.reference = notificationTarget;
        sendRequest(unsubscribe
                .setBody(unSubscribeBody)
                .nestCompletion(
                        (deleteOp, deleteEx) -> {
                            if (deleteEx != null) {
                                unsubscribe.fail(new IllegalStateException(
                                        "Deletion of notification callback failed"));
                                return;
                            }
                            unsubscribe.complete();
                        }));
        // delete the notification target
        sendRequest(Operation
                .createDelete(notificationTarget)
                .setReferer(unsubscribe.getReferer())
                .setCompletion(
                        (deleteOp, deleteEx) -> {
                            if (deleteEx != null) {
                                log(Level.WARNING, "Deletion of notification subscriber failed");
                            }
                        }));
    }

    public static boolean isServiceStartingOrAvailable(ProcessingStage stage) {
        if (stage.ordinal() >= ProcessingStage.INITIALIZING.ordinal()
                && stage.ordinal() <= ProcessingStage.AVAILABLE.ordinal()) {
            return true;
        }
        return false;
    }

    public static boolean isServiceStarting(ProcessingStage stage) {
        if (stage.ordinal() >= ProcessingStage.CREATED.ordinal()
                && stage.ordinal() < ProcessingStage.AVAILABLE.ordinal()) {
            return true;
        }
        return false;
    }

    private boolean isServiceStarting(Service service, String path) {
        if (service != null) {
            return isServiceStarting(service.getProcessingStage());
        }

        if (path != null) {
            return false;
        }

        throw new IllegalArgumentException("service or path is required");
    }

    /**
     * A service becomes available for operation processing after its attached to a running host.
     * Service initialization is asynchronous and two phase, allowing for multiple services to start
     * concurrently but still take dependencies on each other
     */
    public ServiceHost startService(Operation post, Service service) {
        if (service == null) {
            throw new IllegalArgumentException("service is required");
        }

        if (isStopping()) {
            post.fail(new IllegalStateException("ServiceHost not started"));
            return this;
        }

        ProcessingStage stage = service.getProcessingStage();
        if (isServiceStartingOrAvailable(stage)) {
            post.complete();
            return this;
        }

        if (service.getProcessingStage() == Service.ProcessingStage.STOPPED) {
            log(Level.INFO, "Restarting service %s (%s)", service.getClass().getSimpleName(),
                    post.getUri());
        }

        if (post.getUri() == null) {
            // Reflect on the service for its preferred URI path (SELF_LINK
            // field). For non singleton services, we expect a URI provided in
            // the post operation
            post.setUri(UriUtils.buildUri(this, service.getClass()));
        }

        if (post.getReferer() == null) {
            post.setReferer(post.getUri());
        }

        service.setHost(this);

        String servicePath = UriUtils.normalizeUriPath(post.getUri().getPath()).intern();
        if (service.getSelfLink() == null) {
            service.setSelfLink(servicePath);
        }

        // if the service is a helper for one of the known URI suffixes, do not
        // add it to the map. We will special case dispatching to it
        if (isHelperServicePath(servicePath)) {
            if (!service.hasOption(Service.ServiceOption.UTILITY)) {
                post.fail(new IllegalStateException(
                        "Service is using an utility URI path but has not enabled "
                                + ServiceOption.UTILITY));
                return this;
            }
            // do not directly attach utility services
        } else {
            synchronized (this.state) {
                Service previous = this.attachedServices.put(servicePath, service);
                if (previous != null) {
                    this.attachedServices.put(servicePath, previous);
                    post.fail(new ServiceAlreadyStartedException(servicePath));
                    return this;
                }

                this.state.serviceCount++;
            }
        }

        if (post.getExpirationMicrosUtc() == 0) {
            post.setExpiration(this.state.operationTimeoutMicros + Utils.getNowMicrosUtc());
        }

        service.setProcessingStage(ProcessingStage.CREATED);

        // make sure we detach the service on start failure
        post.nestCompletion((o, e) -> {
            this.pendingStartOperations.remove(post);
            if (e != null) {
                stopService(service);
                post.fail(e);
                return;
            }
            post.complete();
        });

        this.pendingStartOperations.add(post);
        if (!validateServiceOptions(service, post)) {
            return this;
        }

        processServiceStart(ProcessingStage.INITIALIZING, service, post, post.hasBody());
        return this;
    }

    private boolean validateServiceOptions(Service service, Operation post) {

        for (ServiceOption o : service.getOptions()) {
            String error = Utils.validateServiceOption(service.getOptions(), o);
            if (error != null) {
                log(Level.WARNING, error);
                post.fail(new IllegalArgumentException(error));
                return false;
            }
        }

        if (service.getMaintenanceIntervalMicros() > 0 &&
                service.getMaintenanceIntervalMicros() < getMaintenanceIntervalMicros()) {
            log(Level.WARNING,
                    "Service maint. interval %d is less than host interval %d, reducing host interval",
                    service.getMaintenanceIntervalMicros(), getMaintenanceIntervalMicros());
            this.setMaintenanceIntervalMicros(service.getMaintenanceIntervalMicros());
        }
        return true;
    }

    void notifyServiceAvailabilitySubscribers(Service s) {
        SortedSet<Operation> ops = null;
        synchronized (this.state) {
            ops = this.pendingServiceAvailableCompletions.remove(s.getSelfLink());
            if (ops == null) {
                return;
            }
        }

        for (Operation op : ops) {
            run(() -> {
                if (op.getUri() == null) {
                    op.setUri(s.getUri());
                }
                op.complete();
            });
        }
    }

    public static boolean isServiceIndexed(Service s) {
        return s.hasOption(ServiceOption.PERSISTENCE);
    }

    private void processServiceStart(ProcessingStage next, Service s,
            Operation post, boolean hasClientSuppliedInitialState) {

        if (next == s.getProcessingStage()) {
            post.complete();
            return;
        }

        if (isStopping()) {
            post.fail(new CancellationException());
            return;
        }

        if (s.getProcessingStage() == ProcessingStage.STOPPED) {
            post.fail(new CancellationException());
            return;
        }

        try {
            s.setProcessingStage(next);

            switch (next) {
            case INITIALIZING:
                final ProcessingStage nextStage =
                        isServiceIndexed(s) ?
                                ProcessingStage.LOADING_INITIAL_STATE :
                                ProcessingStage.SYNCHRONIZING;

                buildDocumentDescription(s);
                if (post.hasBody()) {
                    // make sure body is in native form and has creation time
                    ServiceDocument d = post.getBody(s.getStateType());
                    d.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
                }

                // Populate authorization context if necessary
                if (this.isAuthorizationEnabled() &&
                        this.authorizationService != null &&
                        this.authorizationService.getProcessingStage() == ProcessingStage.AVAILABLE) {
                    post.nestCompletion(op -> {
                        processServiceStart(nextStage, s, post, hasClientSuppliedInitialState);
                    });
                    queueOrScheduleRequest(this.authorizationService, post);
                    break;
                }

                processServiceStart(nextStage, s, post, hasClientSuppliedInitialState);
                break;
            case LOADING_INITIAL_STATE:
                if (isServiceIndexed(s) && !post.isFromReplication()) {
                    // we load state from the local index if the service is indexed and this is NOT
                    // a replication POST that came from another node. If its a replicated POST we
                    // use the body as is
                    loadInitialServiceState(s, post, ProcessingStage.SYNCHRONIZING,
                            hasClientSuppliedInitialState);
                } else {
                    processServiceStart(ProcessingStage.SYNCHRONIZING, s, post,
                            hasClientSuppliedInitialState);
                }
                break;
            case SYNCHRONIZING:
                if (s.hasOption(ServiceOption.FACTORY) || !s.hasOption(ServiceOption.REPLICATION)) {
                    processServiceStart(ProcessingStage.EXECUTING_START_HANDLER, s, post,
                            hasClientSuppliedInitialState);
                    break;
                }

                post.nestCompletion((o) -> {
                    boolean hasInitialState = hasClientSuppliedInitialState;
                    if (o.getLinkedState() != null) {
                        hasInitialState = true;
                    }
                    processServiceStart(ProcessingStage.EXECUTING_START_HANDLER, s, post,
                            hasInitialState);
                });

                // We never synchronize state with peers, on service start. Synchronization occurs
                // due to a node group change event, through handleMaintenance on factories
                boolean synchronizeState = false;
                selectServiceOwnerAndSynchState(s, post, synchronizeState);
                break;
            case EXECUTING_START_HANDLER:
                Long version = null;
                if (post.hasBody()) {
                    ServiceDocument stateFromDocumentStore = post.getLinkedState();
                    if (stateFromDocumentStore != null) {
                        version = stateFromDocumentStore.documentVersion;
                        post.linkState(null);
                    }
                }

                Long finalVersion = version;

                post.nestCompletion((o) -> {
                    ServiceDocument document = null;

                    normalizeInitialServiceState(s, post, finalVersion);

                    if (post.hasBody()) {
                        document = post.getBody(s.getStateType());
                    }
                    if (!authorizeServiceState(s, document, post)) {
                        post.fail(Operation.STATUS_CODE_FORBIDDEN);
                        return;
                    }
                    processServiceStart(ProcessingStage.INDEXING_INITIAL_STATE, s, post,
                            hasClientSuppliedInitialState);
                });

                if (!isDocumentOwner(s)) {
                    // bypass handleStart on nodes that do not own the service
                    post.complete();
                    break;
                }

                String contextId = post.getContextId();
                if (contextId != null) {
                    OperationContext.setContextId(contextId);
                }

                AuthorizationContext originalContext = OperationContext.getAuthorizationContext();
                OperationContext.setAuthorizationContext(post.getAuthorizationContext());

                try {
                    s.handleStart(post);
                } catch (Throwable e) {
                    handleUncaughtException(s, post, e);
                }

                OperationContext.setAuthorizationContext(originalContext);

                if (contextId != null) {
                    OperationContext.setContextId(null);
                }
                break;
            case INDEXING_INITIAL_STATE:
                boolean needsIndexing = isServiceIndexed(s)
                        && hasClientSuppliedInitialState;

                post.nestCompletion(o -> {
                    processServiceStart(ProcessingStage.AVAILABLE, s, post,
                            hasClientSuppliedInitialState);
                });

                if (!post.hasBody() || !needsIndexing) {
                    post.complete();
                    break;
                }

                saveServiceState(s, post, (ServiceDocument) post.getBodyRaw());
                break;
            case AVAILABLE:
                // It's possible a service is stopped before it transitions to available
                if (s.getProcessingStage() == ProcessingStage.STOPPED) {
                    post.complete();
                    return;
                }

                if (s.hasOption(ServiceOption.HTML_USER_INTERFACE)) {
                    startUiFileContentServices(s);
                }
                if (s.hasOption(ServiceOption.PERIODIC_MAINTENANCE)) {
                    this.maintenanceHelper.schedule(s);
                }

                s.setProcessingStage(Service.ProcessingStage.AVAILABLE);

                log(Level.FINE, "Started %s", s.getSelfLink());
                post.complete();

                if (s.hasOption(ServiceOption.DOCUMENT_OWNER)) {
                    scheduleServiceOptionToggleMaintenance(s.getSelfLink(),
                            EnumSet.of(ServiceOption.DOCUMENT_OWNER), null);
                }
                break;

            default:
                break;

            }
        } catch (Throwable e) {
            log(Level.SEVERE, "Unhandled error: %s", Utils.toString(e));
            post.fail(e);
        }
    }

    boolean isDocumentOwner(Service s) {
        return !s.hasOption(ServiceOption.OWNER_SELECTION) ||
                s.hasOption(ServiceOption.DOCUMENT_OWNER);
    }

    /**
     * Invoke the service setInitialState method and ensures the state has proper self link and
     * kind. It caches the state and sets it as the body to the post operation
     */
    void normalizeInitialServiceState(Service s, Operation post, Long finalVersion) {
        if (!post.hasBody()) {
            return;
        }
        // cache initial state, after service had a chance to modify in
        // handleStart(), in memory. We force serialize to JSON to clone
        // and prove the state *is* convertible to JSON.
        Object body = post.getBodyRaw();

        ServiceDocument initialState = s.setInitialState(
                Utils.toJson(body),
                finalVersion);

        initialState.documentSelfLink = s.getSelfLink();
        initialState.documentKind = Utils.buildKind(initialState.getClass());
        initialState.documentAuthPrincipalLink = (post.getAuthorizationContext() != null) ? post
                .getAuthorizationContext().getClaims().getSubject() : null;
        post.setBody(initialState);
        if (!s.hasOption(ServiceOption.CONCURRENT_UPDATE_HANDLING)) {
            initialState = Utils.clone(initialState);
        }

        cacheServiceState(s, initialState, null);
    }

    /**
     * Infrastructure use only.
     *
     * Called on demand or due to node group changes to synchronize replicated services
     * associated with the specified node selector path
     */
    public void scheduleNodeGroupChangeMaintenance(String nodeSelectorPath) {
        if (nodeSelectorPath == null) {
            throw new IllegalArgumentException("nodeGroupPath is required");
        }

        this.pendingNodeSelectorsForFactorySynch.add(nodeSelectorPath);
    }

    void startOrSynchService(Operation post, Service child) {
        Service s = findService(post.getUri().getPath());
        if (s == null) {
            startService(post, child);
            return;
        }

        Operation synchPut = Operation.createPut(post.getUri())
                .setBody(new ServiceDocument())
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_FORWARDING)
                .setReplicationDisabled(true)
                .addRequestHeader(Operation.REPLICATION_PHASE_HEADER,
                        Operation.REPLICATION_PHASE_SYNCHRONIZE)
                .setReferer(post.getReferer())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        post.setStatusCode(o.getStatusCode()).setBodyNoCloning(o.getBodyRaw())
                                .fail(e);
                        return;
                    }

                    post.complete();
                });

        sendRequest(synchPut);
    }

    void selectServiceOwnerAndSynchState(Service s, Operation op, boolean synchronizeState) {
        Operation selectOwnerOp = Operation.createPost(null)
                .setExpiration(op.getExpirationMicrosUtc())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        log(Level.WARNING, "Failure partitioning %s: %s", op.getUri(),
                                e.toString());
                        if (s.hasOption(ServiceOption.ENFORCE_QUORUM)) {
                            op.fail(e);
                            return;
                        }
                        // proceed with starting service anyway
                        s.toggleOption(ServiceOption.DOCUMENT_OWNER, true);
                        op.complete();
                        return;
                    }

                    SelectOwnerResponse rsp = o.getBody(SelectOwnerResponse.class);
                    if (!synchronizeState) {
                        s.toggleOption(ServiceOption.DOCUMENT_OWNER, rsp.isLocalHostOwner);
                        op.complete();
                        return;
                    }
                    synchronizeWithPeers(s, op, rsp);
                });

        selectOwner(s.getPeerNodeSelectorPath(), s.getSelfLink(), selectOwnerOp);
    }

    private void synchronizeWithPeers(Service s, Operation op, SelectOwnerResponse rsp) {
        // service is durable and replicated. We need to ask our peers if they
        // have more recent state version than we do, then pick the latest one
        // (or the most valid one, depending on peer consensus)

        SynchronizePeersRequest t = SynchronizePeersRequest.create();
        t.stateDescription = buildDocumentDescription(s);
        t.wasOwner = s.hasOption(ServiceOption.DOCUMENT_OWNER);
        t.isOwner = rsp.isLocalHostOwner;
        t.ownerNodeReference = rsp.ownerNodeReference;
        t.ownerNodeId = rsp.ownerNodeId;
        s.toggleOption(ServiceOption.DOCUMENT_OWNER, t.isOwner);
        t.options = s.getOptions();
        t.state = op.hasBody() ? op.getBody(s.getStateType()) : null;
        t.factoryLink = UriUtils.getParentPath(s.getSelfLink());
        if (t.factoryLink == null || t.factoryLink.isEmpty()) {
            String error = String.format("Factory not found for %s."
                    + "If the service is not created through a factory it should not set %s",
                    s.getSelfLink(), ServiceOption.OWNER_SELECTION);
            op.fail(new IllegalStateException(error));
            return;
        }

        if (t.state == null) {
            // we have no initial state or state from storage. Create an empty state so we can
            // compare with peers
            ServiceDocument template = null;
            try {
                template = s.getStateType().newInstance();
            } catch (Throwable e) {
                log(Level.SEVERE, "Could not create instance state type: %s", e.toString());
                op.fail(e);
                return;
            }
            template.documentKind = Utils.buildKind(s.getStateType());
            template.documentSelfLink = s.getSelfLink();
            template.documentEpoch = 0L;
            t.state = template;
        }

        if (t.state.documentSelfLink == null) {
            log(Level.WARNING, "missing selflink for %s", s.getClass());
        }

        CompletionHandler c = (o, e) -> {
            ServiceDocument selectedState = null;

            if (isStopping()) {
                op.fail(new CancellationException());
                return;
            }

            if (e != null) {
                op.fail(e);
                return;
            }

            if (o.hasBody()) {
                selectedState = o.getBody(s.getStateType());
            } else {
                // peers did not have a better state to offer
                op.complete();
                return;
            }

            if (ServiceDocument.isDeleted(selectedState)) {
                // The peer nodes have this service but it has been marked as
                // deleted.
                // Fail start and delete local version from index

                log(Level.WARNING,
                        "Attempt to create document marked as deleted: %s",
                        s.getSelfLink());
                op.fail(new IllegalStateException(
                        "Document marked deleted by peers: " + s.getSelfLink()));
                selectedState.documentSelfLink = s.getSelfLink();
                selectedState.documentUpdateAction = Action.DELETE.toString();
                // delete local version
                saveServiceState(s, Operation.createDelete(UriUtils.buildUri(this,
                        s.getSelfLink())).setReferer(s.getUri()),
                        selectedState);
                return;
            }

            // The remote peers have a more recent state than the one we loaded from the store.
            // Use the peer service state as the initial state.
            op.setBodyNoCloning(selectedState).complete();
        };

        URI synchServiceForGroup = UriUtils.extendUri(
                UriUtils.buildUri(this, s.getPeerNodeSelectorPath()),
                ServiceUriPaths.SERVICE_URI_SUFFIX_SYNCHRONIZATION);
        Operation synchPost = Operation
                .createPost(synchServiceForGroup)
                .setBodyNoCloning(t)
                .setReferer(s.getUri())
                .setCompletion(c);
        sendRequest(synchPost);
    }

    void loadServiceState(Service s, Operation op) {
        ServiceDocument state = getCachedServiceState(s.getSelfLink());

        // Clone state if it might change while processing
        if (state != null && !s.hasOption(ServiceOption.CONCURRENT_UPDATE_HANDLING)) {
            state = Utils.clone(state);
        }

        // If either there is cached state, or the service is not indexed (meaning nothing
        // will be found in the index), subject this state to authorization.
        if (state != null || !isServiceIndexed(s)) {
            if (!authorizeServiceState(s, state, op)) {
                op.fail(Operation.STATUS_CODE_FORBIDDEN);
                return;
            }

            if (state != null) {
                op.linkState(state);
            }

            op.complete();
            return;
        }

        if (s.hasOption(ServiceOption.INSTRUMENTATION)) {
            s.adjustStat(Service.STAT_NAME_CACHE_MISS_COUNT, 1);
        }

        URI u = UriUtils.buildDocumentQueryUri(this, s.getSelfLink(), false, true, s.getOptions());
        Operation loadGet = Operation
                .createGet(u)
                .setReferer(op.getReferer())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }

                    if (!o.hasBody()) {
                        op.fail(new IllegalStateException("Unable to locate service state in index"));
                        return;
                    }

                    ServiceDocument st = o.getBody(s.getStateType());
                    if (!authorizeServiceState(s, st, op)) {
                        op.fail(Operation.STATUS_CODE_FORBIDDEN);
                        return;
                    }

                    op.linkState(st).complete();
                });

        Service indexService = this.documentIndexService;
        if (indexService == null) {
            op.fail(new CancellationException());
            return;
        }

        this.documentIndexService.handleRequest(loadGet);
    }

    private boolean authorizeServiceState(Service service, ServiceDocument document, Operation op) {
        // Authorization not enabled, so there is nothing to check
        if (!this.isAuthorizationEnabled()) {
            return true;
        }

        AuthorizationContext ctx = op.getAuthorizationContext();
        if (ctx == null) {
            return false;
        }

        // Allow unconditionally if this is the system user
        if (ctx.isSystemUser()) {
            return true;
        }

        // No service state specified; build artificial state for service so it can be subjected
        // to this authorization check (e.g. stateful without initial state, stateless services).
        if (document == null) {
            Class<? extends ServiceDocument> clazz = service.getStateType();
            try {
                document = clazz.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                log(Level.SEVERE, "Unable to instantiate %s: %s", clazz.toString(), e.toString());
                return false;
            }

            document.documentSelfLink = service.getSelfLink();
            document.documentKind = Utils.buildKind(clazz);
        }

        ServiceDocumentDescription documentDescription = buildDocumentDescription(service);
        QueryFilter queryFilter = ctx.getResourceQueryFilter();
        if (queryFilter == null || !queryFilter.evaluate(document, documentDescription)) {
            return false;
        }

        return true;
    }

    void loadInitialServiceState(Service s, Operation serviceStartPost, ProcessingStage next,
            boolean hasClientSuppliedState) {
        URI u = UriUtils.buildDocumentQueryUri(this,
                serviceStartPost.getUri().getPath(),
                false,
                true,
                s.getOptions());
        Operation loadGet = Operation
                .createGet(u)
                .setReferer(serviceStartPost.getReferer())
                .setCompletion((indexQueryOperation, e) -> {
                    handleLoadInitialStateCompletion(s, serviceStartPost, next,
                            hasClientSuppliedState,
                            indexQueryOperation, e);
                });
        sendRequest(loadGet);
    }

    void cacheServiceState(Service s, ServiceDocument st, Operation op) {
        if (op != null && op.hasBody()) {
            Object rsp = op.getBodyRaw();
            // if the response body is of type Document set its common
            // properties to that of the service state
            if (rsp.getClass().equals(st.getClass())) {
                ServiceDocument r = (ServiceDocument) rsp;
                st.copyTo(r);
            }
        }

        if (!this.state.isServiceStateCaching && isServiceIndexed(s)) {
            return;
        }

        if (op != null && op.getAction() == Action.DELETE) {
            return;
        }

        ServiceDocument p = null;
        synchronized (s.getSelfLink()) {
            p = this.cachedServiceStates.put(s.getSelfLink(), st);
            if (p != null && p.documentVersion > st.documentVersion) {
                // restore cached state, discarding update, if the existing version is higher
                this.cachedServiceStates.put(s.getSelfLink(), p);
            }
        }
    }

    private void handleLoadInitialStateCompletion(Service s, Operation serviceStartPost,
            ProcessingStage next,
            boolean hasClientSuppliedState, Operation indexQueryOperation, Throwable e) {
        if (e != null) {
            if (!isStopping()) {
                log(Level.SEVERE, "Error loading state for service %s: %s",
                        serviceStartPost.getUri(), Utils.toString(e));
            }
            serviceStartPost.fail(e);
            return;
        }

        ServiceDocument stateFromStore = null;
        if (indexQueryOperation.hasBody()) {
            stateFromStore = indexQueryOperation.getBody(s.getStateType());
            serviceStartPost.linkState(stateFromStore);
        }

        if (!isServiceRestartAfterDeleteAllowed(stateFromStore, serviceStartPost)) {
            serviceStartPost.fail(new IllegalStateException("Service has been previously deleted: "
                    + Utils.toJson(stateFromStore)));
            return;
        }

        if (hasClientSuppliedState && stateFromStore != null) {
            // initial state counts as new version
            stateFromStore.documentVersion++;
        } else if (stateFromStore != null
                && stateFromStore.documentSelfLink != null) {
            // set the initial state from what the index returned
            serviceStartPost.setBody(stateFromStore);
        }

        processServiceStart(next, s,
                serviceStartPost, hasClientSuppliedState);
    }

    private static boolean isServiceRestartAfterDeleteAllowed(ServiceDocument stateFromStore,
            Operation serviceStartPost) {
        if (!serviceStartPost.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERSION_CHECK)) {
            return true;
        }

        if (!ServiceDocument.isDeleted(stateFromStore)) {
            return true;
        }

        // if the POST has no body, and is marked deleted, fail restart
        if (!serviceStartPost.hasBody()) {
            return false;
        }

        ServiceDocument initState = (ServiceDocument) serviceStartPost.getBodyRaw();
        if (stateFromStore.documentVersion < initState.documentVersion) {
            // new state is higher than previously indexed state, allow restart
            return true;
        }

        // new state has stale version, fail restart
        return false;
    }

    /**
     * Detaches service from service host, sets processing stage to stop. This method should only be
     * invoked by the service itself (and in most cases that is the only possible way since its the only
     * one with access to its reference)
     * @param service
     */
    public void stopService(Service service) {
        if (service == null) {
            throw new IllegalArgumentException("service is required");
        }
        stopService(service.getSelfLink());
    }

    private void stopService(String path) {
        Service existing = this.attachedServices.remove(path);
        if (existing == null) {
            path = UriUtils.normalizeUriPath(path);
            existing = this.attachedServices.remove(path);
        }

        if (existing != null) {
            existing.setProcessingStage(ProcessingStage.STOPPED);
        }

        this.pendingPauseServices.remove(path);
        clearCachedServiceState(path);

        synchronized (this.state) {
            this.state.serviceCount--;
        }

        // we do not remove from maintenance tracker, service will
        // be ignored and never schedule for maintenance if its stopped
    }

    protected Service findService(String uriPath) {
        Service s = this.attachedServices.get(uriPath);
        if (s != null) {
            return s;
        }

        s = this.attachedServices.get(UriUtils.normalizeUriPath(uriPath));
        if (s != null) {
            return s;
        }

        if (isHelperServicePath(uriPath)) {
            return findHelperService(uriPath);
        }

        return null;
    }

    Service findHelperService(String uriPath) {
        Service s;
        String subPath = uriPath.substring(0,
                uriPath.lastIndexOf(UriUtils.URI_PATH_CHAR));
        // use the prefix to find the actual service
        s = this.attachedServices.get(subPath);
        if (s == null) {
            return null;
        }
        // now find the helper, given the suffix
        return s.getUtilityService(uriPath);
    }

    /**
     * Infrastructure use only
     */
    public boolean handleRequest(Operation inboundOp) {
        return handleRequest(null, inboundOp);
    }

    /**
     * Infrastructure use only
     */
    public boolean handleRequest(Service service, Operation inboundOp) {
        if (inboundOp == null && service != null) {
            inboundOp = service.dequeueRequest();
        }

        if (inboundOp == null) {
            return true;
        }

        if (inboundOp.getUri().getPort() != this.state.httpPort
                && inboundOp.getUri().getPort() != this.state.httpsPort) {
            return false;
        }

        if (!ServiceHost.LOCAL_HOST.equals(inboundOp.getUri().getHost())) {
            if (!UriUtils.isHostEqual(this, inboundOp.getUri())) {
                return false;
            }
        }

        if (inboundOp.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_REPLICATED)) {
            inboundOp.setFromReplication(true).setTargetReplicated(true);
        }

        if (!this.state.isStarted) {
            failRequest(inboundOp, Operation.STATUS_CODE_NOT_FOUND,
                    new IllegalStateException("Service host not started"));
            return true;
        }

        if (inboundOp.getAuthorizationContext() == null) {
            populateAuthorizationContext(inboundOp);
        }

        if (this.isAuthorizationEnabled()) {
            if (this.authorizationService != null) {
                inboundOp.nestCompletion(op -> {
                    handleAuthorizedRequest(service, op);
                });
                queueOrScheduleRequest(this.authorizationService, inboundOp);
                return true;
            }
        }

        return handleAuthorizedRequest(service, inboundOp);
    }

    private boolean handleAuthorizedRequest(Service service, Operation inboundOp) {
        String path;
        if (service == null) {
            path = inboundOp.getUri().getPath();
            if (path == null) {
                failRequestServiceNotFound(inboundOp);
                return true;
            }
            service = findService(path);
        } else {
            path = service.getSelfLink();
        }

        // if this service was about to stop, due to memory pressure, cancel, its still active
        Service pendingStopService = this.pendingPauseServices.remove(path);
        if (pendingStopService != null) {
            service = pendingStopService;
        }

        if (queueRequestUntilServiceAvailable(inboundOp, service, path)) {
            return true;
        }

        if (queueOrForwardRequest(service, path, inboundOp)) {
            return true;
        }

        if (service == null) {
            failRequestServiceNotFound(inboundOp);
            return true;
        }

        traceOperation(inboundOp);

        queueOrScheduleRequest(service, inboundOp);
        return true;
    }

    private void populateAuthorizationContext(Operation op) {
        AuthorizationContext ctx = getAuthorizationContext(op);
        if (ctx == null) {
            // No (valid) authorization context, fall back to guest context
            ctx = getGuestAuthorizationContext();
        }

        op.setAuthorizationContext(ctx);
    }

    private AuthorizationContext getAuthorizationContext(Operation op) {
        String token = op.getRequestHeader(Operation.REQUEST_AUTH_TOKEN_HEADER);
        if (token == null) {
            Map<String, String> cookies = op.getCookies();
            if (cookies == null) {
                return null;
            }
            token = cookies.get(AuthenticationConstants.DCP_JWT_COOKIE);
        }

        if (token == null) {
            return null;
        }

        try {
            AuthorizationContext.Builder b = AuthorizationContext.Builder.create();
            Claims claims = this.getTokenVerifier().verify(token, Claims.class);
            Long expirationTime = claims.getExpirationTime();
            if (expirationTime != null && expirationTime <= Utils.getNowMicrosUtc()) {
                return null;
            }

            b.setClaims(claims);
            b.setToken(token);
            return b.getResult();
        } catch (TokenException | GeneralSecurityException e) {
            log(Level.INFO, "Error verifying token: %s", e);
        }

        return null;
    }

    private void failRequestServiceNotFound(Operation inboundOp) {
        failRequest(inboundOp, Operation.STATUS_CODE_NOT_FOUND, new ServiceNotFoundException());
    }

    /**
     * Forwards request to a peer, if local node is not the owner for the service. This method is
     * part of the consensus logic for the replication protocol. It serves the following functions:
     *
     * 1) If this request came from a client, it performs the role of finding the owner, on behalf
     * of the client
     *
     * 2) If this request came from a peer node AND the local node is the owner, then it handles
     * request, initiating the replication state machine
     *
     * 3) If the request came from a peer owner node, the local node is acting as a certifier
     * replica and needs to verify it agrees on epoch,  owner and the state version.
     *
     * In both cases 2 and 3 the request will be handled locally.
     *
     * Note that we do not require the service to be present locally. We will use the URI path to
     * select an owner and forward.
     *
     * @return
     */
    private boolean queueOrForwardRequest(Service s, String path, Operation op) {

        if (s == null && op.isFromReplication()) {
            if (op.getAction() == Action.DELETE) {
                op.complete();
            } else {
                failRequestServiceNotFound(op);
            }
            return true;
        }

        String nodeSelectorPath;
        Service parent = null;
        EnumSet<ServiceOption> options = null;
        if (s != null) {
            // Common path, service is known.
            options = s.getOptions();

            if (options == null) {
                return false;
            } else if (options.contains(ServiceOption.UTILITY)) {
                // find the parent service, which will have the complete option set
                // relevant to forwarding
                path = UriUtils.getParentPath(path);
                parent = findService(path);
                if (parent == null) {
                    failRequestServiceNotFound(op);
                    return true;
                }
                options = parent.getOptions();
            }

            if (options == null
                    || !options.contains(ServiceOption.OWNER_SELECTION)
                    || options.contains(ServiceOption.FACTORY)) {
                return false;
            }
        } else {
            // Service is unknown.
            // Find the service options indirectly, if there is a parent factory.
            if (isHelperServicePath(path)) {
                path = UriUtils.getParentPath(path);
            }

            String factoryPath = UriUtils.getParentPath(path);
            if (factoryPath == null) {
                failRequestServiceNotFound(op);
                return true;
            }

            parent = findService(factoryPath);
            if (parent == null) {
                failRequestServiceNotFound(op);
                return true;
            }
            options = parent.getOptions();

            if (options == null ||
                    !options.contains(ServiceOption.FACTORY) ||
                    !options.contains(ServiceOption.REPLICATION)) {
                return false;
            }
        }

        if (op.isForwardingDisabled()) {
            return false;
        }

        if (parent != null) {
            nodeSelectorPath = parent.getPeerNodeSelectorPath();
        } else {
            nodeSelectorPath = s.getPeerNodeSelectorPath();
        }

        op.setStatusCode(Operation.STATUS_CODE_OK);

        String servicePath = path;
        CompletionHandler ch = (o, e) -> {
            if (e != null) {
                log(Level.SEVERE, "Owner selection failed for service %s, op %d. Error: %s", op
                        .getUri().getPath(), op.getId(), e.toString());
                op.setRetryCount(0).fail(e);
                run(() -> {
                    handleRequest(s, null);
                });
                return;
            }

            // fail or forward the request if we do not agree with the sender, who the owner is
            SelectOwnerResponse rsp = o.getBody(SelectOwnerResponse.class);

            if (op.isFromReplication()) {
                ServiceDocument body = op.getBody(s.getStateType());
                if (!rsp.ownerNodeId.equals(body.documentOwner)) {
                    failRequestOwnerMismatch(op, rsp.ownerNodeId, body);
                    return;
                }

                queueOrScheduleRequest(s, op);
                return;
            }

            CompletionHandler fc = (fo, fe) -> {
                if (fe != null) {
                    retryOrFailRequest(op, fo, fe);
                    return;
                }

                op.setStatusCode(fo.getStatusCode());
                if (fo.hasBody()) {
                    op.setBodyNoCloning(fo.getBodyRaw());
                }
                op.transferResponseHeadersFrom(fo);
                op.complete();
            };

            Operation forwardOp = op.clone().setCompletion(fc);
            if (rsp.isLocalHostOwner) {
                if (s == null) {
                    queueOrFailRequestForServiceNotFoundOnOwner(servicePath, op);
                    return;
                }
                queueOrScheduleRequest(s, forwardOp);
                return;
            }

            if (op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORWARDED)) {
                // this was forwarded from another node, but we do not think we own the service
                failRequestOwnerMismatch(op, op.getUri().getPath(), null);
                return;
            }

            if (op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_REPLICATED)) {
                failRequestOwnerMismatch(op, op.getUri().getPath(), null);
                return;
            }

            forwardOp.setUri(SelectOwnerResponse.buildUriToOwner(rsp, op));
            forwardOp.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORWARDED);
            forwardOp.removeRequestCallbackLocation();
            // Local host is not the owner, but is the entry host for a client. Forward to owner
            // node
            sendRequest(forwardOp);
        };

        Operation selectOwnerOp = Operation
                .createPost(null)
                .setExpiration(op.getExpirationMicrosUtc())
                .setCompletion(ch);
        selectOwner(nodeSelectorPath, path, selectOwnerOp);
        return true;
    }

    private void queueOrFailRequestForServiceNotFoundOnOwner(String path, Operation op) {
        if (op.getAction() == Action.DELETE) {
            // do not queue DELETE actions for services not present, complete with success
            op.complete();
            return;
        }

        if (checkAndResumePausedService(op)) {
            return;
        }

        boolean doNotQueue = op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_QUEUING);
        String userAgent = op.getRequestHeader(Operation.USER_AGENT_HEADER);
        if (userAgent == null) {
            userAgent = op.getRequestHeader(Operation.USER_AGENT_HEADER.toLowerCase());
        }

        if (userAgent != null && !userAgent.contains(ServiceHost.class.getSimpleName())) {
            // do not implicitly queue requests from other request sources
            doNotQueue = true;
        }

        if (doNotQueue) {
            this.failRequestServiceNotFound(op);
            return;
        }

        log(Level.INFO, "Registering for %s to become available on owner (%s)", path, getId());
        // service not available, register, then retry
        op.nestCompletion((avop) -> {
            handleRequest(null, op);
        });
        registerForServiceAvailability(op, path);
        return;
    }

    void failRequestOwnerMismatch(Operation op, String id, ServiceDocument body) {
        String owner = body != null ? body.documentOwner : "";
        op.setStatusCode(Operation.STATUS_CODE_CONFLICT);
        Throwable e = new IllegalStateException(String.format(
                "Owner in body: %s, computed locally: %s",
                owner, id));
        ServiceErrorResponse rsp = ServiceErrorResponse.create(e, op.getStatusCode(),
                EnumSet.of(ErrorDetail.SHOULD_RETRY));
        op.fail(e, rsp);
    }

    public void failRequestActionNotSupported(Operation request) {
        request.setStatusCode(Operation.STATUS_CODE_BAD_METHOD).fail(
                new IllegalArgumentException("Action not supported: " + request.getAction()));
    }

    void failRequestLimitExceeded(Operation request) {
        // Add a header indicating retry should be attempted after some interval.
        // Currently set to just one second, subject to change in the future
        request.addResponseHeader(Operation.RETRY_AFTER_HEADER, "1");
        // a specific ServiceErrorResponse will be added in the future with retry hints
        request.setStatusCode(Operation.STATUS_CODE_UNAVAILABLE)
                .fail(new CancellationException("queue limit exceeded"));
    }

    private void failForwardRequest(Operation op, Operation fo, Throwable fe) {
        op.setStatusCode(fo.getStatusCode());
        op.setBodyNoCloning(fo.getBodyRaw()).fail(fe);
    }

    private void retryOrFailRequest(Operation op, Operation fo, Throwable fe) {
        boolean shouldRetry = false;

        if (fo.hasBody()) {
            ServiceErrorResponse rsp = fo.clone().getBody(ServiceErrorResponse.class);
            if (rsp != null && rsp.details != null) {
                shouldRetry = rsp.details.contains(ErrorDetail.SHOULD_RETRY);
            }
        }

        if (op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORWARDED)) {
            // only retry on the node the client directly communicates with. Any node that receives
            // a forwarded operation will have forwarding disabled set, and should not retry
            shouldRetry = false;
        }

        if (shouldRetry) {
            this.pendingOperationsForRetry.add(op);
            return;
        }

        if (op.getExpirationMicrosUtc() < Utils.getNowMicrosUtc()) {
            op.setBodyNoCloning(fo.getBodyRaw()).fail(new TimeoutException());
            return;
        }

        failForwardRequest(op, fo, fe);

    }

    /**
     * Determine if the request should be queued because the target service is in the process
     * of being started or, if its parent suffix is registered to a factory, the factory is not yet available
     */
    boolean queueRequestUntilServiceAvailable(Operation inboundOp, Service s, String path) {
        if (s != null && s.getProcessingStage() == ProcessingStage.AVAILABLE) {
            return false;
        }

        if (isHelperServicePath(path)) {
            path = UriUtils.getParentPath(path);
        }

        boolean waitForService = isServiceStarting(s, path);

        String factoryPath = UriUtils.getParentPath(path);
        if (factoryPath != null && !waitForService) {
            Service factoryService = this.findService(factoryPath);
            // Only wait for factory if the logical parent of this service
            // is a factory which itself is starting
            if (factoryService != null) {
                if (factoryService.hasOption(ServiceOption.FACTORY)) {
                    waitForService = isServiceStarting(factoryService, factoryPath);
                }
                if (!waitForService) {
                    // the service might be paused (stopped due to memory pressure)
                    if (factoryService.hasOption(ServiceOption.PERSISTENCE)) {
                        if (checkAndResumePausedService(inboundOp)) {
                            return true;
                        }
                    }
                }
            }
        }

        if (inboundOp.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY)
                || inboundOp.isForwardingDisabled()) {
            waitForService = true;
        }

        if (waitForService || inboundOp.isFromReplication()) {
            if (inboundOp.getAction() == Action.DELETE) {
                // do not register for availability on DELETE action, allow downstream code to forward
                return false;
            }

            Level l = inboundOp.isFromReplication() ? Level.FINE : Level.INFO;
            log(l, "registering for %s (%s) to become available", path, factoryPath);
            // service is in the process of starting
            inboundOp.nestCompletion((o) -> {
                inboundOp.setTargetReplicated(false);
                handleRequest(null, inboundOp);
            });

            registerForServiceAvailability(inboundOp, path);
            return true;
        }

        // indicate we are not waiting for service start, request should be forwarded or failed
        return false;
    }

    private static void failRequest(Operation request, int statusCode, Throwable e) {
        request.setStatusCode(statusCode);
        ServiceErrorResponse r = Utils.toServiceErrorResponse(e);
        r.statusCode = statusCode;

        if (e instanceof ServiceNotFoundException) {
            r.stackTrace = null;
        }
        request.setContentType(Operation.MEDIA_TYPE_APPLICATION_JSON).fail(e, r);
    }

    private void queueOrScheduleRequest(Service s, Operation op) {
        boolean processRequest = true;
        try {

            if (applyRequestRateLimit(op)) {
                processRequest = false;
                return;
            }

            ProcessingStage stage = s.getProcessingStage();
            if (stage == ProcessingStage.AVAILABLE) {
                return;
            }

            if (op.getAction() == Action.DELETE) {
                return;
            }

            if (stage == ProcessingStage.PAUSED) {
                if (checkAndResumePausedService(op)) {
                    processRequest = false;
                    return;
                }
            }

            op.fail(new CancellationException("Service not available, in stage:" + stage));
            processRequest = false;
        } finally {
            if (!processRequest) {
                return;
            }

            if (!s.queueRequest(op)) {
                this.executor.execute(() -> {
                    if (!s.hasOption(ServiceOption.CONCURRENT_UPDATE_HANDLING)) {
                        OperationContext.setContextId(op.getContextId());
                    }

                    OperationContext.setAuthorizationContext(op.getAuthorizationContext());

                    try {
                        s.handleRequest(op);
                    } catch (Throwable e) {
                        handleUncaughtException(s, op, e);
                    }

                    OperationContext.setAuthorizationContext(null);

                    if (op.getContextId() != null
                            && !s.hasOption(ServiceOption.CONCURRENT_UPDATE_HANDLING)) {
                        OperationContext.setContextId(null);
                    }
                });
            }
        }
    }

    private boolean applyRequestRateLimit(Operation op) {
        if (this.state.requestRateLimits.isEmpty()) {
            return false;
        }

        AuthorizationContext authCtx = op.getAuthorizationContext();
        if (authCtx == null) {
            return false;
        }

        Claims claims = authCtx.getClaims();
        if (claims == null) {
            return false;
        }

        String subject = claims.getSubject();
        if (subject == null) {
            return false;
        }

        // TODO: use the roles that applied during authorization as the rate limiting key.
        // We currently just use the subject but this is going to change.
        RequestRateInfo rateInfo = this.state.requestRateLimits.get(subject);
        if (rateInfo == null) {
            return false;
        }

        double count = rateInfo.count.incrementAndGet();
        long now = Utils.getNowMicrosUtc();
        long delta = now - rateInfo.startTimeMicros;
        double deltaInSeconds = delta / 1000000.0;
        if (delta < getMaintenanceIntervalMicros()) {
            return false;
        }

        double requestsPerSec = count / deltaInSeconds;
        if (requestsPerSec > rateInfo.limit) {
            this.failRequestLimitExceeded(op);
            return true;
        }

        return false;
    }

    private void handleUncaughtException(Service s, Operation op, Throwable e) {
        if (!Utils.isValidationError(e)) {
            log(Level.SEVERE, "Uncaught exception in service %s: %s", s.getUri(),
                    Utils.toString(e));
        } else if (this.logger.isLoggable(Level.FINE)) {
            log(Level.FINE, "Validation Error in service %s: %s", s.getUri(), Utils.toString(e));
        }
        op.fail(e);
    }

    public void sendRequest(Operation op) {
        prepareRequest(op);
        traceOperation(op);

        if (this.isStopping()) {
            op.fail(new CancellationException("host is stopping"));
            return;
        }

        ServiceClient c = this.client;
        if (c == null) {
            op.fail(new CancellationException("host is stopped"));
            return;
        }

        c.send(op);
    }

    private void traceOperation(Operation op) {
        // Post to operation tracing service if tracing is enabled.
        if (this.getOperationTracingLevel().intValue() == Level.OFF.intValue()) {
            return;
        }

        if (this.state.operationTracingLinkExclusionList.contains(op.getUri().getPath())) {
            return;
        }

        Operation.SerializedOperation tracingOp = Operation.SerializedOperation.create(op);
        sendRequest(Operation.createPost(UriUtils.buildUri(this, OperationIndexService.class))
                .setReferer(getUri())
                .setBodyNoCloning(tracingOp));
    }

    private void prepareRequest(Operation op) {
        if (op.getUri() == null) {
            throw new IllegalArgumentException("URI is required");
        }

        if (op.getUri().getPort() != this.state.httpPort) {
            // force communication between hosts in the same process to go
            // through sockets. It is less optimal but in production we do not
            // expect multiple hosts per process. In tests, we do expect
            // multiple hosts but they goal is to simulate cross machine or
            // cross process communication
            op.forceRemote();
        }
        if (op.getExpirationMicrosUtc() == 0) {
            op.setExpiration(Utils.getNowMicrosUtc() + this.state.operationTimeoutMicros);
        }

        if (op.getCompletion() == null) {
            op.setCompletion((o, e) -> {
                if (e != null) {
                    log(Level.WARNING, "Operation to %s failed: %s", o.getUri(), e.getMessage());
                }
            });
        }
        // TODO Set default expiration on all out bound operations and track
        // them during scheduled maintenance
    }

    /**
     * See {@code ServiceClient} for details. Sends a request using a callback pattern
     */
    public void sendRequestWithCallback(Operation op) {
        this.client.sendWithCallback(op);
    }

    /**
     * Synchronously stops the host and all services attached. Each service is stopped in parallel
     * and a brief expiration window is set allowing it to complete any shutdown tasks
     */
    public void stop() {

        Set<Service> servicesToClose = null;
        synchronized (this.state) {
            if (!this.state.isStarted || this.state.isStopping) {
                return;
            }
            this.state.isStopping = true;
            servicesToClose = new HashSet<Service>(
                    this.attachedServices.values());

            this.pendingPauseServices.clear();
        }

        stopAndClearPendingQueues();

        ScheduledFuture<?> task = this.maintenanceTask;
        if (task != null) {
            task.cancel(false);
            this.maintenanceTask = null;
        }

        int servicesToCloseCount = servicesToClose.size()
                - this.coreServices.size();
        final CountDownLatch latch = new CountDownLatch(servicesToCloseCount);

        final Operation.CompletionHandler removeServiceCompletion = (o, e) -> {
            this.attachedServices.remove(o.getUri().getPath());
            latch.countDown();
        };

        // first shut down non core services: During their stop processing they
        // might still rely on core services
        for (final Service s : servicesToClose) {
            if (this.coreServices.contains(s.getSelfLink())) {
                // we stop core services last
                continue;
            }
            sendServiceStop(removeServiceCompletion, s);
        }

        log(Level.INFO, "Waiting for DELETE from %d services", servicesToCloseCount);
        waitForServiceStop(latch);
        log(Level.INFO, "All non core services stopped", servicesToCloseCount);

        int coreServiceCount = this.coreServices.size();
        final CountDownLatch cLatch = new CountDownLatch(coreServiceCount);
        final Operation.CompletionHandler c = (o, e) -> {
            cLatch.countDown();
        };

        // now do core service shutdown in parallel
        for (String coreServiceLink : this.coreServices) {
            Service coreService = this.attachedServices.get(coreServiceLink);
            if (coreService == null || coreService instanceof ServiceHostManagementService) {
                // a DELETE to the management service will cause a recursive stop()
                c.handle(null, null);
                continue;
            }
            sendServiceStop(c, coreService);
        }

        log(Level.INFO, "Waiting for DELETE from %d core services", coreServiceCount);
        this.coreServices.clear();
        waitForServiceStop(cLatch);
        log(Level.INFO, "All core services stopped");

        this.attachedServices.clear();
        this.maintenanceHelper.close();
        this.state.isStarted = false;

        removeLogging();

        // listener will implicitly shutdown the executor (which is shared for both I/O dispatching
        // and internal dispatching), so stop it last
        try {
            this.httpListener.stop();
            this.httpListener = null;
            if (this.httpsListener != null) {
                this.httpsListener.stop();
                this.httpsListener = null;
            }
        } catch (Throwable e1) {
        }

        try {
            this.client.stop();
            this.client = null;
        } catch (Throwable e1) {
        }

        this.executor.shutdownNow();
        this.scheduledExecutor.shutdownNow();
    }

    private void stopAndClearPendingQueues() {
        for (Operation op : this.pendingOperationsForRetry) {
            op.fail(new CancellationException());
        }
        this.pendingOperationsForRetry.clear();

        for (Operation op : this.pendingStartOperations) {
            op.fail(new CancellationException());
        }
        this.pendingStartOperations.clear();

        for (SortedSet<Operation> opSet : this.pendingServiceAvailableCompletions.values()) {
            for (Operation op : opSet) {
                op.fail(new CancellationException());
            }
        }
        this.pendingServiceAvailableCompletions.clear();
    }

    private void waitForServiceStop(final CountDownLatch latch) {
        try {
            boolean isTimeout = !latch.await(this.state.maintenanceIntervalMicros * 5,
                    TimeUnit.MICROSECONDS);

            if (isTimeout) {
                log(Level.INFO, "Timeout waiting for service stop");
                for (String l : this.attachedServices.keySet()) {
                    if (this.coreServices.contains(l)) {
                        continue;
                    }
                    log(Level.WARNING, "%s did not complete DELETE", l);
                }
            }
        } catch (Throwable e) {
            log(Level.INFO, "%s", e.toString());
        }
    }

    private void sendServiceStop(final CompletionHandler removeServiceCompletion,
            final Service s) {
        Operation delete = Operation.createDelete(s.getUri())
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)
                .setReplicationDisabled(true)
                .setCompletion(removeServiceCompletion)
                .setReferer(UriUtils.buildUri(this, ""));
        try {
            queueOrScheduleRequest(s, delete);
        } catch (Throwable e) {
            log(Level.WARNING, Utils.toString(e));
            removeServiceCompletion.handle(delete, e);
        }
    }

    public static boolean isHelperServicePath(String serviceUriPath) {
        if (serviceUriPath.endsWith(SERVICE_URI_SUFFIX_REPLICATION)) {
            return true;
        } else if (serviceUriPath.endsWith(SERVICE_URI_SUFFIX_STATS)) {
            return true;
        } else if (serviceUriPath.endsWith(SERVICE_URI_SUFFIX_CONFIG)) {
            return true;
        } else if (serviceUriPath.endsWith(SERVICE_URI_SUFFIX_SUBSCRIPTIONS)) {
            return true;
        } else if (serviceUriPath.endsWith(SERVICE_URI_SUFFIX_TEMPLATE)) {
            return true;
        } else if (serviceUriPath.endsWith(SERVICE_URI_SUFFIX_UI)) {
            return true;
        }
        return false;
    }

    /**
     * Configures host logging and behavior to ease debugging
     *
     * @param enable
     * @return
     */
    public ServiceHost toggleDebuggingMode(boolean enable) {
        Level newLevel = enable ? Level.FINE : Level.INFO;
        setLoggingLevel(newLevel);
        // increase operation timeout
        this.setOperationTimeOutMicros(enable ? TimeUnit.MINUTES.toMicros(10)
                : ServiceHostState.DEFAULT_OPERATION_TIMEOUT_MICROS);
        return this;
    }

    public ServiceHost setLoggingLevel(Level newLevel) {
        this.logger.setLevel(newLevel);
        for (java.util.logging.Handler h : this.logger.getParent().getHandlers()) {
            h.setLevel(newLevel);
        }
        return this;
    }

    public ServiceHost setOperationTracingLevel(Level newLevel) {
        this.state.operationTracingLevel = newLevel.toString();
        return this;
    }

    public Level getOperationTracingLevel() {
        return this.state.operationTracingLevel == null ? Level.OFF
                : Level.parse(this.state.operationTracingLevel);
    }

    public void log(Level level, String fmt, Object... args) {
        log(level, 3, fmt, args);
    }

    protected void log(Level level, Integer nestingLevel, String fmt, Object... args) {
        if (this.logPrefix == null) {
            this.logPrefix = getUri().toString();
        }
        Utils.log(this.logger, nestingLevel, this.logPrefix, level, fmt, args);
    }

    /**
     * Registers a completion that is invoked every time one of the supplied services reaches the
     * available stage. If service start fails for any one, the completion will be called with a
     * failure argument.
     *
     * Note that supplying multiple self links will result in multiple completion invocations. The
     * handler provided must track how many times it has been called
     */
    public void registerForServiceAvailability(
            CompletionHandler completion, String... servicePaths) {
        if (servicePaths == null || servicePaths.length == 0) {
            throw new IllegalArgumentException("selfLinks are required");
        }
        Operation op = Operation.createPost(null)
                .setCompletion(completion)
                .setExpiration(getOperationTimeoutMicros() + Utils.getNowMicrosUtc());
        registerForServiceAvailability(op, servicePaths);
    }

    void registerForServiceAvailability(
            Operation opTemplate, String... servicePaths) {
        final boolean doOpClone = servicePaths.length > 1;
        // clone client supplied array since this method mutates it
        final String[] clonedLinks = Arrays.copyOf(servicePaths, servicePaths.length);

        synchronized (this.state) {
            for (int i = 0; i < clonedLinks.length; i++) {
                String link = clonedLinks[i];
                Service s = findService(link);
                if (s != null && s.getProcessingStage() == Service.ProcessingStage.AVAILABLE) {
                    continue;
                }
                SortedSet<Operation> pendingOps = this.pendingServiceAvailableCompletions
                        .get(link);
                if (pendingOps == null) {
                    // create sorted set using the operation expiration as the key. This allows us
                    // to efficiently detect expiration during host maintenance
                    pendingOps = createOperationSet();
                    this.pendingServiceAvailableCompletions.put(link, pendingOps);
                }
                pendingOps.add(doOpClone ? opTemplate.clone() : opTemplate);
                // null the link so we do not attempt to invoke the completion below
                clonedLinks[i] = null;
            }
        }

        for (String link : clonedLinks) {
            if (link == null) {
                continue;
            }

            run(() -> {
                Operation o = opTemplate;
                if (doOpClone) {
                    o = opTemplate.clone().setUri(UriUtils.buildUri(this, link));
                }
                if (o.getUri() == null) {
                    o.setUri(UriUtils.buildUri(this, link));
                }
                o.complete();
            });
        }
    }

    /**
     * Infrastructure use only.
     *
     * Sets an upper limit, in terms of operations per second, for all operations
     * associated with some context. The context is (tenant, user, referrer) is used
     * to derive the key.
     */
    public ServiceHost setRequestRateLimit(String key, double operationsPerSecond) {
        RequestRateInfo ri = new RequestRateInfo();
        ri.limit = operationsPerSecond;
        ri.startTimeMicros = Utils.getNowMicrosUtc();
        this.state.requestRateLimits.put(key, ri);
        return this;
    }

    /**
     * Set a relative memory limit for a given service.
     */
    public ServiceHost setServiceMemoryLimit(String servicePath, double percentOfTotal) {
        if (servicePath == null) {
            throw new IllegalArgumentException("servicePath is required");
        }

        if (!servicePath.equals(ROOT_PATH) && isStarted()) {
            throw new IllegalStateException(
                    "Service memory limit can only be changed before host start");
        }

        if (percentOfTotal >= 1.0 || percentOfTotal <= 0.0) {
            throw new IllegalArgumentException(
                    "percentOfTotal must be within 0.0 and 1.0 exclusive");
        }

        double total = percentOfTotal;
        for (Map.Entry<String, Double> e : this.state.relativeMemoryLimits.entrySet()) {
            if (!e.getKey().equals(servicePath)) {
                total += e.getValue();
            }
        }

        if (total >= 1.0) {
            throw new IllegalStateException("Total memory limit, across all services exceeds 1.0: "
                    + Utils.toJsonHtml(this.state.relativeMemoryLimits));
        }
        this.state.relativeMemoryLimits.put(servicePath, percentOfTotal);
        return this;
    }

    /**
     * Retrieves the memory limit, in MB for a given service path
     */
    public Long getServiceMemoryLimitMB(String servicePath, MemoryLimitType limitType) {
        Double limitAsPercentTotalMemory = this.state.relativeMemoryLimits.get(servicePath);
        if (limitAsPercentTotalMemory == null) {
            return null;
        }
        long maxMemoryMB = Runtime.getRuntime().maxMemory();
        maxMemoryMB /= 1024 * 1024;
        long exactLimitMB = (long) (maxMemoryMB * limitAsPercentTotalMemory);

        switch (limitType) {
        case LOW_WATERMARK:
            return exactLimitMB / 4;
        case HIGH_WATERMARK:
            return (exactLimitMB * 3) / 4;
        case EXACT:
            // intentional fall through
        default:
            return exactLimitMB;
        }
    }

    public ProcessingStage getServiceStage(String servicePath) {
        Service s = this.findService(servicePath);
        if (s == null) {
            return null;
        }
        return s.getProcessingStage();
    }

    public boolean checkServiceAvailable(String servicePath) {
        Service s = this.findService(servicePath);
        if (s == null) {
            return false;
        }
        return s.getProcessingStage() == ProcessingStage.AVAILABLE;
    }

    public SystemHostInfo getSystemInfo() {
        if (!this.info.properties.isEmpty() && !this.info.ipAddresses.isEmpty()) {
            return Utils.clone(this.info);
        }
        return updateSystemInfo(true);
    }

    public SystemHostInfo updateSystemInfo(boolean enumerateNetworkInterfaces) {
        Runtime r = Runtime.getRuntime();
        this.info.availableProcessorCount = r.availableProcessors();
        this.info.freeMemoryByteCount = r.freeMemory();
        this.info.totalMemoryByteCount = r.totalMemory();
        this.info.maxMemoryByteCount = r.maxMemory();

        this.info.osName = Utils.getOsName(this.info);
        this.info.osFamily = Utils.determineOsFamily(this.info.osName);

        try {
            URI sandbox = getStorageSandbox();
            if (sandbox == null) {
                throw new RuntimeException("Sandbox not set");
            }
            File f = new File(sandbox);
            this.info.freeDiskByteCount = f.getFreeSpace();
            this.info.usableDiskByteCount = f.getUsableSpace();
            this.info.totalDiskByteCount = f.getTotalSpace();
        } catch (Throwable e) {
            log(Level.WARNING, "Exception getting disk usage: %s", Utils.toString(e));
        }

        for (Entry<Object, Object> e : System.getProperties().entrySet()) {
            String k = e.getKey().toString();
            String v = e.getValue().toString();
            this.info.properties.put(k, v);
        }

        for (Entry<String, String> e : System.getenv().entrySet()) {
            this.info.environmentVariables.put(e.getKey(), e.getValue());
        }

        if (!enumerateNetworkInterfaces) {
            return Utils.clone(this.info);
        }

        List<String> ipAddresses = new ArrayList<>();
        ipAddresses.add(LOOPBACK_ADDRESS);

        try {
            Enumeration<NetworkInterface> niEnum = NetworkInterface
                    .getNetworkInterfaces();
            while (niEnum.hasMoreElements()) {
                NetworkInterface ni = niEnum.nextElement();
                if (ni.isLoopback()) {
                    continue;
                }
                if (ni.isPointToPoint()) {
                    continue;
                }
                if (!ni.isUp()) {
                    continue;
                }
                Enumeration<InetAddress> e = ni.getInetAddresses();
                while (e.hasMoreElements()) {
                    InetAddress addr = e.nextElement();
                    String host = Utils.getNormalizedHostAddress(this.info, addr);
                    ipAddresses.add(host);
                }
            }

            Collections.reverse(ipAddresses);

            if (this.state.bindAddress != null
                    && !ServiceHost.ALL_INTERFACES.equals(this.state.bindAddress)) {
                ipAddresses.remove(this.state.bindAddress);
                // always put bind address at index 0 so its the preferred address
                ipAddresses.add(0, this.state.bindAddress);
            }

            this.info.ipAddresses = ipAddresses;
        } catch (Throwable e) {
            log(Level.SEVERE, "Failure: %s", Utils.toString(e));
        }

        if (this.info.ipAddresses.isEmpty()) {
            log(Level.WARNING, "No IP or network interfaces detected. Adding loopback address");
            this.info.ipAddresses.add(ServiceHost.LOOPBACK_ADDRESS);
        }

        return Utils.clone(this.info);
    }

    private boolean checkAndSetPreferredAddress(String address) {
        address = normalizeAddress(address);
        List<String> ipAddresses = new ArrayList<>(this.info.ipAddresses);
        for (int i = 0; i < ipAddresses.size(); i++) {
            if (!address.equals(ipAddresses.get(i))) {
                continue;
            }

            // set the supplied address as the preferred address, in index 0
            if (i == 0) {
                break;
            }

            // swap with address at index 0
            String oldPreferred = ipAddresses.get(0);
            ipAddresses.set(i, oldPreferred);
            ipAddresses.set(0, address);
            log(Level.INFO, "Swapped preferred address to %s from %s", address, oldPreferred);
            this.info.ipAddresses = ipAddresses;
            this.cachedUri = null;
            return true;
        }

        return address.equals(ipAddresses.get(0));
    }

    private String normalizeAddress(String address) {
        if (address.length() > 2 && address.startsWith("[") && address.endsWith("]")) {
            return address.substring(1, address.length() - 1);
        } else {
            return address;
        }
    }

    public void run(Runnable task) {
        if (this.executor.isShutdown()) {
            throw new IllegalStateException("Stopped");
        }
        AuthorizationContext origContext = OperationContext.getAuthorizationContext();
        this.executor.execute(() -> {
            OperationContext.setAuthorizationContext(origContext);
            executeRunnableSafe(task);
        });
    }

    public ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
        if (this.isStopping()) {
            throw new IllegalStateException("Stopped");
        }
        if (this.scheduledExecutor.isShutdown()) {
            throw new IllegalStateException("Stopped");
        }

        AuthorizationContext origContext = OperationContext.getAuthorizationContext();
        return this.scheduledExecutor.schedule(() -> {
            OperationContext.setAuthorizationContext(origContext);
            executeRunnableSafe(task);
        } , delay, unit);
    }

    private void executeRunnableSafe(Runnable task) {
        try {
            task.run();
        } catch (Throwable e) {
            log(Level.SEVERE, "Unhandled exception executing task: %s", Utils.toString(e));
        }
    }

    private enum MaintenanceStage {
        UTILS, MEMORY, IO, NODE_SELECTORS, SERVICE
    }

    /**
     * Initiates host periodic maintenance cycle
     */
    private void scheduleMaintenance() {
        Runnable r = () -> {
            this.state.lastMaintenanceTimeUtcMicros = Utils.getNowMicrosUtc();
            performMaintenanceStage(Operation.createPost(getUri()),
                    MaintenanceStage.UTILS);
        };

        this.maintenanceTask = schedule(r, getMaintenanceIntervalMicros(), TimeUnit.MICROSECONDS);
    }


    /**
     * Performs maintenance tasks for the given stage. Only a single instance of this
     * state machine must be active per host, at any time. Maintenance is re-scheduled
     * when the final stage is complete.
     */
    private void performMaintenanceStage(Operation post, MaintenanceStage stage) {

        try {
            long now = Utils.getNowMicrosUtc();
            long deadline = this.state.lastMaintenanceTimeUtcMicros
                    + this.state.maintenanceIntervalMicros;

            switch (stage) {
            case UTILS:
                Utils.performMaintenance();
                stage = MaintenanceStage.MEMORY;
                break;
            case MEMORY:
                applyMemoryLimit(deadline);
                stage = MaintenanceStage.IO;
                break;
            case IO:
                performIOMaintenance(post, now, MaintenanceStage.NODE_SELECTORS);
                return;
            case NODE_SELECTORS:
                performNodeSelectorChangeMaintenance();
                stage = MaintenanceStage.SERVICE;
                break;
            case SERVICE:
                this.maintenanceHelper.performMaintenance(post, deadline);
                stage = null;
                break;
            default:
                stage = null;
                break;
            }

            if (stage == null) {
                post.complete();
                scheduleMaintenance();
                return;
            }
            performMaintenanceStage(post, stage);
        } catch (Throwable e) {
            log(Level.SEVERE, "Uncaught exception: %s", Utils.toString(e));
            post.fail(e);
        }
    }

    private void performIOMaintenance(Operation post, long now, MaintenanceStage nextStage) {
        try {
            performPendingOperationMaintenance();

            // reset request limits, start new time window
            for (RequestRateInfo rri : this.state.requestRateLimits.values()) {
                if (now - rri.startTimeMicros < ONE_MINUTE_IN_MICROS) {
                    // reset only after a fixed interval
                    return;
                }
                rri.startTimeMicros = now;
                rri.count.set(0);
            }

            int expected = 0;
            ServiceClient c = getClient();
            if (c != null) {
                expected++;
            }
            ServiceRequestListener l = getListener();
            if (l != null) {
                expected++;
            }
            ServiceRequestListener sl = getSecureListener();
            if (sl != null) {
                expected++;
            }

            AtomicInteger pending = new AtomicInteger(expected);
            CompletionHandler ch = ((o, e) -> {
                int r = pending.decrementAndGet();
                if (r != 0) {
                    return;
                }
                performMaintenanceStage(post, nextStage);
            });

            if (c != null) {
                c.handleMaintenance(Operation.createPost(null).setCompletion(ch));
            }

            if (l != null) {
                l.handleMaintenance(Operation.createPost(null).setCompletion(ch));
            }

            if (sl != null) {
                sl.handleMaintenance(Operation.createPost(null).setCompletion(ch));
            }
        } catch (Throwable e) {
            log(Level.WARNING, "Exception: %s", Utils.toString(e));
            performMaintenanceStage(post, nextStage);
        }
    }

    private void performPendingOperationMaintenance() {
        long now = Utils.getNowMicrosUtc();
        Iterator<Operation> startOpsIt = this.pendingStartOperations.iterator();
        checkOperationExpiration(now, startOpsIt);

        for (SortedSet<Operation> ops : this.pendingServiceAvailableCompletions.values()) {
            Iterator<Operation> it = ops.iterator();
            checkOperationExpiration(now, it);
        }

        Iterator<Operation> it = this.pendingOperationsForRetry.iterator();
        while (it.hasNext()) {
            Operation o = it.next();
            if (isStopping()) {
                o.fail(new CancellationException());
                return;
            }
            it.remove();
            handleRequest(null, o);
        }
    }

    private void performNodeSelectorChangeMaintenance() {

        Iterator<String> it = this.pendingNodeSelectorsForFactorySynch.iterator();
        while (it.hasNext()) {
            String selectorPath = it.next();
            it.remove();
            performNodeSelectorChangeMaintenance(selectorPath);
        }
    }

    private void performNodeSelectorChangeMaintenance(String nodeSelectorPath) {
        for (Service s : this.attachedServices.values()) {
            if (isStopping()) {
                return;
            }

            if (!s.hasOption(ServiceOption.FACTORY)) {
                continue;
            }

            if (!s.hasOption(ServiceOption.REPLICATION)) {
                continue;
            }

            String serviceSelectorPath = s.getPeerNodeSelectorPath();
            if (!nodeSelectorPath.equals(serviceSelectorPath)) {
                continue;
            }

            Operation maintOp = Operation.createPost(s.getUri()).setCompletion((o, e) -> {
                if (e != null) {
                    log(Level.WARNING, "Node group change maintenance failed for %s: %s",
                            s.getSelfLink(),
                            e.getMessage());
                }

                log(Level.FINE, "Node group change maintenance done for service %s, group %s",
                        nodeSelectorPath, s.getSelfLink());
                s.adjustStat(Service.STAT_NAME_NODE_GROUP_CHANGE_PENDING_MAINTENANCE_COUNT, -1);

            });

            ServiceMaintenanceRequest body = ServiceMaintenanceRequest.create();
            body.reasons.add(MaintenanceReason.NODE_GROUP_CHANGE);
            maintOp.setBodyNoCloning(body);

            s.adjustStat(Service.STAT_NAME_NODE_GROUP_CHANGE_PENDING_MAINTENANCE_COUNT, 1);

            // allow overlapping node group change maintenance requests
            this.run(() -> {
                OperationContext.setAuthorizationContext(this.getSystemAuthorizationContext());
                s.adjustStat(Service.STAT_NAME_NODE_GROUP_CHANGE_MAINTENANCE_COUNT, 1);
                s.handleMaintenance(maintOp);
            });
        }
    }

    private void checkOperationExpiration(long now, Iterator<Operation> iterator) {
        while (iterator.hasNext()) {
            Operation op = iterator.next();
            if (op == null || op.getExpirationMicrosUtc() > now) {
                // not expired, and since we walk in ascending order, no other operations
                // are expired
                break;
            }
            iterator.remove();
            run(() -> op.fail(new TimeoutException(op.toString())));
        }
    }

    /**
     * Estimates how much memory is used by host caches, queues and based on the memory limits
     * takes appropriate action: clears cached service state, temporarily stops services
     */
    private void applyMemoryLimit(long deadlineMicros) {
        long memoryLimitLowMB = getServiceMemoryLimitMB(ROOT_PATH,
                MemoryLimitType.HIGH_WATERMARK);

        long memoryInUseMB = this.state.serviceCount * DEFAULT_SERVICE_INSTANCE_COST_BYTES;
        memoryInUseMB /= (1024 * 1024);

        if (memoryLimitLowMB > memoryInUseMB) {
            return;
        }

        int pauseServiceCount = 0;
        for (Service service : this.attachedServices.values()) {
            ServiceDocument s = this.cachedServiceStates.get(service.getSelfLink());

            // explicitly check if its a factory since a factory service will inherit service options from its
            // child services, and will appears as indexed
            if (service.hasOption(ServiceOption.FACTORY)) {
                continue;
            }

            if (!isServiceIndexed(service)) {
                // we do not clear cache or stop in memory services
                continue;
            }

            // TODO Optimize with a sorted set based on last update time.
            // Skip services and state documents that have been active within the last maintenance interval
            if (s != null
                    && this.state.lastMaintenanceTimeUtcMicros
                            - s.documentUpdateTimeMicros < service
                                    .getMaintenanceIntervalMicros() * 2) {
                continue;
            }

            // remove cached state
            if (s != null) {
                clearCachedServiceState(service.getSelfLink());
            }

            // we still want to clear a cache for periodic services, so check here, after the cache clear
            if (service.hasOption(ServiceOption.PERIODIC_MAINTENANCE)) {
                // Services with periodic maintenance stay resident, for now. We might stop them in the future
                // if they have long periods
                continue;
            }

            if (!service.hasOption(ServiceOption.FACTORY_ITEM)) {
                continue;
            }

            if (isServiceStarting(service, service.getSelfLink())) {
                continue;
            }

            Service existing = this.pendingPauseServices.put(service.getSelfLink(), service);
            if (existing == null) {
                pauseServiceCount++;
            }

            String factoryPath = UriUtils.getParentPath(service.getSelfLink());
            if (factoryPath != null) {
                this.serviceFactoriesUnderMemoryPressure.add(factoryPath);
            }

            if (deadlineMicros < Utils.getNowMicrosUtc()) {
                break;
            }
        }

        if (pauseServiceCount == 0) {
            return;
        }

        // Make sure our service count matches the list contents, they could drift. Using size()
        // on a concurrent data structure is costly so we do this only when pausing services
        synchronized (this.state) {
            this.state.serviceCount = this.attachedServices.size();
        }

        // schedule a task to actually stop the services. If a request arrives in the mean time,
        // it will remove the service from the pendingStopService map (since its active).
        schedule(() -> {
            pauseServices();
        } , getMaintenanceIntervalMicros(), TimeUnit.MICROSECONDS);
    }

    boolean checkAndResumePausedService(Operation inboundOp) {
        String key = inboundOp.getUri().getPath();
        if (isHelperServicePath(key)) {
            key = UriUtils.getParentPath(key);
        }

        String factoryPath = UriUtils.getParentPath(key);
        if (factoryPath != null
                && !this.serviceFactoriesUnderMemoryPressure.contains(factoryPath)) {
            // minor optimization: if the service factory has never experienced a pause for one of the child
            // services, do not bother querying the blob index. A node might never come under memory
            // pressure so this lookup avoids the index query.
            return false;
        }

        String path = key;
        if (inboundOp.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_QUEUING)) {
            return false;
        }

        if (inboundOp.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK)) {
            Service service = this.pendingPauseServices.remove(key);
            if (service != null) {
                // Abort pause
                log(Level.INFO, "Service %s in the process of pausing", path);
                resumeService(path, service);
                return false;
            }

            if (inboundOp.getExpirationMicrosUtc() < Utils.getNowMicrosUtc()) {
                log(Level.WARNING, "Request to %s has expired", path);
                return false;
            }

            if (isStopping()) {
                return false;
            }

            inboundOp.removePragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK);

            long pendingPauseCount = this.pendingPauseServices.size();
            if (pendingPauseCount == 0) {
                // there is nothing pending and the service index did not have a paused service
                return false;
            }

            // there is a small window between pausing a service, and the service being indexed in the
            // blob store, where an operation coming in might find the service missing from the blob index and from
            // attachedServices map.
            schedule(
                    () -> {
                        log(Level.INFO,
                                "Retrying index lookup for %s, pending pause: %d",
                                path, pendingPauseCount);
                        checkAndResumePausedService(inboundOp);
                    } , 1, TimeUnit.SECONDS);
            return true;
        }

        inboundOp.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK);

        Operation query = ServiceContextIndexService
                .createGet(this, path)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                log(Level.WARNING,
                                        "Failure checking if service paused: " + Utils.toString(e));
                                handleRequest(null, inboundOp);
                                return;
                            }

                            if (!o.hasBody()) {
                                log(Level.INFO, "%s not paused", path);
                                // service is not paused
                                handleRequest(null, inboundOp);
                                return;
                            }

                            Service resumedService = (Service) o.getBodyRaw();
                            resumeService(path, resumedService);
                            handleRequest(null, inboundOp);
                        });

        sendRequest(query.setReferer(getUri()));
        return true;
    }

    private void resumeService(String path, Service resumedService) {
        if (isStopping()) {
            return;
        }
        resumedService.setHost(this);
        resumedService.setProcessingStage(ProcessingStage.AVAILABLE);
        synchronized (this.state) {
            if (!this.attachedServices.containsKey(path)) {
                this.attachedServices.put(path, resumedService);
                this.state.serviceCount++;
            }
        }
    }

    private void pauseServices() {
        if (isStopping()) {
            return;
        }

        int servicePauseCount = 0;
        for (Service s : this.pendingPauseServices.values()) {
            if (s.getProcessingStage() != ProcessingStage.AVAILABLE) {
                continue;
            }

            s.setProcessingStage(ProcessingStage.PAUSED);
            servicePauseCount++;
            String path = s.getSelfLink();

            // ask object index to store service object. It should be tiny since services
            // should hold no instanced fields. We avoid service stop/start by doing this
            sendRequest(ServiceContextIndexService.createPost(this, path, s)
                    .setReferer(getUri()).setCompletion((o, e) -> {
                        if (e != null && !this.isStopping()) {
                            log(Level.WARNING, "Failure indexing service for pause: %s",
                                    Utils.toString(e));
                            resumeService(path, s);
                            return;
                        }

                        Service serviceEntry = this.pendingPauseServices.remove(path);
                        if (serviceEntry == null) {
                            log(Level.INFO, "aborting pause for %s", path);
                            resumeService(path, s);
                            // this means service received a request and is active. Its OK, the index will have
                            // a stale entry that will get deleted next time we query for this self link.
                            notifyServiceAvailabilitySubscribers(s);
                            return;
                        }

                        synchronized (this.state) {
                            if (null != this.attachedServices.remove(path)) {
                                this.state.serviceCount--;
                            }
                        }
                    }));
        }
        log(Level.INFO, "Paused %d services, attached: %d", servicePauseCount,
                this.state.serviceCount);
    }

    private ServiceDocument getCachedServiceState(String servicePath) {
        ServiceDocument state = this.cachedServiceStates.get(servicePath);
        if (state == null) {
            return null;
        }

        // Check if this is expired and, if so, remove it from cache.
        if (state.documentExpirationTimeMicros > 0 &&
                state.documentExpirationTimeMicros < Utils.getNowMicrosUtc()) {
            clearCachedServiceState(servicePath);
            return null;
        }

        return state;
    }

    private void clearCachedServiceState(String servicePath) {
        this.cachedServiceStates.remove(servicePath);
    }

    public ServiceHost setOperationTimeOutMicros(long timeoutMicros) {
        this.state.operationTimeoutMicros = timeoutMicros;
        return this;
    }

    public ServiceHost setProcessOwner(boolean isOwner) {
        this.state.isProcessOwner = isOwner;
        return this;
    }

    public boolean isProcessOwner() {
        return this.state.isProcessOwner;
    }

    public void setListener(ServiceRequestListener listener) {
        if (isStarted() || this.httpListener != null) {
            throw new IllegalStateException("Already started");
        }
        this.httpListener = listener;
    }

    public ServiceRequestListener getSecureListener() {
        return this.httpsListener;
    }

    public void setSecureListener(ServiceRequestListener listener) {
        if (isStarted() || this.httpsListener != null) {
            throw new IllegalStateException("Already started");
        }
        this.httpsListener = listener;
    }

    public ServiceRequestListener getListener() {
        return this.httpListener;
    }

    public ServiceClient getClient() {
        return this.client;
    }

    public void setClient(ServiceClient client) {
        this.client = client;
    }

    public long getMaintenanceIntervalMicros() {
        return this.state.maintenanceIntervalMicros;
    }

    void saveServiceState(Service s, Operation op, ServiceDocument state) {
        if (state == null) {
            op.fail(new IllegalArgumentException("linkedState is required"));
            return;
        }

        // If this request doesn't originate from replication (which might happen asynchronously, i.e. through
        // (re-)synchronization after a node group change), don't update the documentAuthPrincipalLink because
        // it will be set to the system user. The specified state is expected to have the documentAuthPrincipalLink
        // set from when it was first saved.
        if (!op.isFromReplication()) {
            state.documentAuthPrincipalLink = (op.getAuthorizationContext() != null)
                    ? op.getAuthorizationContext().getClaims().getSubject() : null;
        }

        if (this.transactionService != null) {
            state.documentTransactionId = op.getTransactionId() == null ? "" : op
                    .getTransactionId();
        }
        state.documentUpdateAction = op.getAction().toString();

        if (!isServiceIndexed(s)) {
            cacheServiceState(s, state, op);
            op.complete();
            return;
        }

        URI u = getDocumentIndexServiceUri();

        // serialize state and compute signature. The index service will take
        // the serialized state and store as is, and it will index all fields
        // from the document instance, using the description for instructions
        UpdateIndexRequest body = new UpdateIndexRequest();
        body.document = state;
        // retrieve the description through the cached template so its the thread safe,
        // immutable version
        body.description = buildDocumentDescription(s);
        try {
            cacheServiceState(s, state, op);
        } catch (Throwable e1) {
            op.fail(e1);
            return;
        }

        Operation post = Operation.createPost(u)
                .setReferer(op.getReferer())
                .setBodyNoCloning(body)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        clearCachedServiceState(s.getSelfLink());
                        op.fail(e);
                        return;
                    }
                    op.complete();
                });
        sendRequest(post);
    }

    private NodeSelectorService findNodeSelectorService(String path,
            Operation request) {
        if (path == null) {
            path = ServiceUriPaths.DEFAULT_NODE_SELECTOR;
        }

        Service s = this.findService(path);
        if (s == null) {
            request.fail(new ServiceNotFoundException());
            return null;
        }
        if (!(s instanceof NodeSelectorService)) {
            String msg = String.format("path '%s' (%s) is not a node selector service",
                    path, s.getClass().getName());
            request.fail(new IllegalArgumentException(msg));
            return null;
        }
        NodeSelectorService nss = (NodeSelectorService) s;
        return nss;
    }

    public void broadcastRequest(String selectorPath, boolean excludeThisHost, Operation request) {
        broadcastRequest(selectorPath, null, excludeThisHost, request);
    }

    public void broadcastRequest(String selectorPath, String key, boolean excludeThisHost,
            Operation request) {
        if (isStopping()) {
            request.fail(new CancellationException());
            return;
        }

        if (selectorPath == null) {
            throw new IllegalArgumentException("selectorPath is required");
        }

        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }

        prepareRequest(request);

        NodeSelectorService nss = findNodeSelectorService(selectorPath, request);
        if (nss == null) {
            return;
        }

        SelectAndForwardRequest req = new SelectAndForwardRequest();
        req.options = EnumSet.of(ForwardingOption.BROADCAST);
        if (excludeThisHost) {
            req.options.add(ForwardingOption.EXCLUDE_ENTRY_NODE);
        }
        req.key = key;
        req.targetPath = request.getUri().getPath();
        req.targetQuery = request.getUri().getQuery();
        nss.selectAndForward(request, req);
    }

    /**
     * Convenience method that issues a {@code SelectOwnerRequest} to the node selector service. If
     * the supplied path is null the default selector will be used
     */
    public void selectOwner(String selectorPath, String key, Operation op) {
        if (isStopping()) {
            op.fail(new CancellationException());
            return;
        }

        SelectAndForwardRequest body = new SelectAndForwardRequest();
        body.key = key;

        NodeSelectorService nss = findNodeSelectorService(selectorPath, op);
        if (nss == null) {
            return;
        }

        nss.selectAndForward(op, body);
    }

    /**
     * Convenience method that forwards the supplied request to the node selected by hashing the
     * request URI path
     */
    public void forwardRequest(String groupPath, Operation request) {
        forwardRequest(groupPath, null, request);
    }

    /**
     * Convenience method that forwards the request to the node id that hashes closest to the key
     */
    public void forwardRequest(String selectorPath, String key, Operation request) {
        if (isStopping()) {
            request.fail(new CancellationException());
            return;
        }

        NodeSelectorService nss = findNodeSelectorService(selectorPath, request);
        if (nss == null) {
            return;
        }

        prepareRequest(request);

        SelectAndForwardRequest body = new SelectAndForwardRequest();
        body.targetPath = request.getUri().getPath();
        body.targetQuery = request.getUri().getQuery();
        body.key = key;
        body.options = EnumSet.of(ForwardingOption.UNICAST);
        nss.selectAndForward(request, body);
    }

    public void replicateRequest(EnumSet<ServiceOption> serviceOptions, ServiceDocument state,
            String selectorPath,
            String selectionKey,
            Operation op) {
        if (isStopping()) {
            op.fail(new CancellationException());
            return;
        }

        if (state == null) {
            op.fail(new IllegalStateException("state is required"));
            return;
        }

        NodeSelectorService nss = findNodeSelectorService(selectorPath, op);
        if (nss == null) {
            return;
        }

        state.documentOwner = getId();

        SelectAndForwardRequest req = new SelectAndForwardRequest();
        req.key = selectionKey;
        req.targetPath = op.getUri().getPath();
        req.targetQuery = op.getUri().getQuery();
        req.options = EnumSet.of(ForwardingOption.BROADCAST, ForwardingOption.REPLICATE);
        req.serviceOptions = serviceOptions;
        req.linkedState = state;
        nss.selectAndForward(op, req);
    }

    /**
     * Queries services in the AVAILABLE stage using a simple exact or prefix match on the supplied
     * self link
     */
    public void queryServiceUris(String servicePath, Operation get) {
        // TODO Use Radix trees for efficient prefix searches. This is not
        // urgent since we consider queries directly on the host instead of the
        // document index, to be rare

        ServiceDocumentQueryResult r = new ServiceDocumentQueryResult();

        boolean doPrefixMatch = servicePath.endsWith(UriUtils.URI_WILDCARD_CHAR);
        servicePath = servicePath.replace(UriUtils.URI_WILDCARD_CHAR, "");

        for (Service s : this.attachedServices.values()) {
            if (s.getProcessingStage() != ProcessingStage.AVAILABLE) {
                continue;
            }
            if (s.hasOption(ServiceOption.UTILITY)) {
                continue;
            }
            String path = s.getSelfLink();
            if (doPrefixMatch) {
                if (!path.startsWith(servicePath)) {
                    continue;
                }
            } else {
                if (!path.equals(servicePath)) {
                    continue;
                }
            }

            r.documentLinks.add(path);
        }
        r.documentOwner = getId();
        get.setBodyNoCloning(r).complete();
    }

    /**
     * Queries services in the AVAILABLE stage based on the provided options
     *
     * matchAllOptions = true : all options must match
     * matchAllOptions = false : any option must match
     */
    public void queryServiceUris(EnumSet<ServiceOption> options, boolean matchAllOptions,
            Operation get) {
        ServiceDocumentQueryResult r = new ServiceDocumentQueryResult();

        for (Service s : this.attachedServices.values()) {
            if (s.getProcessingStage() != ProcessingStage.AVAILABLE) {
                continue;
            }
            if (s.hasOption(ServiceOption.UTILITY)) {
                continue;
            }
            String servicePath = s.getSelfLink();

            if (matchAllOptions) {
                boolean hasAllOptions = true;

                for (ServiceOption option : options) {
                    if (option != null && !s.hasOption(option)) {
                        hasAllOptions = false;
                        break;
                    }
                }
                if (hasAllOptions) {
                    r.documentLinks.add(servicePath);
                }
            } else {
                for (ServiceOption option : options) {
                    if (option != null && s.hasOption(option)) {
                        r.documentLinks.add(servicePath);
                        break;
                    }
                }
            }
        }
        r.documentOwner = getId();
        get.setBodyNoCloning(r).complete();
    }

    /**
     * Infrastructure use only. Create service document description.
     */
    ServiceDocumentDescription buildDocumentDescription(String servicePath) {
        Service s = findService(servicePath);
        if (s == null) {
            return null;
        }
        return buildDocumentDescription(s);
    }

    /**
     * Infrastructure use only. Create service document description.
     *
     * Returns a cached service document description if it was created before.
     *
     * @param s {@link Service}
     * @return {@link ServiceDocumentDescription}
     */
    ServiceDocumentDescription buildDocumentDescription(Service s) {
        Class<? extends ServiceDocument> serviceStateClass = s.getStateType();
        if (serviceStateClass == null) {
            return null;
        }

        // Use the service type name to describe this state because its state class might be
        // shared between multiple services. This way, each service will have its own instance.
        String serviceTypeName = s.getClass().getCanonicalName();
        synchronized (this.descriptionCache) {
            ServiceDocumentDescription desc = this.descriptionCache.get(serviceTypeName);
            if (desc != null) {
                return desc;
            }

            // Description has to be built in three stages:
            // 1) Build the base description and add it to the cache
            desc = this.descriptionBuilder.buildDescription(serviceStateClass, s.getOptions(),
                    RequestRouter.findRequestRouter(s.getOperationProcessingChain()));
            this.descriptionCache.put(serviceTypeName, desc);

            // 2) Call the service's getDocumentTemplate() to allow the service author to modify it
            // We are calling a function inside a lock, which is bad practice. This is however
            // by contract a synchronous function that should be O(1). We also only call it once.
            desc = s.getDocumentTemplate().documentDescription;

            // 3) Update the cached entry
            this.descriptionCache.put(serviceTypeName, desc);
            return desc;
        }
    }

    public URI getPublicUri() {
        if (this.state.publicUri == null) {
            return getUri();
        }
        return this.state.publicUri;
    }

    public URI getUri() {
        if (this.cachedUri == null) {
            this.cachedUri = UriUtils.buildUri(getPreferredAddress(), getPort(), "", null);
        }
        return this.cachedUri;
    }

    public URI getSecureUri() {
        return UriUtils.buildUri(UriUtils.HTTPS_SCHEME, getUri().getHost(), getSecurePort(), "",
                null);
    }

    public String getPreferredAddress() {
        if (this.info == null || this.info.ipAddresses == null || this.info.ipAddresses.isEmpty()) {
            return this.state.bindAddress == null ? ServiceHost.LOCAL_HOST
                    : this.state.bindAddress;
        }
        return this.info.ipAddresses.get(0);
    }

    /**
     * Return the host's token signer.
     *
     * Visibility is intentionally set to non-public since access to the signer
     * must be limited to authorized services only.
     *
     * @return token signer.
     */
    protected Signer getTokenSigner() {
        return this.tokenSigner;
    }

    /**
     * Return the host's token verifier.
     *
     * Visibility is intentionally set to non-public since access to the signer
     * must be limited to authorized services only.
     *
     * @return token verifier.
     */
    protected Verifier getTokenVerifier() {
        return this.tokenVerifier;
    }

    /**
     * Generate new authorization context for a system user.
     *
     * @return fresh authorization context
     */
    private AuthorizationContext createAuthorizationContext(String userLink) {
        Claims.Builder cb = new Claims.Builder();
        cb.setIssuer(AuthenticationConstants.JWT_ISSUER);
        cb.setSubject(userLink);

        // Set an effective expiration to never
        Calendar cal = Calendar.getInstance();
        cal.set(9999, Calendar.DECEMBER, 31);
        cb.setExpirationTime(TimeUnit.MILLISECONDS.toMicros(cal.getTimeInMillis()));

        // Generate token for set of claims
        Claims claims = cb.getResult();
        String token;
        try {
            token = getTokenSigner().sign(claims);
        } catch (GeneralSecurityException e) {
            // This function is run first when the host starts, which will fail if this
            // exception comes up. This is necessary because the host cannot function
            // without having access to the system user's context.
            throw new RuntimeException(e);
        }

        AuthorizationContext.Builder ab = AuthorizationContext.Builder.create();
        ab.setClaims(claims);
        ab.setToken(token);
        ab.setPropagateToClient(false);
        return ab.getResult();
    }

    /**
     * Return the system user's authorization context.
     *
     * @return authorization context.
     */
    protected AuthorizationContext getSystemAuthorizationContext() {
        AuthorizationContext ctx = this.systemAuthorizationContext;
        if (ctx == null) {
            // No locking needed; duplicate work is benign
            ctx = createAuthorizationContext(SystemUserService.SELF_LINK);
            this.systemAuthorizationContext = ctx;
        }

        return ctx;
    }

    /**
     * Return the guest user's authorization context.
     *
     * @return authorization context.
     */
    protected AuthorizationContext getGuestAuthorizationContext() {
        AuthorizationContext ctx = this.guestAuthorizationContext;
        if (ctx == null) {
            // No locking needed; duplicate work is benign
            ctx = createAuthorizationContext(GuestUserService.SELF_LINK);
            this.guestAuthorizationContext = ctx;
        }

        return ctx;
    }

    /**
     * Call to add a service to a privileged list for interaction with
     * auth context.
     */
    protected void addPrivilegedService(Class<? extends Service> serviceType) {
        this.privilegedServiceList.put(serviceType.getName(), serviceType);
    }

    protected boolean isPrivilegedService(Service service) {
        // Checks if caller is privileged for auth context calls.
        boolean result = false;

        for (Class<? extends Service> privilegedService : this.privilegedServiceList
                .values()) {
            if (service.getClass().equals(privilegedService)) {
                result = true;
                break;
            }
        }

        return result;
    }

    void scheduleServiceOptionToggleMaintenance(String path, EnumSet<ServiceOption> newOptions,
            EnumSet<ServiceOption> removedOptions) {
        Service s = findService(path);
        if (s == null || s.getProcessingStage() == ProcessingStage.STOPPED) {
            return;
        }
        ServiceMaintenanceRequest body = ServiceMaintenanceRequest.create();
        body.reasons.add(MaintenanceReason.NODE_GROUP_CHANGE);
        body.reasons.add(MaintenanceReason.SERVICE_OPTION_TOGGLE);
        body.configUpdate = new ServiceConfigUpdateRequest();
        body.configUpdate.addOptions = newOptions;
        body.configUpdate.removeOptions = removedOptions;
        s.adjustStat(Service.STAT_NAME_NODE_GROUP_CHANGE_MAINTENANCE_COUNT, 1);
        run(() -> {
            OperationContext.setAuthorizationContext(getSystemAuthorizationContext());
            s.handleMaintenance(Operation.createPost(s.getUri()).setBody(body));
        });
    }

}
