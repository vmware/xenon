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
import java.net.InetAddress;
import java.net.NetworkInterface;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
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
import java.util.function.Consumer;
import java.util.function.Supplier;
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
import com.vmware.xenon.common.Operation.OperationOption;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ProcessingStage;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceDocumentDescription.Builder;
import com.vmware.xenon.common.ServiceErrorResponse.ErrorDetail;
import com.vmware.xenon.common.ServiceHost.RequestRateInfo.Option;
import com.vmware.xenon.common.ServiceHost.ServiceHostState.MemoryLimitType;
import com.vmware.xenon.common.ServiceHost.ServiceHostState.SslClientAuthMode;
import com.vmware.xenon.common.ServiceMaintenanceRequest.MaintenanceReason;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.AggregationType;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.TimeBin;
import com.vmware.xenon.common.ServiceSubscriptionState.ServiceSubscriber;
import com.vmware.xenon.common.http.netty.NettyHttpListener;
import com.vmware.xenon.common.http.netty.NettyHttpServiceClient;
import com.vmware.xenon.common.jwt.JWTUtils;
import com.vmware.xenon.common.jwt.Signer;
import com.vmware.xenon.common.jwt.Verifier;
import com.vmware.xenon.services.common.AuthCredentialsService;
import com.vmware.xenon.services.common.AuthorizationContextService;
import com.vmware.xenon.services.common.AuthorizationTokenCacheService;
import com.vmware.xenon.services.common.ConsistentHashingNodeSelectorService;
import com.vmware.xenon.services.common.FileContentService;
import com.vmware.xenon.services.common.GraphQueryTaskService;
import com.vmware.xenon.services.common.GuestUserService;
import com.vmware.xenon.services.common.LocalQueryTaskFactoryService;
import com.vmware.xenon.services.common.LuceneDocumentIndexBackupService;
import com.vmware.xenon.services.common.LuceneDocumentIndexService;
import com.vmware.xenon.services.common.NodeGroupFactoryService;
import com.vmware.xenon.services.common.NodeGroupService.JoinPeerRequest;
import com.vmware.xenon.services.common.NodeGroupUtils;
import com.vmware.xenon.services.common.ODataQueryService;
import com.vmware.xenon.services.common.OperationIndexService;
import com.vmware.xenon.services.common.ProcessFactoryService;
import com.vmware.xenon.services.common.QueryFilter;
import com.vmware.xenon.services.common.QueryTaskFactoryService;
import com.vmware.xenon.services.common.ReliableSubscriptionService;
import com.vmware.xenon.services.common.ResourceGroupService;
import com.vmware.xenon.services.common.RoleService;
import com.vmware.xenon.services.common.ServiceContextIndexService;
import com.vmware.xenon.services.common.ServiceHostLogService;
import com.vmware.xenon.services.common.ServiceHostManagementService;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.SystemUserService;
import com.vmware.xenon.services.common.TaskFactoryService;
import com.vmware.xenon.services.common.TenantService;
import com.vmware.xenon.services.common.TransactionFactoryService;
import com.vmware.xenon.services.common.UpdateIndexRequest;
import com.vmware.xenon.services.common.UserGroupService;
import com.vmware.xenon.services.common.UserService;
import com.vmware.xenon.services.common.authn.AuthenticationConstants;
import com.vmware.xenon.services.common.authn.BasicAuthenticationService;
import com.vmware.xenon.services.common.authn.BasicAuthenticationUtils;

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
public class ServiceHost implements ServiceRequestSender {

    public static class ServiceAlreadyStartedException extends IllegalStateException {
        private static final long serialVersionUID = -1444810129515584386L;

        public ServiceAlreadyStartedException(String servicePath) {
            super("Service already started: " + servicePath);
        }

        public ServiceAlreadyStartedException(String servicePath, ProcessingStage stage) {
            super("Service already started: " + servicePath + " stage: " + stage);
        }

        public ServiceAlreadyStartedException(String servicePath, String customErrorMessage) {
            super("Service already started: " + servicePath + ". " + customErrorMessage);
        }
    }

    public static class ServiceNotFoundException extends IllegalStateException {
        private static final long serialVersionUID = 663670123267539178L;

        public ServiceNotFoundException() {
            super();
        }

        public ServiceNotFoundException(String servicePath) {
            super("Service not found: " + servicePath);
        }

        public ServiceNotFoundException(String servicePath, String customErrorMessage) {
            super("Service not found: " + servicePath + ". " + customErrorMessage);
        }
    }

    public static class Arguments {
        /**
         * HTTP port
         */
        public int port = DEFAULT_PORT;

        /**
         * HTTPS port
         */
        public int securePort = PORT_VALUE_LISTENER_DISABLED;

        /**
         * SSL client authorization mode
         */
        public SslClientAuthMode sslClientAuthMode = SslClientAuthMode.NONE;

        /**
         * File path to key file(PKCS#8 private key file in PEM format)
         */
        public Path keyFile;

        /**
         * Key passphrase
         */
        public String keyPassphrase;

        /**
         * File path to certificate file
         */
        public Path certificateFile;

        /**
         * File directory path used to store service state
         */
        public Path sandbox = DEFAULT_SANDBOX;

        /**
         * Network interface address to bind to
         */
        public String bindAddress = DEFAULT_BIND_ADDRESS;

        /**
         * Optional public URI the host uses to advertise itself to peers. If its
         * not set, the bind address and port will be used to form the host URI
         */
        public String publicUri;

        /**
         * Comma separated list of one or more peer nodes to join through Nodes
         * must be defined in URI form, e.g --peerNodes=http://192.168.1.59:8000,http://192.168.1.82
         */
        public String[] peerNodes;

        /**
         * A stable identity associated with this host
         */
        public String id;

        /**
         * An upper bound, in seconds, for service synchronization to complete. The runtime synchronizes
         * one replicated factory at a time. This limit applies to upper bound the runtime will wait for
         * a given factory, before moving on to the next. The factory that did not finish in time will stay
         * unavailable (/available will return error). The runtime will continue synchronization with the next
         * factory and the node will be marked as available even if one factory fails to complete in time.
         * If a factory does not finish in time, its availability can be explicitly reset with a PATCH to
         * the STAT_NAME_IS_AVALABLE, to the factory /stats utility service.
         *
         * A factory will accept POST requests, even during synchronization, and even if it fails to
         * complete synchronization in time. The availability indicator on /available is a hint, it does
         * not prevent the factory from functioning.
         *
         * The default value of 10 minutes allows for 1.8M services to synchronize, given an estimate of
         * 3,000 service synchronizations per second, on a three node cluster, on a local network.
         *
         * Synchronization starts automatically if {@link Arguments#isPeerSynchronizationEnabled} is true,
         * and the node group has observed a node joining or leaving (becoming unavailable)
         */
        public int perFactoryPeerSynchronizationLimitSeconds = (int) TimeUnit.MINUTES.toSeconds(10);

        /**
         * Value indicating whether node group changes will automatically
         * trigger replicated service state synchronization. If set to false, client can issue
         * synchronization requests through core management service
         */
        public boolean isPeerSynchronizationEnabled = true;

        /**
         * Mandate an auth context for all requests
         * This option will be set to true and authn/authz enabled by default after a transition period
         */
        public boolean isAuthorizationEnabled = false;

        /**
         * Base URI of the xenon instance that acts as the auth source for this service host
         */
        public String authProviderHostUri;
        /**
         * File directory path to resource files. If specified resources will loaded from here instead of
         * the JAR file of the host
         */
        public Path resourceSandbox;

        /**
         * The logical location of this host, if any
         */
        public String location;

    }

    protected static final LogFormatter LOG_FORMATTER = new LogFormatter();
    protected static final LogFormatter COLOR_LOG_FORMATTER = new ColorLogFormatter();

    public static final String SERVICE_HOST_STATE_FILE = "serviceHostState.json";

    public static final Double DEFAULT_PCT_MEMORY_LIMIT = 0.49;
    public static final Double DEFAULT_PCT_MEMORY_LIMIT_DOCUMENT_INDEX = 0.45;
    public static final Double DEFAULT_PCT_MEMORY_LIMIT_SERVICE_CONTEXT_INDEX = 0.01;

    public static final String LOOPBACK_ADDRESS = "127.0.0.1";
    public static final String LOCAL_HOST = LOOPBACK_ADDRESS;
    public static final String DEFAULT_BIND_ADDRESS = ServiceHost.LOCAL_HOST;

    public static final int PORT_VALUE_HTTP_DEFAULT = 8000;

    /**
     * Indicates that the listener associated with this port field should not be started
     */
    public static final int PORT_VALUE_LISTENER_DISABLED = -1;

    public static final int DEFAULT_PORT = PORT_VALUE_HTTP_DEFAULT;

    public static final String ALL_INTERFACES = "0.0.0.0";

    public static final String ROOT_PATH = "";

    public static final String SERVICE_URI_SUFFIX_STATS = "/stats";
    public static final String SERVICE_URI_SUFFIX_SUBSCRIPTIONS = "/subscriptions";

    public static final String SERVICE_URI_SUFFIX_AVAILABLE = "/available";
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
            SERVICE_URI_SUFFIX_AVAILABLE,
            SERVICE_URI_SUFFIX_REPLICATION,
            SERVICE_URI_SUFFIX_STATS,
            SERVICE_URI_SUFFIX_SUBSCRIPTIONS,
            SERVICE_URI_SUFFIX_UI,
            SERVICE_URI_SUFFIX_CONFIG,
            SERVICE_URI_SUFFIX_TEMPLATE };

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
    public static final int DEFAULT_SERVICE_INSTANCE_COST_BYTES = 4096;

    private static final String PROPERTY_NAME_APPEND_PORT_TO_SANDBOX = Utils.PROPERTY_NAME_PREFIX
            + "ServiceHost.APPEND_PORT_TO_SANDBOX";

    /**
     * Control creating a directory using port number under sandbox directory.
     *
     * VM argument: "-Dxenon.ServiceHost.APPEND_PORT_TO_SANDBOX=[true|false]"
     * Default is set to true.
     */
    public static final boolean APPEND_PORT_TO_SANDBOX = System
            .getProperty(PROPERTY_NAME_APPEND_PORT_TO_SANDBOX) == null
            || Boolean.getBoolean(PROPERTY_NAME_APPEND_PORT_TO_SANDBOX);




    /**
     * Request rate limiting configuration and real time statistics
     */
    public static class RequestRateInfo {
        public enum Option {
            /**
             * Fail request when limit is reached
             */
            FAIL,

            /**
             * Pause reads from I/O channel
             */
            PAUSE_PROCESSING
        }

        /**
         * Request limit (upper bound). The value represents the maximum number of requests
         * for a given time window, specified through the {@link #timeSeries} parameters
         */
        public double limit;

        /**
         * Options affecting rate limit behavior
         */
        public EnumSet<Option> options = null;

        /**
         * Time series statistics used to track number of requests per time bin. If not
         * specified, the system will use a one minute, 60 second time summation series
         */
        public TimeSeriesStats timeSeries;
    }

    public static class ServiceHostState extends ServiceDocument {
        public enum MemoryLimitType {
            LOW_WATERMARK, HIGH_WATERMARK, EXACT
        }

        public enum SslClientAuthMode {
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
        public long serviceCacheClearDelayMicros = DEFAULT_OPERATION_TIMEOUT_MICROS;
        public String operationTracingLevel;
        public SslClientAuthMode sslClientAuthMode;
        public int responsePayloadSizeLimit;
        public int requestPayloadSizeLimit;

        public URI storageSandboxFileReference;
        public URI resourceSandboxFileReference;
        public URI privateKeyFileReference;
        public String privateKeyPassphrase;
        public URI certificateFileReference;

        public URI documentIndexReference;
        public URI authorizationServiceReference;
        public URI transactionServiceReference;
        public String id;
        public boolean isPeerSynchronizationEnabled;
        public int peerSynchronizationTimeLimitSeconds;
        public boolean isAuthorizationEnabled;
        public transient boolean isStarted;
        public transient boolean isStopping;
        public SystemHostInfo systemInfo;
        public long lastMaintenanceTimeUtcMicros;
        public boolean isProcessOwner;
        public boolean isServiceStateCaching = true;
        public Properties codeProperties;
        public long serviceCount;
        public String location;
        public URI authProviderHostURI;

        /**
         * Relative memory limit per service path. The limit is expressed as
         * percentage (range of [0.0,1.0]) of max memory available to the java virtual machine
         *
         * The empty path, "", is reserved for the host memory limit
         */
        public ConcurrentHashMap<String, Double> relativeMemoryLimits = new ConcurrentHashMap<>();

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
        public ConcurrentHashMap<String, RequestRateInfo> requestRateLimits = new ConcurrentHashMap<>();

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
                        ServiceUriPaths.CORE_LOCAL_QUERY_TASKS,
                        ServiceUriPaths.CORE_QUERY_TASKS }));
        public String[] initialPeerNodes;
    }

    public enum HttpScheme {
        HTTP_ONLY, HTTPS_ONLY, HTTP_AND_HTTPS, NONE
    }

    /**
     * Simple way of creating ServiceHost.
     *
     * This method performs initialization phase of service host - initialize by argument and register shutdown hook.
     * If more detailed configuration is required, create a dedicated host class extending ServiceHost.
     *
     * NOTE:
     * {@link #startDefaultCoreServicesSynchronously()} requires {@link #start()} to be called beforehand.
     *
     * Sample:
     * <pre>
     *     ServiceHost host = ServiceHost.create();
     *     host.start();  // you need to call "start()" BEFORE "startCoreServicesSynchronously()"
     *     host.startCoreServicesSynchronously();
     *     host.startService(...);
     *     ...
     * </pre>
     *
     * @param args initialization arguments
     * @return a ServiceHost
     */
    public static ServiceHost create(String... args) throws Throwable {
        ServiceHost host = new ServiceHost();
        host.initialize(args);
        host.registerRuntimeShutdownHook();
        return host;
    }

    /**
     * Default shutdown hook to stop this host.
     */
    protected final Thread defaultShutdownHook = new Thread(() -> {
        this.log(Level.WARNING, "Host stopping ...");
        this.stop();
        this.log(Level.WARNING, "Host is stopped");
    });


    private Logger logger = Logger.getLogger(getClass().getName());
    private FileHandler handler;

    private final ConcurrentHashMap<String, AuthorizationContext> authorizationContextCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> userLinkToTokenMap = new ConcurrentHashMap<>();

    private final Map<String, ServiceDocumentDescription> descriptionCache = new HashMap<>();
    private final Map<String, ServiceDocumentDescription> descriptionCachePerFactoryLink = new HashMap<>();
    private final ServiceDocumentDescription.Builder descriptionBuilder = Builder.create();

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    private final ConcurrentHashMap<String, Service> attachedServices = new ConcurrentHashMap<>();
    private final ConcurrentSkipListMap<String, Service> attachedNamespaceServices = new ConcurrentSkipListMap<>();

    private final ConcurrentSkipListSet<String> coreServices = new ConcurrentSkipListSet<>();
    private final ConcurrentHashMap<String, Class<? extends Service>> privilegedServiceTypes = new ConcurrentHashMap<>();

    private final Set<String> pendingServiceDeletions = Collections
            .synchronizedSet(new HashSet<String>());
    private final ConcurrentHashMap<String, Service> pendingPauseServices = new ConcurrentHashMap<>();

    private ServiceHostState state;
    private Service documentIndexService;
    private Service authorizationService;
    private Service transactionService;
    private Service managementService;
    private Service authenticationService;
    private Service basicAuthenticationService;
    private SystemHostInfo info = new SystemHostInfo();
    private ServiceClient client;

    private ServiceRequestListener httpListener;
    private ServiceRequestListener httpsListener;

    private URI documentIndexServiceUri;
    private URI operationIndexServiceUri;
    private URI authorizationServiceUri;
    private URI transactionServiceUri;
    private URI managementServiceUri;
    private URI authenticationServiceUri;
    private URI basicAuthenticationServiceUri;
    private ScheduledFuture<?> maintenanceTask;

    private final ServiceSynchronizationTracker serviceSynchTracker = ServiceSynchronizationTracker
            .create(this);
    private final ServiceMaintenanceTracker serviceMaintTracker = ServiceMaintenanceTracker
            .create(this);
    private final ServiceResourceTracker serviceResourceTracker = ServiceResourceTracker
            .create(this, this.attachedServices, this.pendingPauseServices);
    private final OperationTracker operationTracker = OperationTracker.create(this);

    private String hashedId;
    private String logPrefix;
    private URI cachedUri;
    private String cachedPublicUriString;

    private Signer tokenSigner;
    private Verifier tokenVerifier;

    private AuthorizationContext systemAuthorizationContext;
    private AuthorizationContext guestAuthorizationContext;
    private ScheduledExecutorService serviceScheduledExecutor;

    protected ServiceHost() {
        this.state = new ServiceHostState();
        this.state.id = UUID.randomUUID().toString();
    }

    public ServiceHost initialize(String[] args) throws Throwable {
        Arguments hostArgs = new Arguments();
        initialize(args, hostArgs);
        return this;
    }

    /**
     * This method is intended for subclasses that extend the Arguments class
     */
    protected ServiceHost initialize(String[] args, Arguments hostArgs) throws Throwable {
        CommandLineArgumentParser.parse(hostArgs, args);
        CommandLineArgumentParser.parse(COLOR_LOG_FORMATTER, args);
        initialize(hostArgs);
        return this;

    }

    public ServiceHost initialize(Arguments args) throws Throwable {
        setSystemProperties();

        if (args.port == PORT_VALUE_LISTENER_DISABLED
                && args.securePort == PORT_VALUE_LISTENER_DISABLED) {
            throw new IllegalArgumentException("both http and https are disabled");
        }

        if (args.port != PORT_VALUE_LISTENER_DISABLED && args.port < 0) {
            throw new IllegalArgumentException("port: negative values not allowed");
        }

        if (args.securePort != PORT_VALUE_LISTENER_DISABLED && args.securePort < 0) {
            throw new IllegalArgumentException("securePort: negative values not allowed");
        }

        Path sandbox = args.sandbox;
        if (APPEND_PORT_TO_SANDBOX) {
            int sandboxPort = args.port == PORT_VALUE_LISTENER_DISABLED ? args.securePort
                    : args.port;
            sandbox = sandbox.resolve(Integer.toString(sandboxPort));
        }

        URI storageSandbox = sandbox.toFile().toURI();

        if (!Files.exists(sandbox)) {
            Files.createDirectories(sandbox);
        }

        if (args.publicUri != null) {
            URI u = new URI(args.publicUri);
            if (!u.isAbsolute() || u.getHost() == null || u.getHost().isEmpty()) {
                throw new IllegalArgumentException("publicUri should be a non empty absolute URI");
            }
        }

        if (args.bindAddress != null && args.bindAddress.equals("")) {
            throw new IllegalArgumentException(
                    "bindAddress should be a non empty valid IP address");
        }

        if (this.state == null) {
            throw new IllegalStateException();
        }

        File s = new File(storageSandbox);

        if (!s.exists()) {
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

        ServiceHostManagementService managementService = new ServiceHostManagementService();
        setManagementService(managementService);

        BasicAuthenticationService basicAuthenticationService = new BasicAuthenticationService();
        setAuthenticationService(basicAuthenticationService);
        setBasicAuthenticationService(basicAuthenticationService);

        this.state.codeProperties = FileUtils.readPropertiesFromResource(this.getClass(),
                GIT_COMMIT_PROPERTIES_RESOURCE_NAME);

        updateSystemInfo(false);

        // Set default limits for memory utilization on core services and the host
        if (getServiceMemoryLimitMB(ROOT_PATH, MemoryLimitType.EXACT) == null) {
            setServiceMemoryLimit(ROOT_PATH, DEFAULT_PCT_MEMORY_LIMIT);
        }
        if (getServiceMemoryLimitMB(ServiceUriPaths.CORE_DOCUMENT_INDEX,
                MemoryLimitType.EXACT) == null) {
            setServiceMemoryLimit(ServiceUriPaths.CORE_DOCUMENT_INDEX,
                    DEFAULT_PCT_MEMORY_LIMIT_DOCUMENT_INDEX);
        }
        if (getServiceMemoryLimitMB(ServiceUriPaths.CORE_SERVICE_CONTEXT_INDEX,
                MemoryLimitType.EXACT) == null) {
            setServiceMemoryLimit(ServiceUriPaths.CORE_SERVICE_CONTEXT_INDEX,
                    DEFAULT_PCT_MEMORY_LIMIT_SERVICE_CONTEXT_INDEX);
        }
        allocateExecutors();
        return this;
    }

    private void allocateExecutors() {
        if (this.executor != null) {
            this.executor.shutdownNow();
        }
        if (this.scheduledExecutor != null) {
            this.scheduledExecutor.shutdownNow();
        }
        if (this.serviceScheduledExecutor != null) {
            this.serviceScheduledExecutor.shutdownNow();
        }
        this.executor = Executors.newWorkStealingPool(Utils.DEFAULT_THREAD_COUNT);
        this.scheduledExecutor = Executors.newScheduledThreadPool(Utils.DEFAULT_THREAD_COUNT,
                r -> new Thread(r, getUri().toString() + "/scheduled/" + this.state.id));
        this.serviceScheduledExecutor = Executors.newScheduledThreadPool(
                Utils.DEFAULT_THREAD_COUNT / 2,
                r -> new Thread(r, getUri().toString() + "/service-scheduled/" + this.state.id));
    }

    /**
     * Retrieve secret for sign/verify JSON(JWT)
     */
    protected byte[] getJWTSecret() throws IOException {
        URI privateKeyFileUri = this.state.privateKeyFileReference;
        String privateKeyPassphrase = this.state.privateKeyPassphrase;

        return JWTUtils.getJWTSecret(privateKeyFileUri, privateKeyPassphrase,
                this.isAuthorizationEnabled());
    }

    private void initializeStateFromArguments(File s, Arguments args) throws URISyntaxException {
        if (args.resourceSandbox != null) {
            File resDir = args.resourceSandbox.toFile();
            if (resDir.exists()) {
                this.state.resourceSandboxFileReference = resDir.toURI();
            } else {
                log(Level.WARNING, "Resource sandbox does not exist: %s", args.resourceSandbox);
            }
        }

        this.state.httpPort = args.port;
        this.state.httpsPort = args.securePort;
        this.state.sslClientAuthMode = args.sslClientAuthMode;

        if (args.keyFile != null) {
            this.state.privateKeyFileReference = args.keyFile.toUri();
            this.state.privateKeyPassphrase = args.keyPassphrase;
        }

        if (args.certificateFile != null) {
            this.state.certificateFileReference = args.certificateFile.toUri();
        }

        if (args.id != null) {
            this.state.id = args.id;
        }
        this.hashedId = Utils.computeHash(this.state.id);

        this.state.peerSynchronizationTimeLimitSeconds = args.perFactoryPeerSynchronizationLimitSeconds;
        this.state.isPeerSynchronizationEnabled = args.isPeerSynchronizationEnabled;
        this.state.isAuthorizationEnabled = args.isAuthorizationEnabled;
        if (args.authProviderHostUri != null) {
            this.state.authProviderHostURI = new URI(args.authProviderHostUri);
        }

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
        this.state.location = args.location;
    }

    public String getLocation() {
        return this.state.location;
    }

    public void setLocation(String location) {
        if (isStarted()) {
            throw new IllegalStateException("Already started");
        }
        this.state.location = location;
    }

    protected void configureLogging(File storageSandboxDir) throws IOException {
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

        configureLoggerFormatter(this.logger);

        this.logPrefix = getClass().getSimpleName() + ":" + getPort();
    }

    protected void configureLoggerFormatter(Logger logger) {
        for (java.util.logging.Handler h : logger.getParent().getHandlers()) {
            if (h instanceof ConsoleHandler) {
                h.setFormatter(COLOR_LOG_FORMATTER);
            } else {
                h.setFormatter(LOG_FORMATTER);
            }
        }
    }

    protected void removeLogging() {
        if (this.handler != null) {
            this.logger.getParent().removeHandler(this.handler);
            this.handler.close();
            this.handler = null;
        }
    }

    private void loadState(URI storageSandbox, File s) throws IOException, InterruptedException {
        File hostStateFile = new File(s, SERVICE_HOST_STATE_FILE);
        if (!hostStateFile.isFile()) {
            return;
        }

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
                                if (fileState.maintenanceIntervalMicros < Service.MIN_MAINTENANCE_INTERVAL_MICROS) {
                                    fileState.maintenanceIntervalMicros = Service.MIN_MAINTENANCE_INTERVAL_MICROS;
                                }
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

    private void saveState() throws IOException, InterruptedException {
        saveState(new File(this.state.storageSandboxFileReference));
    }

    private void saveState(File sandboxDir) throws IOException, InterruptedException {
        File hostStateFile = new File(sandboxDir, SERVICE_HOST_STATE_FILE);
        this.state.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        byte[] serializedState = Utils.toJsonHtml(this.state).getBytes(Utils.CHARSET);
        Files.write(hostStateFile.toPath(), serializedState, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
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
        this.serviceResourceTracker.setServiceStateCaching(enable);
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
        if (this.httpListener != null) {
            try {
                this.httpListener.stop();
            } catch (IOException e) {
            }
            this.httpListener = null;
        }
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

    public int getPeerSynchronizationTimeLimitSeconds() {
        return this.state.peerSynchronizationTimeLimitSeconds;
    }

    public void setPeerSynchronizationTimeLimitSeconds(int seconds) {
        this.state.peerSynchronizationTimeLimitSeconds = seconds;
    }

    public int getSecurePort() {
        return this.state.httpsPort;
    }

    public ServiceHost setSecurePort(int port) {
        if (isStarted()) {
            throw new IllegalStateException("Already started");
        }
        this.state.httpsPort = port;
        if (this.httpsListener != null) {
            try {
                this.httpsListener.stop();
            } catch (IOException e) {
            }
            this.httpsListener = null;
        }
        return this;
    }

    /**
     * URI to a PKCS#8 private key file in PEM format.
     */
    public ServiceHost setPrivateKeyFileReference(URI fileReference) {
        this.state.privateKeyFileReference = fileReference;
        return this;
    }

    /**
     * Passphrase for private key file.
     *
     * @param privateKeyPassphrase {@code null} if it's not password-protected.
     */
    public ServiceHost setPrivateKeyPassphrase(String privateKeyPassphrase) {
        this.state.privateKeyPassphrase = privateKeyPassphrase;
        return this;
    }

    /**
     * URI to an X.509 certificate chain file in PEM format.
     */
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

        clearUriAndLogPrefix();
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
        clearUriAndLogPrefix();
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

        if (micros < Service.MIN_MAINTENANCE_INTERVAL_MICROS) {
            log(Level.WARNING, "Maintenance interval %d is less than the minimum interval %d"
                    + ", reducing to min interval", micros,
                    Service.MIN_MAINTENANCE_INTERVAL_MICROS);
            micros = Service.MIN_MAINTENANCE_INTERVAL_MICROS;
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

    /**
     * Returns a unique version 1 UUID-like string based on the node id and the current time.
     * @return
     */
    public String nextUUID() {
        return Utils.buildUUID(this.hashedId);
    }

    public long getOperationTimeoutMicros() {
        return this.state.operationTimeoutMicros;
    }

    public ServiceHostState getState() {
        ServiceHostState s = Utils.clone(this.state);
        s.systemInfo = getSystemInfo();
        return s;
    }

    ServiceHostState getStateNoCloning() {
        this.state.systemInfo = this.info;
        return this.state;
    }

    Service getDocumentIndexService() {
        return this.documentIndexService;
    }

    public URI getDocumentIndexServiceUri() {
        if (this.documentIndexService == null) {
            return null;
        }
        if (this.documentIndexServiceUri == null) {
            this.documentIndexServiceUri = this.documentIndexService.getUri();
        }
        return this.documentIndexServiceUri;
    }

    public URI getAuthorizationServiceUri() {
        if (this.authorizationService == null) {
            return null;
        }
        if (this.authorizationServiceUri == null) {
            this.authorizationServiceUri = this.authorizationService.getUri();
        }
        return this.authorizationServiceUri;
    }

    public URI getTransactionServiceUri() {
        if (this.transactionService == null) {
            return null;
        }
        if (this.transactionServiceUri == null) {
            this.transactionServiceUri = this.transactionService.getUri();
        }
        return this.transactionServiceUri;
    }

    public URI getManagementServiceUri() {
        if (this.managementService == null) {
            return null;
        }
        if (this.managementServiceUri == null) {
            this.managementServiceUri = this.managementService.getUri();
        }
        return this.managementServiceUri;
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

    public ServiceHost setManagementService(Service service) {
        if (this.state.isStarted) {
            throw new IllegalStateException("Host is started");
        }
        this.managementService = service;
        return this;
    }

    Service getManagementService() {
        return this.managementService;
    }

    public ServiceHost setAuthenticationService(Service service) {
        if (this.state.isStarted) {
            throw new IllegalStateException("Host is started");
        }
        this.authenticationService = service;
        return this;
    }

    Service getAuthenticationService() {
        return this.authenticationService;
    }

    public URI getAuthenticationServiceUri() {
        if (this.authenticationService == null) {
            return null;
        }
        if (this.authenticationServiceUri == null) {
            this.authenticationServiceUri = this.authenticationService.getUri();
        }
        return this.authenticationServiceUri;
    }

    private ServiceHost setBasicAuthenticationService(Service service) {
        this.basicAuthenticationService = service;
        return this;
    }

    private URI getBasicAuthenticationServiceUri() {
        if (this.basicAuthenticationService == null) {
            return null;
        }
        if (this.basicAuthenticationServiceUri == null) {
            this.basicAuthenticationServiceUri = this.basicAuthenticationService.getUri();
        }
        return this.basicAuthenticationServiceUri;
    }

    public ScheduledExecutorService getScheduledExecutor() {
        return this.serviceScheduledExecutor;
    }

    public ExecutorService getExecutor() {
        return this.executor;
    }

    public ExecutorService allocateExecutor(Service s) {
        return allocateExecutor(s, Utils.DEFAULT_THREAD_COUNT);
    }

    public ExecutorService allocateExecutor(Service s, int threadCount) {
        return Executors.newFixedThreadPool(threadCount, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, s.getUri() + "/" + Utils.getSystemNowMicrosUtc());
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

        // replace attached management service if it is in invalid state.
        // this may happen when host was restarted (calling host.stop(), then host.start())
        if (this.managementService == null
                || this.managementService.getProcessingStage() == ProcessingStage.STOPPED) {
            setManagementService(new ServiceHostManagementService());
        }

        synchronized (this.state) {
            if (isStarted()) {
                return this;
            }
            this.state.isStarted = true;
            this.state.isStopping = false;
        }

        if (this.executor == null || this.scheduledExecutor == null
                || this.serviceScheduledExecutor == null) {
            allocateExecutors();
        }

        if (this.isAuthorizationEnabled() && this.authorizationService == null) {
            this.authorizationService = new AuthorizationContextService();
        }

        byte[] secret = getJWTSecret();
        this.tokenSigner = new Signer(secret);
        this.tokenVerifier = new Verifier(secret);

        // Start listeners and client under system context, they start helper services
        AuthorizationContext ctx = OperationContext.getAuthorizationContext();
        OperationContext.setAuthorizationContext(getSystemAuthorizationContext());

        if (getPort() != PORT_VALUE_LISTENER_DISABLED) {
            if (this.httpListener == null) {
                this.httpListener = new NettyHttpListener(this);
            }

            if (this.state.responsePayloadSizeLimit > 0) {
                this.httpListener.setResponsePayloadSizeLimit(this.state.responsePayloadSizeLimit);
            }

            this.httpListener.start(getPort(), this.state.bindAddress);
        }

        if (getSecurePort() != PORT_VALUE_LISTENER_DISABLED) {
            if (this.httpsListener == null) {
                if (this.state.certificateFileReference == null
                        && this.state.privateKeyFileReference == null) {
                    log(Level.WARNING, "certificate and private key are missing");
                } else {
                    this.httpsListener = new NettyHttpListener(this);
                }
            }

            if (this.httpsListener != null) {
                if (!this.httpsListener.isSSLConfigured()) {
                    this.httpsListener.setSSLContextFiles(this.state.certificateFileReference,
                            this.state.privateKeyFileReference, this.state.privateKeyPassphrase);
                }
                if (this.state.responsePayloadSizeLimit > 0) {
                    this.httpsListener
                            .setResponsePayloadSizeLimit(this.state.responsePayloadSizeLimit);
                }

                this.httpsListener.start(getSecurePort(), this.state.bindAddress);
            }
        }

        // Update the state JSON file if the port was chosen by the httpListener.
        // An external process can then get the port from the state file.
        if (this.state.httpPort == 0) {
            this.state.httpPort = this.httpListener.getPort();
        }

        if (this.state.httpsPort == 0 && this.httpsListener != null) {
            this.state.httpsPort = this.httpsListener.getPort();
        }

        // Update the caching policy on the ServiceResourceTracker.
        this.serviceResourceTracker.setServiceStateCaching(this.state.isServiceStateCaching);

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
            // supply a scheduled executor for re-use by the client, but do not supply our
            // regular executor, since the I/O threads might take up all threads
            this.client = NettyHttpServiceClient.create(userAgent,
                    null,
                    this.scheduledExecutor,
                    this);
            SSLContext clientContext = SSLContext.getInstance(ServiceClient.TLS_PROTOCOL_NAME);
            TrustManagerFactory trustManagerFactory = TrustManagerFactory
                    .getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init((KeyStore) null);
            clientContext.init(null, trustManagerFactory.getTrustManagers(), null);
            this.client.setSSLContext(clientContext);
        }

        if (this.state.requestPayloadSizeLimit > 0) {
            this.client.setRequestPayloadSizeLimit(this.state.requestPayloadSizeLimit);
        }

        this.client.start();

        // restore authorization context
        OperationContext.setAuthorizationContext(ctx);

        scheduleMaintenance();

        clearUriAndLogPrefix();
        log(Level.INFO, "%s listening on %s", userAgent, getUri());

        return this;
    }

    /**
     * Starts core singleton services. Should be called once from the service host entry point.
     */
    public void startDefaultCoreServicesSynchronously() throws Throwable {
        if (findService(ServiceHostManagementService.SELF_LINK) != null) {
            throw new IllegalStateException("Already started");
        }

        addPrivilegedService(this.managementService.getClass());
        addPrivilegedService(OperationIndexService.class);
        addPrivilegedService(BasicAuthenticationService.class);

        // Capture authorization context; this function executes as the system user
        AuthorizationContext ctx = OperationContext.getAuthorizationContext();
        OperationContext.setAuthorizationContext(getSystemAuthorizationContext());

        // Start authorization service first since it sits in the dispatch path
        if (this.authorizationService != null) {
            addPrivilegedService(this.authorizationService.getClass());
            addPrivilegedService(AuthorizationTokenCacheService.class);
            startCoreServicesSynchronously(this.authorizationService, new AuthorizationTokenCacheService());
        }

        // start AuthN service before factories since its invoked in the IO path on every
        // request
        if (this.authenticationService != null) {
            if (!(this.authenticationService instanceof BasicAuthenticationService)) {
                addPrivilegedService(this.authenticationService.getClass());
                startCoreServicesSynchronously(this.authenticationService);
            } else {
                // if the authenticationService is set as BasicAuthenticationService use it
                setBasicAuthenticationService(this.authenticationService);
            }
        }

        // start the BasicAuthenticationService anyways
        startCoreServicesSynchronously(this.basicAuthenticationService);

        // Normalize peer list and find our external address
        // This must be done BEFORE node group starts.
        List<URI> peers = getInitialPeerHosts();

        startDefaultReplicationAndNodeGroupServices();

        // The framework supports two phase asynchronous start to avoid explicit
        // ordering of services. However, core query services must be started before anyone else
        // since factories with persisted services use queries to enumerate their children.
        if (this.documentIndexService != null) {
            addPrivilegedService(this.documentIndexService.getClass());
            if (this.documentIndexService instanceof LuceneDocumentIndexService) {
                LuceneDocumentIndexService luceneDocumentIndexService = (LuceneDocumentIndexService)this.documentIndexService;
                Service[] queryServiceArray = new Service[]{
                        luceneDocumentIndexService,
                        new ServiceContextIndexService(),
                        new LuceneDocumentIndexBackupService(luceneDocumentIndexService),
                        new QueryTaskFactoryService(),
                        new LocalQueryTaskFactoryService(),
                        TaskFactoryService.create(GraphQueryTaskService.class),
                        TaskFactoryService.create(SynchronizationTaskService.class)};
                startCoreServicesSynchronously(queryServiceArray);
            }
        }

        List<Service> coreServices = new ArrayList<>();
        coreServices.add(this.managementService);
        coreServices.add(new ProcessFactoryService());
        coreServices.add(new ODataQueryService());

        // Start persisted factories here, after document index is added
        coreServices.add(AuthCredentialsService.createFactory());
        Service userGroupFactory = UserGroupService.createFactory();
        addPrivilegedService(userGroupFactory.getClass());
        addPrivilegedService(UserGroupService.class);
        coreServices.add(userGroupFactory);
        addPrivilegedService(ResourceGroupService.class);
        coreServices.add(ResourceGroupService.createFactory());
        Service roleFactory = RoleService.createFactory();
        addPrivilegedService(RoleService.class);
        addPrivilegedService(roleFactory.getClass());
        coreServices.add(roleFactory);
        addPrivilegedService(UserService.class);
        coreServices.add(UserService.createFactory());
        coreServices.add(TenantService.createFactory());
        coreServices.add(new SystemUserService());
        coreServices.add(new GuestUserService());

        Service transactionFactoryService = new TransactionFactoryService();
        coreServices.add(transactionFactoryService);

        Service[] coreServiceArray = new Service[coreServices.size()];
        coreServices.toArray(coreServiceArray);
        startCoreServicesSynchronously(coreServiceArray);
        setTransactionService(transactionFactoryService);

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

        scheduleCore(() -> {
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
        if (!s.hasOption(ServiceOption.HTML_USER_INTERFACE)) {
            return;
        }
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

        String prefix = path.toString().replace('\\', '/');

        if (this.state.resourceSandboxFileReference != null) {
            discoverFileResources(s, pathToURIPath, baseUriPath, prefix);
        }

        if (pathToURIPath.isEmpty()) {
            discoverJarResources(path, s, pathToURIPath, baseUriPath, prefix);
        }
        return pathToURIPath;
    }

    /**
     * Infrastructure use. Discover all jar resources for the specified service.
     */
    public void discoverJarResources(Path path, Service s, Map<Path, String> pathToURIPath,
            Path baseUriPath, String prefix) throws URISyntaxException, IOException {
        for (ResourceEntry entry : FileUtils.findResources(s.getClass(), prefix)) {
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
    }

    /**
     * Infrastructure use. Discover all file system resources for the specified service.
     */
    public void discoverFileResources(Service s, Map<Path, String> pathToURIPath,
            Path baseUriPath,
            String prefix) {
        File rootDir = new File(new File(this.state.resourceSandboxFileReference), prefix);
        if (!rootDir.exists()) {
            log(Level.INFO, "Resource directory not found: %s", rootDir.toString());
            return;
        }

        String basePath = baseUriPath.toString();
        String serviceName = s.getClass().getSimpleName();
        List<File> resources = FileUtils.findFiles(rootDir.toPath(),
                new HashSet<String>(), false);
        for (File f : resources) {
            String subPath = f.getAbsolutePath();
            subPath = subPath.substring(subPath.indexOf(serviceName));
            subPath = subPath.replace(serviceName, "");
            Path uriPath = Paths.get(basePath, subPath);
            pathToURIPath.put(f.toPath(), uriPath.toString().replace('\\', '/'));
        }

        if (pathToURIPath.isEmpty()) {
            log(Level.INFO, "No resources found in directory: %s", rootDir.toString());
        }
    }

    private void startDefaultReplicationAndNodeGroupServices() throws Throwable {
        // start the node group factory allowing for N number of independent groups
        startCoreServicesSynchronously(new NodeGroupFactoryService());

        // create a default node group
        ServiceDocument serviceState = new ServiceDocument();
        serviceState.documentSelfLink = ServiceUriPaths.DEFAULT_NODE_GROUP_NAME;
        log(Level.FINE, "starting %s", ServiceUriPaths.DEFAULT_NODE_GROUP);
        startFactoryChildServiceSynchronously(NodeGroupFactoryService.SELF_LINK, serviceState);

        List<Operation> startNodeSelectorPosts = new ArrayList<>();
        List<Service> nodeSelectorServices = new ArrayList<>();
        // start a default node selector that replicates to all available nodes
        Operation startPost = Operation.createPost(UriUtils.buildUri(this,
                ServiceUriPaths.DEFAULT_NODE_SELECTOR));
        startNodeSelectorPosts.add(startPost);
        nodeSelectorServices.add(new ConsistentHashingNodeSelectorService());

        // we start second node selector that does 1X replication (owner only)
        createCustomNodeSelectorService(startNodeSelectorPosts,
                nodeSelectorServices,
                ServiceUriPaths.DEFAULT_1X_NODE_SELECTOR,
                1);

        // we start a third node selector that does 3X replication (owner plus 2 peers)
        createCustomNodeSelectorService(startNodeSelectorPosts,
                nodeSelectorServices,
                ServiceUriPaths.DEFAULT_3X_NODE_SELECTOR,
                3);

        // start node selectors before any other core service since the host APIs of forward
        // and broadcast must be ready before any I/O
        startCoreServicesSynchronously(startNodeSelectorPosts, nodeSelectorServices);
    }

    void createCustomNodeSelectorService(List<Operation> startNodeSelectorPosts,
            List<Service> nodeSelectorServices, String link, long factor) {
        Operation startPost = Operation.createPost(UriUtils.buildUri(this, link));
        NodeSelectorState initialState = new NodeSelectorState();
        initialState.nodeGroupLink = ServiceUriPaths.DEFAULT_NODE_GROUP;
        initialState.replicationFactor = factor;
        startPost.setBodyNoCloning(initialState);
        startNodeSelectorPosts.add(startPost);
        nodeSelectorServices.add(new ConsistentHashingNodeSelectorService());
    }

    public void joinPeers(List<URI> peers, String nodeGroupUriPath) {
        if (peers == null) {
            return;
        }

        try {
            for (URI peerNodeBaseUri : peers) {
                URI localNodeGroupUri = UriUtils.buildUri(this, nodeGroupUriPath);
                JoinPeerRequest joinBody = JoinPeerRequest.create(
                        UriUtils.extendUri(peerNodeBaseUri, nodeGroupUriPath), null);
                sendJoinPeerRequest(joinBody, localNodeGroupUri);
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

        URI publicUri = this.state.publicUri;
        for (String peer : peers) {
            URI peerNodeBaseUri;
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

            int selfPort = getPort();
            if (UriUtils.HTTPS_SCHEME.equals(peerNodeBaseUri.getScheme())) {
                selfPort = getSecurePort();
            }

            if (publicUri != null &&
                    publicUri.getHost().equals(peerNodeBaseUri.getHost()) &&
                    publicUri.getPort() == peerNodeBaseUri.getPort()) {
                // self, skip
                log(Level.INFO, "Skipping peer %s, its us (%s)", peerNodeBaseUri, peerNodeBaseUri.getHost());
                continue;
            }

            if (checkAndSetPreferredAddress(peerNodeBaseUri.getHost())
                    && peerNodeBaseUri.getPort() == selfPort) {
                // self, skip
                log(Level.INFO, "Skipping peer %s, its us", peerNodeBaseUri);
                continue;
            }
            peerList.add(peerNodeBaseUri);
        }
        return peerList;
    }

    private void sendJoinPeerRequest(JoinPeerRequest joinBody, URI localNodeGroupUri) {
        Operation peerRequestOp = Operation
                .createPost(localNodeGroupUri)
                .setReferer(UriUtils.buildUri(this, ""))
                .setBody(joinBody)
                .setCompletion((o, e) -> {
                    if (e == null) {
                        return;
                    }
                    if (e != null) {
                        log(Level.WARNING, "Failure from local node group for join to: %s: %s",
                                joinBody.memberGroupReference,
                                e.toString());
                    }
                });
        peerRequestOp.setAuthorizationContext(getSystemAuthorizationContext());
        sendRequest(peerRequestOp);
    }

    /**
     * Helper method to start both anonymous and non-anonymous factory services uniformly.
     *
     * Starts factory services using:
     * -  {@code UriUtils.FIELD_NAME_SELF_LINK} field on service or
     * -  {@code UriUtils.FIELD_NAME_FACTORY_LINK} field on childService.
     *
     * Services do not start in case:
     * - Any instance is not a factory service or
     * - {@code UriUtils.FIELD_NAME_SELF_LINK} and {@code UriUtils.FIELD_NAME_FACTORY_LINK} fields are missing.
     */
    public void startFactoryServicesSynchronously(Service... services) throws Throwable {
        List<Operation> posts = new ArrayList<>();
        for (Service s : services) {
            if (!(s instanceof FactoryService)) {
                String message = String
                        .format("Service %s is not a FactoryService", s.getClass().getSimpleName());
                throw new IllegalArgumentException(message);
            }
            URI u = null;
            if (ReflectionUtils.hasField(s.getClass(), UriUtils.FIELD_NAME_SELF_LINK)) {
                u = UriUtils.buildUri(this, s.getClass());
            } else {
                Class<? extends Service> childClass = ((FactoryService) s).createServiceInstance()
                        .getClass();
                if (ReflectionUtils.hasField(childClass, UriUtils.FIELD_NAME_FACTORY_LINK)) {
                    u = UriUtils.buildFactoryUri(this, childClass);
                }
                if (u == null) {
                    String message = String
                            .format("%s field not found in class %s and %s field not found in class %s",
                                    UriUtils.FIELD_NAME_SELF_LINK, s.getClass().getSimpleName(),
                                    UriUtils.FIELD_NAME_FACTORY_LINK,
                                    childClass.getSimpleName());
                    throw new IllegalArgumentException(message);
                }
            }
            Operation startPost = Operation.createPost(u);
            posts.add(startPost);
        }
        startCoreServicesSynchronously(posts, Arrays.asList(services));
    }

    protected void startCoreServicesSynchronously(Service... services) throws Throwable {
        List<Operation> posts = new ArrayList<>();
        for (Service s : services) {
            URI u = null;
            if (ReflectionUtils.hasField(s.getClass(), UriUtils.FIELD_NAME_SELF_LINK)) {
                u = UriUtils.buildUri(this, s.getClass());
            } else if (s instanceof FactoryService) {
                u = UriUtils.buildFactoryUri(this,
                        ((FactoryService) s).createServiceInstance().getClass());
            } else {
                throw new IllegalStateException("field SELF_LINK or FACTORY_LINK is required");
            }
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

    protected void startFactoryChildServiceSynchronously(String factoryLink, ServiceDocument serviceState) throws Throwable {
        CountDownLatch latch = new CountDownLatch(1);
        Throwable[] failure = new Throwable[1];
        CompletionHandler comp = (o, e) -> {
            if (e != null) {
                failure[0] = e;
                log(Level.SEVERE, "Exception creating service %s:", e.toString());
                stop();
                latch.countDown();
                return;
            }
            log(Level.FINE, "started %s", o.getUri().getPath());
            this.coreServices.add(o.getUri().getPath());
            latch.countDown();
        };
        if (serviceState.documentSelfLink == null) {
            serviceState.documentSelfLink = nextUUID();
        }
        this.registerForServiceAvailability(comp, UriUtils.buildUriPath(factoryLink, serviceState.documentSelfLink));

        Operation post = Operation.createPost(UriUtils.buildUri(this, factoryLink))
                            .setBody(serviceState)
                            .setReferer(getUri());
        post.setAuthorizationContext(getSystemAuthorizationContext());
        sendRequest(post);

        if (!latch.await(getState().operationTimeoutMicros, TimeUnit.MICROSECONDS)) {
            throw new TimeoutException();
        }
        if (failure[0] != null) {
            throw failure[0];
        }
    }

    protected void setAuthorizationContext(AuthorizationContext context) {
        OperationContext.setAuthorizationContext(context);
    }

    /**
     * Subscribe to the service specified in the subscribe operation URI. Note that this won't
     * replay state: use the version of startSubscriptionService that takes the ServiceSubscriber
     * as an option to get that.
     */
    public URI startSubscriptionService(
            Operation subscribe,
            Consumer<Operation> notificationConsumer) {
        return startSubscriptionService(subscribe, notificationConsumer,
                ServiceSubscriber.create(false));
    }

    /**
     * Start a {@code ReliableSubscriptionService} service and using it as the target, subscribe to the
     * service specified in the subscribe operation URI. Note that this won't replay state:
     * use the version of startSubscriptionService that takes the ServiceSubscriber as an option
     * to get that.
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
            throw new IllegalArgumentException("subscribe operation is required");
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
            public void authorizeRequest(Operation op) {
                op.complete();
                return;
            }

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
                notificationTargetSelfLink = UriUtils.buildUriPath(ServiceUriPaths.CORE_CALLBACKS,
                        nextUUID());
            }
            if (request.usePublicUri) {
                subscriptionUri = UriUtils.buildPublicUri(this, notificationTargetSelfLink);
            } else {
                subscriptionUri = UriUtils.buildUri(this, notificationTargetSelfLink);
            }
        }

        if (request.documentExpirationTimeMicros != 0) {
            long delta = request.documentExpirationTimeMicros - Utils.getSystemNowMicrosUtc();
            if (delta <= 0) {
                log(Level.WARNING, "Expiration time is in the past: %d",
                        request.documentExpirationTimeMicros);
                subscribe.fail(new CancellationException("Subscription has already expired"));
                return null;
            }

            scheduleCore(() -> {
                sendRequest(Operation.createDelete(
                        UriUtils.buildUri(this, notificationTarget.getSelfLink()))
                        .transferRefererFrom(subscribe));
            }, delta, TimeUnit.MICROSECONDS);
        }

        if (request.reference == null) {
            request.reference = subscriptionUri;
        } else {
            subscriptionUri = request.reference;
        }

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
                .setBodyNoCloning(unSubscribeBody)
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
                .transferRefererFrom(unsubscribe)
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

    boolean isServiceStarting(Service service, String path) {
        if (service != null) {
            return isServiceStarting(service.getProcessingStage());
        }

        if (path != null) {
            return false;
        }

        throw new IllegalArgumentException("service or path is required");
    }

    /**
     * Start a service using the default start operation.
     * @param service the service to start
     * @return the service host
     */
    public ServiceHost startService(Service service) {
        Operation post = Operation.createPost(UriUtils.buildUri(this, service.getClass()));
        return startService(post, service);
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

        if (!post.hasReferer()) {
            post.setReferer(post.getUri());
        }

        service.setHost(this);

        URI serviceUri = post.getUri().normalize();
        String servicePath = UriUtils.normalizeUriPath(serviceUri.getPath());

        if (servicePath.endsWith(UriUtils.URI_WILDCARD_CHAR)) {
            post.fail(new IllegalArgumentException(
                    "service path must not end in wild card character: " + servicePath));
            return this;
        }
        if (service.getSelfLink() == null) {
            service.setSelfLink(servicePath);
        }

        if (post.getExpirationMicrosUtc() == 0) {
            post.setExpiration(Utils.fromNowMicrosUtc(this.state.operationTimeoutMicros));
        }

        // if the service is a helper for one of the known URI suffixes, do not
        // add it to the map. We will special case dispatching to it
        if (isHelperServicePath(servicePath)) {
            // do not directly attach utility services
            if (!service.hasOption(Service.ServiceOption.UTILITY)) {
                String errorMsg = "Service is using an utility URI path but has not enabled "
                        + ServiceOption.UTILITY;
                log(Level.WARNING, errorMsg);
                post.fail(new IllegalStateException(errorMsg));
                return this;
            }
        } else if (checkIfServiceExistsAndAttach(service, servicePath, post)) {
            // service exists, do not proceed with start
            return this;
        }

        try {
            service.setProcessingStage(ProcessingStage.CREATED);
        } catch (Throwable t) {
            log(Level.SEVERE, "Unhandled error: %s", Utils.toString(t));
            post.fail(t);
            return this;
        }

        // make sure we detach the service on start failure
        post.nestCompletion((o, e) -> {
            this.operationTracker.removeStartOperation(post);
            if (e == null) {
                post.complete();
                return;
            }
            stopService(service);
            this.serviceSynchTracker.failStartServiceOrSynchronize(service, post, o, e);
        });

        this.operationTracker.trackStartOperation(post);
        if (!Utils.validateServiceOptions(this, service, post)) {
            return this;
        }

        if (this.isAuthorizationEnabled() && post.getAuthorizationContext() == null) {
            populateAuthorizationContext(post, authorizationContext -> {
                // kick off service start state machine
                processServiceStart(ProcessingStage.INITIALIZING, service, post, post.hasBody());
            });
        } else {
            // kick off service start state machine
            processServiceStart(ProcessingStage.INITIALIZING, service, post, post.hasBody());
        }
        return this;
    }

    /**
     * Starts a default factory service for the given instance service. Note that this will not start the instance
     * service.
     * @param instanceService the instance service whose factory service should be started
     * @return the service host
     */
    public ServiceHost startFactory(Service instanceService) {
        final Class<? extends Service> serviceClass = instanceService.getClass();
        return startFactory(serviceClass,
                () -> FactoryService.create(serviceClass, instanceService.getStateType()));
    }

    /**
     * Starts a factory service for the given instance service class using the provided factory creator
     * on the factory's default URI path.
     * @param instServiceClass the class of the instance service
     * @param factoryCreator a function which creates a factory service
     * @return the service host
     */
    public ServiceHost startFactory(Class<? extends Service> instServiceClass,
            Supplier<FactoryService> factoryCreator) {
        URI factoryUri = UriUtils.buildFactoryUri(this, instServiceClass);
        return startFactory(factoryCreator, factoryUri.getPath());
    }

    /**
     * Starts a factory service using the provided factory creator and the provided factory URI.
     * This is helpful for starting a factory with a custom path.
     * @param factoryCreator a function which creates a factory service
     * @param servicePath the path to use for the factory
     * @return the service host
     */
    public ServiceHost startFactory(Supplier<FactoryService> factoryCreator, String servicePath) {
        Operation post = Operation.createPost(UriUtils.buildUri(this, servicePath));
        FactoryService factoryService = factoryCreator.get();
        return startService(post, factoryService);
    }

    /**
     * Starts an idempotent factory service for the given instance service. Note that this will not start the
     * instance service.
     * @param instanceService the instance service whose factory service should be started
     * @return the service host
     */
    public ServiceHost startIdempotentFactory(Service instanceService) {
        final Class<? extends Service> serviceClass = instanceService.getClass();
        return startFactory(serviceClass,
                () -> FactoryService.createIdempotent(serviceClass));
    }

    void processPendingServiceAvailableOperations(Service s, Throwable e, boolean logFailure) {
        if (logFailure && !isStopping() && e != null) {
            log(Level.WARNING, "Service %s failed start: %s", s.getSelfLink(),
                    e.toString());
        }

        // even if service failed to start, immediately process any operations registered
        // for service available. If one of them is to start the service, its given a chance to try.
        // The alternative is to just let these operations timeout.
        SortedSet<Operation> ops = null;
        synchronized (this.state) {
            ops = this.operationTracker.removeServiceAvailableCompletions(s.getSelfLink());
            if (ops == null || ops.isEmpty()) {
                return;
            }
        }

        if (e != null && logFailure) {
            log(Level.INFO, "Retrying %d operations waiting on failed start for %s", ops.size(),
                    s.getSelfLink());
        }

        // Complete all. Any updates or GETs will get re-queued if the service is not going to ever
        // start, but any POSTs, or IDEMPOTENT POSTs -> PUT will attempt to start the service
        for (Operation op : ops) {
            run(() -> {
                if (op.getUri() == null) {
                    op.setUri(s.getUri());
                }
                if (e != null && op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_POST_TO_PUT)) {
                    restoreActionOnChildServiceToPostOnFactory(s.getSelfLink(), op);
                }
                op.complete();
            });
        }
    }

    private void restoreActionOnChildServiceToPostOnFactory(String link, Operation op) {
        log(Level.FINE, "Changing URI for (id:%d) %s from %s to factory",
                op.getId(), op.getAction(), link);
        // restart a PUT to a child service, to a POST to the factory
        op.removePragmaDirective(Operation.PRAGMA_DIRECTIVE_POST_TO_PUT);
        String factoryPath = UriUtils.getParentPath(link);
        op.setUri(UriUtils.buildUri(this, factoryPath));
        op.setAction(Action.POST);
    }

    private boolean checkIfServiceExistsAndAttach(Service service, String servicePath,
            Operation post) {
        boolean isCreateOrSynchRequest = post.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_CREATED)
                || post.isSynchronize();
        Service existing = null;
        boolean synchPendingDelete = false;

        synchronized (this.state) {
            existing = this.attachedServices.get(servicePath);
            if (existing == null &&
                    this.pendingServiceDeletions.contains(servicePath) &&
                    post.isSynchronizeOwner()) {
                // We may receive a synch request while a delete is being processed.
                // If we don't look at pendingServiceDeletions, we may end up starting
                // a service that is in the deletion phase.
                synchPendingDelete = true;
            } else if (existing != null &&
                    existing.getProcessingStage() == ProcessingStage.STOPPED &&
                    post.isSynchronizeOwner()) {
                // Same as above. We might be in the middle of stopping the service.
                synchPendingDelete = true;
            } else {
                if (existing != null) {
                    if (isCreateOrSynchRequest
                            && existing.getProcessingStage() == ProcessingStage.STOPPED) {
                        // service was just stopped and about to be removed. We are creating a new instance, so
                        // its fine to re-attach. We will do a state version check if this is a persisted service
                        existing = null;
                    }
                }

                if (existing == null) {
                    this.attachedServices.put(servicePath, service);
                    if (service.hasOption(ServiceOption.URI_NAMESPACE_OWNER)) {
                        this.attachedNamespaceServices.put(servicePath, service);
                    }

                    if (service.hasOption(ServiceOption.REPLICATION)
                            && service.hasOption(ServiceOption.FACTORY)) {
                        this.serviceSynchTracker.addService(servicePath, 0L);
                    }
                    this.state.serviceCount++;
                    return false;
                }
            }
        }

        if (synchPendingDelete) {
            // If this is a synch request and the service was going
            // through deletion, we fail the synch request.
            Operation.failServiceMarkedDeleted(servicePath, post);
            return true;
        }

        boolean isIdempotent = service.hasOption(ServiceOption.IDEMPOTENT_POST);
        if (!isIdempotent) {
            // check factory, its more likely to have the IDEMPOTENT option
            String parentPath = UriUtils.getParentPath(servicePath);
            Service parent = parentPath != null ? findService(parentPath) : null;
            isIdempotent = parent != null
                    && parent.hasOption(ServiceOption.IDEMPOTENT_POST);
        }

        if (!isIdempotent && !post.isSynchronize()) {
            ProcessingStage ps = existing.getProcessingStage();
            if (ps == ProcessingStage.STOPPED || ServiceHost.isServiceStarting(ps)) {
                // there is a possibility of collision with a synchronization attempt: The sync task
                // attaches a child it enumerated from a peer, starts in stage CREATED while loading
                // state from index, and then discovers service is deleted. In the meantime a legitimate
                // re-start (a POST following a DELETE, with version > delete version) arrives and since
                // the service is attached, can fail with conflict. To avoid this. retry. Retry is bounded
                // since sync task will fail its attempt if the service is marked deleted
                log(Level.INFO, "Retrying (%d) startService() POST to %s in stage %s",
                        post.getId(),
                        servicePath, existing.getProcessingStage());
                scheduleCore(() -> {
                    startService(post, service);
                }, this.getMaintenanceIntervalMicros(), TimeUnit.MICROSECONDS);
                return true;
            }
            // service already attached, not idempotent, and this is not a synchronization attempt.
            // We fail request with conflict
            failRequestServiceAlreadyStarted(servicePath, service, post);
            return true;
        }

        if (!isCreateOrSynchRequest) {
            // This is a restart, do nothing, service already attached. We should have sent a PUT, but this
            // can happen if a service is just starting. This means it will replicate and there is
            // no need for explicit synch
            post.complete();
            return true;
        }

        if (existing.getProcessingStage() != ProcessingStage.AVAILABLE) {
            restoreActionOnChildServiceToPostOnFactory(servicePath, post);
            log(Level.FINE, "Retrying (%d) POST to idempotent %s in stage %s",
                    post.getId(),
                    servicePath, existing.getProcessingStage());
            // Service is in the process of starting or stopping. Retry at a later time.
            scheduleCore(() -> {
                handleRequest(null, post);
            }, this.getMaintenanceIntervalMicros(), TimeUnit.MICROSECONDS);
            return true;
        }

        log(Level.FINE, "Converting (%d) POST to PUT for idempotent %s in stage %s",
                post.getId(),
                servicePath, existing.getProcessingStage());

        // service exists, on IDEMPOTENT factory. Convert to a PUT
        post.setAction(Action.PUT);
        post.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_POST_TO_PUT);

        handleRequest(null, post);
        return true;
    }

    public static boolean isServiceIndexed(Service s) {
        return s.hasOption(ServiceOption.PERSISTENCE);
    }

    public static boolean isServiceOnDemandLoad(Service s) {
        return s.hasOption(ServiceOption.ON_DEMAND_LOAD);
    }

    public static boolean isServiceImmutable(Service s) {
        return s.hasOption(ServiceOption.IMMUTABLE);
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
                final ProcessingStage nextStage = isServiceIndexed(s)
                        ? ProcessingStage.LOADING_INITIAL_STATE : ProcessingStage.SYNCHRONIZING;

                buildDocumentDescription(s);
                if (post.hasBody()) {
                    // make sure body is in native form and has creation time
                    ServiceDocument d = post.getBody(s.getStateType());
                    d.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
                }

                // Populate authorization context if necessary
                if (this.isAuthorizationEnabled() &&
                        this.authorizationService != null &&
                        this.authorizationService
                                .getProcessingStage() == ProcessingStage.AVAILABLE) {
                    post.nestCompletion(op -> {
                        processServiceStart(nextStage, s, post, hasClientSuppliedInitialState);
                    });
                    queueOrScheduleRequest(this.authorizationService, post);
                    break;
                }

                processServiceStart(nextStage, s, post, hasClientSuppliedInitialState);
                break;
            case LOADING_INITIAL_STATE:
                boolean isImmutableStart = ServiceHost.isServiceCreate(post)
                        && isServiceImmutable(s);
                if (!isImmutableStart && isServiceIndexed(s) && !post.isFromReplication()) {
                    // Skip querying the index for existing state if any of the following is true:
                    // 1) Service is marked IMMUTABLE. This means no previous version should exist,
                    //     its up to the client to enforce unique links
                    // 2) Request is from replication. This means state is already attached to the
                    //     request
                    // 3) Service is NOT indexed.
                    loadInitialServiceState(s, post, ProcessingStage.SYNCHRONIZING,
                            hasClientSuppliedInitialState);
                } else {
                    processServiceStart(ProcessingStage.SYNCHRONIZING, s, post,
                            hasClientSuppliedInitialState);
                }
                break;
            case SYNCHRONIZING:
                ProcessingStage nxt = isServiceCreate(post)
                        ? ProcessingStage.EXECUTING_CREATE_HANDLER
                        : ProcessingStage.EXECUTING_START_HANDLER;
                if (s.hasOption(ServiceOption.FACTORY) || !s.hasOption(ServiceOption.REPLICATION)) {
                    processServiceStart(nxt, s, post, hasClientSuppliedInitialState);
                    break;
                }

                post.nestCompletion((o) -> {
                    boolean hasInitialState = hasClientSuppliedInitialState;
                    if (!hasInitialState && o.getLinkedState() != null) {
                        hasInitialState = true;
                    }
                    processServiceStart(nxt, s, post,
                            hasInitialState);
                });

                selectServiceOwnerAndSynchState(s, post);
                break;

            case EXECUTING_CREATE_HANDLER:
                post.nestCompletion((o) -> {
                    processServiceStart(ProcessingStage.EXECUTING_START_HANDLER, s, post,
                            hasClientSuppliedInitialState);
                });

                if (!isDocumentOwner(s)) {
                    // Bypass handleCreate on nodes that do not own the service. We still proceed
                    // to EXECUTING_START_HANDLER since there is some state related logic
                    // that needs to execute, regardless of owner
                    post.complete();
                    break;
                }

                if (post.isFromReplication()) {
                    // Only direct request from clients are eligible for handleCreate
                    post.complete();
                    break;
                }

                OperationContext opCtx = extractAndApplyContext(post);
                try {
                    s.adjustStat(Service.STAT_NAME_CREATE_COUNT, 1);
                    s.handleCreate(post);
                } catch (Throwable e) {
                    handleUncaughtException(s, post, e);
                    return;
                } finally {
                    OperationContext.restoreOperationContext(opCtx);
                }

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
                    } else {
                        document = new ServiceDocument();
                        document.documentSelfLink = s.getSelfLink();
                    }
                    if (!isAuthorized(s, document, post)) {
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

                if (!post.hasBody()
                        && ServiceHost.isServiceOnDemandLoad(s)
                        && post.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK)) {
                    // skip handleStart for ODL probes (the POST was issued to check if the service
                    // existed
                    post.complete();
                    return;
                }

                opCtx = extractAndApplyContext(post);
                try {
                    s.handleStart(post);
                } catch (Throwable e) {
                    handleUncaughtException(s, post, e);
                    return;
                } finally {
                    OperationContext.restoreOperationContext(opCtx);
                }
                break;
            case INDEXING_INITIAL_STATE:
                boolean needsIndexing = false;

                if (isServiceIndexed(s) && !s.hasOption(ServiceOption.FACTORY)) {
                    // we only index if this is a synchronization request from
                    // a remote peer, or this is a new "create", brand new service start.
                    if (post.isSynchronizePeer() || hasClientSuppliedInitialState) {
                        needsIndexing = true;
                    }
                }

                post.nestCompletion(o -> {
                    processServiceStart(ProcessingStage.REPLICATE_STATE, s, post,
                            hasClientSuppliedInitialState);
                });

                if (post.hasBody()) {
                    ServiceDocument state = (ServiceDocument) post.getBodyRaw();
                    if (state != null && state.documentKind == null) {
                        log(Level.WARNING, "documentKind is null for %s", s.getSelfLink());
                        state.documentKind = Utils.buildKind(s.getStateType());
                    }

                    // Skip caching for replication requests if the service
                    // is indexed.
                    boolean skipCaching = post.isFromReplication() &&
                            isServiceIndexed(s);
                    if (!skipCaching) {
                        this.serviceResourceTracker.updateCachedServiceState(s,
                                state, post);
                    }
                }

                if (!post.hasBody() || !needsIndexing) {
                    post.complete();
                    break;
                }

                if (post.isFromReplication()) {
                    post.linkSerializedState(null);
                }

                ServiceDocument state = (ServiceDocument) post.getBodyRaw();
                saveServiceState(s, post, state);
                break;
            case REPLICATE_STATE:
                // The state should be replicated only if it's a POST
                // request from the FactoryService for a replicated service.
                // If this was a replication request from the owner node or
                // a POST converted to a PUT, we avoid replication and
                // directly jump to STARTED stage.
                boolean shouldReplicate = isServiceCreate(post) &&
                        post.getAction() == Action.POST &&
                        s.hasOption(ServiceOption.REPLICATION) &&
                        !post.isFromReplication() &&
                        !post.isReplicationDisabled();

                if (!shouldReplicate) {
                    processServiceStart(ProcessingStage.AVAILABLE, s, post,
                            hasClientSuppliedInitialState);
                    return;
                }

                String factoryPath = post.getAndRemoveRequestHeaderAsIs(
                        Operation.REPLICATION_PARENT_HEADER);
                post.setUri(UriUtils.buildUri(this, factoryPath));

                ServiceDocument initialState = post.getBody(s.getStateType());
                final ServiceDocument clonedInitState = Utils.clone(initialState);

                // The factory services on the remote nodes must see the request body as it was before it
                // was fixed up by this instance. Restore self link to be just the child suffix "hint", removing the
                // factory prefix added upstream.
                String originalLink = clonedInitState.documentSelfLink;
                clonedInitState.documentSelfLink = originalLink.replace(factoryPath, "");

                post.nestCompletion((replicatedOp) -> {
                    clonedInitState.documentSelfLink = originalLink;
                    post.setBodyNoCloning(clonedInitState);
                    processServiceStart(ProcessingStage.AVAILABLE, s, post,
                            hasClientSuppliedInitialState);
                });

                // if limited replication is used for this service, supply a selection key, the fully qualified service link
                // so the same set of nodes get selected for the POST to create the service, as the nodes chosen
                // for subsequent updates to the child service
                post.linkState(clonedInitState);
                this.replicateRequest(s.getOptions(), clonedInitState, s.getPeerNodeSelectorPath(),
                        originalLink, post);
                break;
            case AVAILABLE:
                // It's possible a service is stopped before it transitions to available
                if (s.getProcessingStage() == ProcessingStage.STOPPED) {
                    post.complete();
                    return;
                }

                s.setProcessingStage(Service.ProcessingStage.AVAILABLE);
                if (!isServiceImmutable(s)) {
                    startUiFileContentServices(s);
                    scheduleServiceMaintenance(s);
                }
                post.complete();

                break;

            default:
                break;

            }
        } catch (Throwable e) {
            log(Level.SEVERE, "Unhandled error: %s", Utils.toString(e));
            post.fail(e);
        }
    }

    private OperationContext extractAndApplyContext(Operation op) {
        OperationContext opCtx = OperationContext.getOperationContext();
        OperationContext.setFrom(op);
        return opCtx;
    }

    boolean isDocumentOwner(Service s) {
        return !s.hasOption(ServiceOption.OWNER_SELECTION) ||
                s.hasOption(ServiceOption.DOCUMENT_OWNER);
    }

    /**
     * Invoke the service setInitialState method and ensures the state has proper self link and
     * kind. If the service is not marked with {@link ServiceOption#IMMUTABLE}, the state
     * is serialized to JSON to verify serialization is possible, and cloned
     */
    void normalizeInitialServiceState(Service s, Operation post, Long finalVersion) {
        if (!post.hasBody()) {
            return;
        }
        // We force serialize to JSON to clone
        // and prove the state *is* convertible to JSON. It also forces type
        // to the service state type through type coercion
        Object body = post.getBodyRaw();
        if (!body.getClass().equals(s.getStateType())) {
            body = Utils.toJson(body);
        }
        ServiceDocument initialState = s.setInitialState(
                body,
                finalVersion);

        initialState.documentSelfLink = s.getSelfLink();
        initialState.documentKind = Utils.buildKind(initialState.getClass());
        initialState.documentAuthPrincipalLink = (post.getAuthorizationContext() != null) ? post
                .getAuthorizationContext().getClaims().getSubject() : null;

        if (!isServiceImmutable(s)) {
            initialState = Utils.clone(initialState);
        }
        post.setBodyNoCloning(initialState);
    }

    /**
     * Infrastructure use only.
     *
     * Called on demand or due to node group changes to synchronize replicated services
     * associated with the specified node selector path
     */
    public void scheduleNodeGroupChangeMaintenance(String nodeSelectorPath) {
        this.serviceSynchTracker.scheduleNodeGroupChangeMaintenance(nodeSelectorPath);
    }

    void loadServiceState(Service s, Operation op) {
        ServiceDocument state = this.serviceResourceTracker.getCachedServiceState(s, op);

        // Clone state if it might change while processing
        if (state != null && !s.hasOption(ServiceOption.CONCURRENT_UPDATE_HANDLING)) {
            state = Utils.clone(state);
        }

        if (state != null && state.documentKind == null) {
            log(Level.WARNING, "documentKind is null for %s", s.getSelfLink());
            state.documentKind = Utils.buildKind(s.getStateType());
        }

        // If either there is cached state, or the service is not indexed (meaning nothing
        // will be found in the index), subject this state to authorization.
        if (state != null || !isServiceIndexed(s)) {
            if (!isAuthorized(s, state, op)) {
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

        Operation getOp = Operation.createGet(op.getUri())
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK)
                .transferRefererFrom(op)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }

                    if (!o.hasBody()) {
                        Operation.failServiceNotFound(op);
                        return;
                    }

                    ServiceDocument st = o.getBody(s.getStateType());
                    if (!isAuthorized(s, st, op)) {
                        op.fail(Operation.STATUS_CODE_FORBIDDEN);
                        return;
                    }

                    op.linkState(st).complete();
                });

        Service indexService = getIndexServiceForService(s);

        if (indexService == null) {
            op.fail(new CancellationException());
            return;
        }

        indexService.handleRequest(getOp);
    }

    private Service getIndexServiceForService(Service s) {
        Service indexService = this.documentIndexService;
        if (s.getDocumentIndexPath() != null && ServiceUriPaths.CORE_DOCUMENT_INDEX.hashCode() != s
                .getDocumentIndexPath().hashCode()) {
            indexService = this.findService(s.getDocumentIndexPath());
        }
        return indexService;
    }

    /**
     * Infrastructure use. Applies authorization policy on the supplied document and fails the
     * operation if authorization fails
     * @return True if request was authorized, false otherwise
     */
    public boolean isAuthorized(Service service, ServiceDocument document, Operation op) {
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

        try {
            ServiceDocumentDescription documentDescription = buildDocumentDescription(service);
            QueryFilter queryFilter = ctx.getResourceQueryFilter(op.getAction());
            if (queryFilter == null || !queryFilter.evaluate(document, documentDescription)) {
                return false;
            }
        } catch (Throwable t) {
            log(Level.SEVERE, "Unexpected failure during authorization check. %s", t.toString());
            return false;
        }

        return true;
    }

    void loadInitialServiceState(Service s, Operation serviceStartPost, ProcessingStage next,
            boolean hasClientSuppliedState) {
        Service indexService = getIndexServiceForService(s);
        if (indexService == null) {
            serviceStartPost.fail(new CancellationException());
            return;
        }

        Operation getLatestState = Operation.createGet(serviceStartPost.getUri())
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK)
                .transferRefererFrom(serviceStartPost);

        getLatestState.setCompletion((indexQueryOperation, e) -> {
            handleLoadInitialStateCompletion(s, serviceStartPost, next,
                    hasClientSuppliedState,
                    indexQueryOperation, e);
        });
        indexService.handleRequest(getLatestState);
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

        if (op != null && op.getAction() == Action.DELETE) {
            return;
        }

        if (st != null && st.documentKind == null) {
            log(Level.WARNING, "documentKind is null for %s", s.getSelfLink());
            st.documentKind = Utils.buildKind(s.getStateType());
        }
        this.serviceResourceTracker.updateCachedServiceState(s, st, op);
    }

    void clearTransactionalCachedServiceState(Service s, String transactionId) {
        this.serviceResourceTracker.clearTransactionalCachedServiceState(s.getSelfLink(),
                transactionId);
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

        if (!checkServiceExistsOrDeleted(s, stateFromStore, serviceStartPost)) {
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

    private boolean checkServiceExistsOrDeleted(Service s, ServiceDocument stateFromStore,
            Operation serviceStartPost) {
        if (!serviceStartPost.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERSION_CHECK)) {
            return true;
        }

        if (serviceStartPost.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)) {
            return true;
        }

        if (stateFromStore == null) {
            return true;
        }

        boolean isDeleted = ServiceDocument.isDeleted(stateFromStore)
                || this.pendingServiceDeletions.contains(s.getSelfLink());

        if (isDeleted && serviceStartPost.isSynchronizeOwner()) {
            return true;
        }

        if (!serviceStartPost.hasBody()) {
            if (isDeleted) {
                // this POST is due to a restart which will never have a body
                Operation.failServiceMarkedDeleted(stateFromStore.documentSelfLink,
                        serviceStartPost);
                return false;
            } else {
                // this POST is due to a restart, which will never have a body
                // service is not deleted we can restart it
                return true;
            }
        }
        ServiceDocument initState = (ServiceDocument) serviceStartPost.getBodyRaw();
        if (isDeleted) {
            if (stateFromStore.documentVersion < initState.documentVersion) {
                // new state is higher than previously indexed state, allow restart
                return true;
            } else {
                log(Level.WARNING,
                        " (%d) Attempt to start deleted service %s.Version: %d, in body: %d (%s)",
                        serviceStartPost.getId(),
                        stateFromStore.documentSelfLink,
                        stateFromStore.documentVersion,
                        initState.documentVersion,
                        serviceStartPost.getRequestHeaderAsIs(Operation.PRAGMA_HEADER));
                Operation.failServiceMarkedDeleted(stateFromStore.documentSelfLink,
                        serviceStartPost);
                return false;
            }
        }

        if (!s.hasOption(ServiceOption.IDEMPOTENT_POST)) {
            // ON_DEMAND_LOAD services might not be present in the attachedService map, but will
            // exist in the index. This is an attempt to start such a service that already exists,
            // operation
            log(Level.WARNING, "Attempt to start existing service %s.Version: %d, in body: %d",
                    stateFromStore.documentSelfLink,
                    stateFromStore.documentVersion,
                    initState.documentVersion);
            failRequestServiceAlreadyStarted(s.getSelfLink(), s, serviceStartPost);
            return false;
        }

        return true;
    }

    void markAsPendingDelete(Service service) {
        if (isServiceIndexed(service)) {
            this.pendingServiceDeletions.add(service.getSelfLink());
            this.managementService.adjustStat(
                    ServiceHostManagementService.STAT_NAME_PENDING_SERVICE_DELETION_COUNT, 1);
        }
    }

    void unmarkAsPendingDelete(Service service) {
        if (isServiceIndexed(service)) {
            this.pendingServiceDeletions.remove(service.getSelfLink());
            this.managementService.adjustStat(
                    ServiceHostManagementService.STAT_NAME_PENDING_SERVICE_DELETION_COUNT, -1);

        }
    }

    /**
     * Infrastructure use only. Service authors should never call this method.
     * To stop a service issue a DELETE operation to its a URI. To only stop but not
     * mark as deleted in the index, use {@link Operation#PRAGMA_DIRECTIVE_NO_INDEX_UPDATE}
     *
     * Detaches service from service host, sets processing stage to stop.
     */
    public void stopService(Service service) {
        if (service == null) {
            throw new IllegalArgumentException("service is required");
        }
        stopService(service.getSelfLink());
    }

    private void stopService(String path) {
        EnumSet<ServiceOption> options = null;
        synchronized (this.state) {
            Service existing = this.attachedServices.remove(path);
            if (existing == null) {
                path = UriUtils.normalizeUriPath(path);
                existing = this.attachedServices.remove(path);
            }

            if (existing != null) {
                options = existing.getOptions();
                existing.setProcessingStage(ProcessingStage.STOPPED);
                if (existing.hasOption(ServiceOption.URI_NAMESPACE_OWNER)) {
                    this.attachedNamespaceServices.remove(path);
                }
            }

            this.serviceSynchTracker.removeService(path);
            this.serviceResourceTracker.clearCachedServiceState(existing, path, null);
            this.pendingPauseServices.remove(path);

            this.state.serviceCount--;
        }

        // we do not remove from maintenance tracker, service will
        // be ignored and never schedule for maintenance if its stopped
        if (options == null || this.managementService == null) {
            return;
        }

        if (options.contains(ServiceOption.ON_DEMAND_LOAD)) {
            this.managementService.adjustStat(
                    ServiceHostManagementService.STAT_NAME_ODL_STOP_COUNT,
                    1);
        }

    }

    protected Service findService(String uriPath) {
        return findService(uriPath, true);
    }

    protected Service findService(String uriPath, boolean doExactMatch) {
        Service s = this.attachedServices.get(uriPath);
        if (s != null) {
            return s;
        }

        String normalizedUriPath = UriUtils.normalizeUriPath(uriPath);
        // Check if we got a new normalized uri path
        if (!normalizedUriPath.equals(uriPath)) {
            s = this.attachedServices.get(normalizedUriPath);
            if (s != null) {
                return s;
            }
        }

        if (isHelperServicePath(uriPath)) {
            s = findHelperService(uriPath);
            if (s != null) {
                return s;
            }
        }

        if (!doExactMatch) {
            s = findNamespaceOwnerService(uriPath);
        }

        return s;
    }

    private Service findNamespaceOwnerService(String uriPath) {
        // TODO We do not expect a lot of name space owner services, but we should switch to
        // radix trees
        int charsNotMatched = Integer.MAX_VALUE;
        int uriPathLength = uriPath.length();
        Service candidate = null;
        // pick the service with the longest match
        for (Entry<String, Service> e : this.attachedNamespaceServices.headMap(uriPath, true).entrySet()) {
            if (!uriPath.startsWith(e.getKey())) {
                continue;
            }
            int notMatchedCount = uriPathLength - e.getKey().length();
            if (notMatchedCount < charsNotMatched) {
                candidate = e.getValue();
                charsNotMatched = notMatchedCount;
            }
        }

        return candidate;
    }

    Service findHelperService(String uriPath) {
        String subPath;

        int i = uriPath.indexOf(SERVICE_URI_SUFFIX_UI + "/");
        if (i > 0) {
            // catches the case of /service/ui/
            // but is smart to ignore /ui/abc
            subPath = uriPath.substring(0, i);
        } else {
            subPath = uriPath.substring(0, uriPath.lastIndexOf(UriUtils.URI_PATH_CHAR));
        }
        // use the prefix to find the actual service
        Service s = this.attachedServices.get(subPath);
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
            if (!isHostEqual(inboundOp.getUri())) {
                return false;
            }
        }

        if (!this.state.isStarted) {
            Operation.failServiceNotFound(inboundOp);
            return true;
        }

        if (this.isAuthorizationEnabled()) {
            checkAndPopulateAuthContext(service, inboundOp);
        } else {
            handleRequestWithAuthContext(service, inboundOp);
        }

        return true;
    }

    private void checkAndPopulateAuthContext(Service service, Operation inboundOp) {
        if (inboundOp.getAuthorizationContext() != null) {
            checkAndPopulateAuthzContext(service, inboundOp);
            return;
        }

        if (BasicAuthenticationUtils.getAuthToken(inboundOp) != null) {
            populateAuthorizationContext(inboundOp, (authorizationContext) -> {
                checkAndPopulateAuthzContext(service, inboundOp);
            });
            return;
        }

        // If the inbound op targets a valid authentication service, then allow it to proceed using
        // the guest context; this is needed so that clients can get the token.
        URI authServiceUri = getAuthenticationServiceUri();
        if (authServiceUri != null
                && authServiceUri.getPath().equals(inboundOp.getUri().getPath())) {
            populateAuthorizationContext(inboundOp, (authorizationContext) -> {
                checkAndPopulateAuthzContext(service, inboundOp);
            });
            return;
        }

        URI basicAuthServiceUri = getBasicAuthenticationServiceUri();
        if (basicAuthServiceUri != null
                && basicAuthServiceUri.getPath().equals(inboundOp.getUri().getPath())) {
            populateAuthorizationContext(inboundOp, authorizationContext -> {
                checkAndPopulateAuthzContext(service, inboundOp);
            });
            return;
        }

        // Dispatch the operation to the authentication service for handling.
        inboundOp.nestCompletion((op, ex) -> {
            if (ex != null) {
                inboundOp.setBodyNoCloning(op.getBodyRaw())
                        .setStatusCode(op.getStatusCode()).fail(ex);
                return;
            }
            // If the status code was anything but 200, and the operation
            // was not marked as failed, terminate the processing chain;
            // else proceed with the original request using the guest context
            if (op.getStatusCode() != Operation.STATUS_CODE_OK) {
                inboundOp.setBodyNoCloning(op.getBodyRaw())
                        .setStatusCode(op.getStatusCode()).complete();
                return;
            }
            populateAuthorizationContext(inboundOp, authorizationContext -> {
                checkAndPopulateAuthzContext(service, inboundOp);
            });
        });
        queueOrScheduleRequest(this.authenticationService, inboundOp);
    }

    private void checkAndPopulateAuthzContext(Service service, Operation inboundOp) {
        if (this.authorizationService != null) {
            inboundOp.nestCompletion(op -> {
                handleRequestWithAuthContext(null, inboundOp);
            });
            queueOrScheduleRequest(this.authorizationService, inboundOp);
        } else {
            handleRequestWithAuthContext(service, inboundOp);
        }
    }

    private void handleRequestWithAuthContext(Service service, Operation inboundOp) {
        String path;
        if (service == null) {
            path = inboundOp.getUri().getPath();
            if (path == null) {
                Operation.failServiceNotFound(inboundOp);
                return;
            }

            // request service using either prefix or longest match
            service = findService(path, false);
        } else {
            path = service.getSelfLink();
        }

        // if this service was about to stop, due to memory pressure, cancel, its still active
        Service pendingStopService = this.pendingPauseServices.remove(path);
        if (pendingStopService != null) {
            service = pendingStopService;
        }

        if (applyRequestRateLimit(service, inboundOp)) {
            return;
        }

        if (queueRequestUntilServiceAvailable(inboundOp, service, path)) {
            return;
        }

        if (queueOrForwardRequest(service, path, inboundOp)) {
            return;
        }

        if (service == null) {
            Operation.failServiceNotFound(inboundOp);
            return;
        }

        traceOperation(inboundOp);

        if (isAuthorizationEnabled()) {
            final Service sFinal = service;
            inboundOp.nestCompletion((o) -> {
                queueOrScheduleRequest(sFinal, inboundOp);
            });
            service.authorizeRequest(inboundOp);
            return;
        }

        queueOrScheduleRequest(service, inboundOp);
        return;
    }

    void getAuthorizationContext(Operation op, Consumer<AuthorizationContext> authorizationContextHandler) {
        String token = BasicAuthenticationUtils.getAuthToken(op);

        if (token == null) {
            authorizationContextHandler.accept(null);
            return;
        }

        AuthorizationContext ctx = this.authorizationContextCache.get(token);
        if (ctx != null) {
            ctx = checkAndGetAuthorizationContext(ctx, ctx.getClaims(), token, op);
            authorizationContextHandler.accept(ctx);
            return;
        }

        verifyToken(token, op, authorizationContextHandler);
    }

    private void verifyToken(String token, Operation op,
            Consumer<AuthorizationContext> authorizationContextHandler) {
        boolean shoudRetry = true;
        URI tokenVerificationUri = getAuthenticationServiceUri();

        if (getBasicAuthenticationServiceUri().equals(getAuthenticationServiceUri())) {
            // if authenticationService is BasicAuthenticationService, then no need to retry
            shoudRetry = false;
        }

        verifyTokenInternal(token, op, tokenVerificationUri, authorizationContextHandler, shoudRetry);
    }

    private void verifyTokenInternal(String token, Operation parentOp, URI tokenVerificationUri,
            Consumer<AuthorizationContext> authorizationContextHandler, boolean shouldRetry) {
        Operation verifyOp = Operation
                .createPost(tokenVerificationUri)
                .setReferer(parentOp.getUri())
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN)
                .addRequestHeader(Operation.REQUEST_AUTH_TOKEN_HEADER, token)
                .setCompletion(
                        (resultOp, ex) -> {
                            if (ex != null) {
                                log(Level.WARNING, "Error verifying token: %s", ex);
                                if (shouldRetry) {
                                    log(Level.INFO, "Retrying token verification with basic auth.");
                                    verifyTokenInternal(token, parentOp, getBasicAuthenticationServiceUri(),
                                            authorizationContextHandler, false);
                                } else {
                                    authorizationContextHandler.accept(null);
                                }
                            } else {
                                Claims claims = resultOp.getBody(Claims.class);
                                // check to see if the subject is valid
                                Operation getUserOp = Operation.createGet(
                                        AuthUtils.buildUserUriFromClaims(this, claims))
                                        .setReferer(parentOp.getUri())
                                        .setCompletion((getOp, getEx) -> {
                                            if (getEx != null) {
                                                log(Level.WARNING, "Error obtaining subject: %s", getEx);
                                                // return a null context. This will result in the auth context
                                                // for this operation defaulting to the guest context
                                                authorizationContextHandler.accept(null);
                                                return;
                                            }
                                            AuthorizationContext authCtx = checkAndGetAuthorizationContext(
                                                    null, claims, token, parentOp);
                                            authorizationContextHandler.accept(authCtx);
                                        });
                                getUserOp.setAuthorizationContext(getSystemAuthorizationContext());
                                sendRequest(getUserOp);
                            }
                        });
        verifyOp.setAuthorizationContext(getSystemAuthorizationContext());
        sendRequest(verifyOp);
    }

    private AuthorizationContext checkAndGetAuthorizationContext(AuthorizationContext ctx,
            Claims claims, String token, Operation op) {

        if (claims == null) {
            log(Level.INFO, "Request to %s has no claims found with token: %s",
                    op.getUri().getPath(), token);
            return null;
        }

        Long expirationTime = claims.getExpirationTime();
        if (expirationTime != null && TimeUnit.SECONDS.toMicros(expirationTime) <= Utils.getSystemNowMicrosUtc()) {
            synchronized (this.state) {
                this.authorizationContextCache.remove(token);
                this.userLinkToTokenMap.remove(claims.getSubject());
            }
            return null;
        }

        if (ctx != null) {
            return ctx;
        }

        AuthorizationContext.Builder b = AuthorizationContext.Builder.create();
        b.setClaims(claims);
        b.setToken(token);
        ctx = b.getResult();
        synchronized (this.state) {
            this.authorizationContextCache.put(token, ctx);
            addUserToken(this.userLinkToTokenMap, claims.getSubject(), token);
        }
        return ctx;
    }

    /**
     * Helper method to associate a token with a userServiceLink
     * @param userLinktoTokenMap map to add the entry to
     * @param userServiceLink the user service reference
     * @param token user token
     */
    private void addUserToken(Map<String, Set<String>> userLinktoTokenMap, String userServiceLink,
            String token) {
        Set<String> tokenSet = userLinktoTokenMap.get(userServiceLink);
        if (tokenSet == null) {
            tokenSet = new HashSet<String>();
        }
        tokenSet.add(token);
        userLinktoTokenMap.put(userServiceLink, tokenSet);
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

                // If this is a synchronization request, we should accept the ServiceDocument
                // in the request. The local node has an out-dated copy of the document
                // which is why we are receiving this request in the first place.
                if (op.isSynchronizePeer()) {
                    Service factory = findService(
                            UriUtils.getParentPath(op.getUri().getPath()));
                    if (factory != null) {
                        Service childService;
                        try {
                            childService = ((FactoryService) factory).createServiceInstance();
                        } catch (Throwable t) {
                            op.fail(t);
                            return true;
                        }
                        saveServiceState(childService, op, op.getBody(childService.getStateType()));
                    }
                } else {
                    op.complete();
                }
            } else {
                Operation.failServiceNotFound(op);
            }
            return true;
        }

        Service parent = null;
        EnumSet<ServiceOption> options;
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
                    if (op.getRetryCount() == 0) {
                        op.setRetryCount(1);
                    }
                    if (op.decrementRetriesRemaining() >= 0) {
                        log(Level.WARNING, "Parent for %s missing, retrying", op.getUri().getPath());
                        retryPauseOrOnDemandLoadConflict(op, false);
                        return true;
                    }
                    Operation.failServiceNotFound(op);
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
                Operation.failServiceNotFound(op);
                return true;
            }

            parent = findService(factoryPath);
            if (parent == null) {
                Operation.failServiceNotFound(op);
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

        return selectAndForwardRequestToOwner(s, path, op, parent, options);
    }

    private boolean selectAndForwardRequestToOwner(Service s, String path, Operation op,
            Service parent,
            EnumSet<ServiceOption> options) {

        if (options.contains(ServiceOption.ON_DEMAND_LOAD) &&
                op.getAction() == Action.DELETE &&
                op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)) {
            // This request was to stop an ODL service as part of reducing Xenon's
            // memory foot-print (See ServiceResourceTracker). So we will avoid forwarding
            // the request to the owner and instead just stop the local service instance.
            if (s == null) {
                op.complete();
                return true;
            }
            return false;
        }

        String nodeSelectorPath;
        if (parent != null) {
            nodeSelectorPath = parent.getPeerNodeSelectorPath();
        } else {
            nodeSelectorPath = s.getPeerNodeSelectorPath();
        }

        op.setStatusCode(Operation.STATUS_CODE_OK);

        String servicePath = path;
        Service parentService = parent;
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
                if (rsp.isLocalHostOwner) {
                    Operation.failOwnerMismatch(op, rsp.ownerNodeId, body);
                    return;
                }

                queueOrScheduleRequest(s, op);
                return;
            }

            forwardRequestToOwner(s, op, servicePath, parentService, rsp);
        };

        Operation selectOwnerOp = Operation
                .createPost(null)
                .setExpiration(op.getExpirationMicrosUtc())
                .setCompletion(ch);
        selectOwner(nodeSelectorPath, path, selectOwnerOp);
        return true;
    }

    private void forwardRequestToOwner(Service s, Operation op, String servicePath,
            Service parentService, SelectOwnerResponse rsp) {
        CompletionHandler fc = (fo, fe) -> {
            if (fe != null) {
                retryOrFailRequest(op, fo, fe);
                return;
            }

            op.setStatusCode(fo.getStatusCode());
            if (fo.hasBody()) {
                op.setBodyNoCloning(fo.getBodyRaw());
            }

            op.setContentType(fo.getContentType());
            op.setContentLength(fo.getContentLength());
            op.transferResponseHeadersFrom(fo);

            op.complete();
        };

        Operation forwardOp = op.clone().setCompletion(fc);
        if (rsp.isLocalHostOwner) {
            if (s == null) {
                queueOrFailRequestForServiceNotFoundOnOwner(
                        parentService, servicePath, op, rsp.availableNodeCount);
                return;
            }
            queueOrScheduleRequest(s, forwardOp);
            return;
        }

        if (op.isForwarded()) {
            // this was forwarded from another node, but we do not think we own the service
            Operation.failOwnerMismatch(op, op.getUri().getPath(), null);
            return;
        }

        // Forwarded operations are retried until the parent operation, from the client,
        // expires. Since a peer might have become unresponsive, we want short time outs
        // and retries, to whatever peer we select, on each retry.
        forwardOp.setExpiration(Utils.fromNowMicrosUtc(
                this.state.operationTimeoutMicros / 10));
        forwardOp.setUri(SelectOwnerResponse.buildUriToOwner(rsp, op));

        prepareForwardRequest(forwardOp);
        // Local host is not the owner, but is the entry host for a client. Forward to owner
        // node
        sendRequest(forwardOp);
    }

    private void queueOrFailRequestForServiceNotFoundOnOwner(
            Service parent, String path, Operation op, int availableNodeCount) {
        if (this.serviceResourceTracker.checkAndResumeService(op)) {
            return;
        }

        boolean synchService = parent != null &&
                parent.hasOption(ServiceOption.FACTORY) &&
                parent.hasOption(ServiceOption.REPLICATION) &&
                op.getAction() != Action.POST &&
                availableNodeCount > 1 &&
                !op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY);

        if (!synchService) {
            if (op.getAction() == Action.DELETE) {
                // do not queue DELETE actions for services not present, complete with success
                op.complete();
                return;
            }
            checkPragmaAndRegisterForAvailability(path, op);
            return;
        }

        this.serviceSynchTracker.failWithNotFoundOrSynchronize(parent, path, op);
    }

    void checkPragmaAndRegisterForAvailability(String path, Operation op) {
        if (!op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY)) {
            Operation.failServiceNotFound(op);
            return;
        }

        log(Level.INFO, "(%d) Registering for %s to become available on owner %s", op.getId(),
                path, getId());
        // service not available, register, then retry
        op.nestCompletion((avop) -> {
            handleRequest(null, op);
        });
        registerForServiceAvailability(op, path);
    }

    void retryPauseOrOnDemandLoadConflict(Operation op,
            boolean isOdlConflict) {
        this.serviceResourceTracker.retryPauseOrOnDemandLoadConflict(op, isOdlConflict);
    }

    private void retryOrFailRequest(Operation op, Operation fo, Throwable fe) {
        boolean shouldRetry = false;

        if (fo.hasBody()) {
            ServiceErrorResponse rsp = fo.clone().getBody(ServiceErrorResponse.class);
            if (rsp != null && rsp.details != null) {
                shouldRetry = rsp.details.contains(ErrorDetail.SHOULD_RETRY);
            }
        }

        if (fo.getStatusCode() == Operation.STATUS_CODE_TIMEOUT) {
            // the I/O code might have timed out, but we will keep retrying until the operation
            // expiration is reached
            shouldRetry = true;
        }

        if (op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORWARDED)) {
            // only retry on the node the client directly communicates with. Any node that receives
            // a forwarded operation will have forwarding disabled set, and should not retry
            shouldRetry = false;
        }

        if (op.getExpirationMicrosUtc() < Utils.getSystemNowMicrosUtc()) {
            op.setBodyNoCloning(fo.getBodyRaw())
                    .fail(new CancellationException("Expired at " + op.getExpirationMicrosUtc()));
            return;
        }

        if (!shouldRetry) {
            Operation.failForwardedRequest(op, fo, fe);
            return;
        }

        this.operationTracker.trackOperationForRetry(Utils.getNowMicrosUtc(), fe, op);
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

        String parentPath = UriUtils.getParentPath(path);
        if (parentPath != null && !waitForService) {
            Service parentService = this.findService(parentPath);
            // Only wait for factory if the logical parent of this service
            // is a factory which itself is starting
            if (parentService != null) {
                if (parentService.hasOption(ServiceOption.FACTORY)) {
                    waitForService = isServiceStarting(parentService, parentPath);
                    FactoryService parent = (FactoryService) parentService;
                    if (!inboundOp.isFromReplication()
                            && parent.hasChildOption(ServiceOption.OWNER_SELECTION)) {
                        // owner must do registration for availability, so proceed to queueOrForward
                        return false;
                    }
                }
                // the service might be paused (stopped due to memory pressure)
                if (parentService.hasOption(ServiceOption.PERSISTENCE)) {
                    if (this.serviceResourceTracker.checkAndResumeService(inboundOp)) {
                        return true;
                    }
                }
            }
        }

        if (inboundOp.isFromReplication()) {
            // If this is a replicated update request but the service is not
            // AVAILABLE, then we fail the request with 404 - NOT FOUND error.
            if (!isServiceAvailable(s) && inboundOp.isUpdate()) {
                this.log(Level.WARNING, "Service %s is not available. Failing replication request",
                        inboundOp.getUri().getPath());

                IllegalStateException ex = new IllegalStateException("Service not found on replica");
                Operation.fail(inboundOp, Operation.STATUS_CODE_NOT_FOUND,
                        ServiceErrorResponse.ERROR_CODE_SERVICE_NOT_FOUND_ON_REPLICA, ex);
                return true;
            }
        }

        if (inboundOp
                .hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY)) {
            waitForService = true;
        }

        if (waitForService || inboundOp.isFromReplication()) {
            if (inboundOp.getAction() == Action.DELETE) {
                // do not register for availability on DELETE action, allow downstream code to forward
                return false;
            }

            if (isStopping()) {
                // host is stopping, request will fail downstream
                return false;
            }

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

    private void queueOrScheduleRequest(Service s, Operation op) {
        ProcessingStage stage = s.getProcessingStage();
        if (stage == ProcessingStage.AVAILABLE) {
            queueOrScheduleRequestInternal(s, op);
            return;
        }

        if (op.getAction() == Action.DELETE) {
            queueOrScheduleRequestInternal(s, op);
            return;
        }

        if (stage == ProcessingStage.STOPPED) {
            if (op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_POST_TO_PUT)) {
                // service stopped after we decided it already existed and attempted
                // a IDEMPOTENT POST->PUT. Retry the original POST.
                restoreActionOnChildServiceToPostOnFactory(s.getSelfLink(), op);
                handleRequest(null, op);
                return;
            }
            if (s.hasOption(ServiceOption.ON_DEMAND_LOAD)) {
                retryPauseOrOnDemandLoadConflict(op, true);
                return;
            }
            op.setStatusCode(Operation.STATUS_CODE_NOT_FOUND);
        } else if (stage == ProcessingStage.PAUSED) {
            retryPauseOrOnDemandLoadConflict(op, false);
            return;
        }

        op.fail(new CancellationException("Service not available, in stage: " + stage));
    }

    private void queueOrScheduleRequestInternal(Service s, Operation op) {
        if (!s.queueRequest(op)) {
            Runnable r = () -> {
                OperationContext opCtx = extractAndApplyContext(op);
                try {
                    s.handleRequest(op);
                } catch (Throwable e) {
                    handleUncaughtException(s, op, e);
                } finally {
                    OperationContext.setFrom(opCtx);
                }
            };
            this.executor.execute(r);
        }
    }

    private boolean applyRequestRateLimit(Service s, Operation op) {
        if (this.state.requestRateLimits.isEmpty()) {
            return false;
        }

        if (op.isFromReplication() || op.isForwarded()) {
            // rate limiting is applied on the entry point host
            return false;
        }

        if (!op.isRemote()) {
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

        RequestRateInfo rateInfo = this.state.requestRateLimits.get(subject);
        if (rateInfo == null) {
            return false;
        }


        synchronized (rateInfo) {
            rateInfo.timeSeries.add(Utils.getSystemNowMicrosUtc(), 0, 1);
            TimeBin mostRecentBin = rateInfo.timeSeries.bins
                    .get(rateInfo.timeSeries.bins.lastKey());
            if (mostRecentBin.sum < rateInfo.limit) {
                return false;
            }
        }

        this.getManagementService().adjustStat(
                ServiceHostManagementService.STAT_NAME_RATE_LIMITED_OP_COUNT, 1);

        if (rateInfo.options.contains(Option.PAUSE_PROCESSING)) {
            // Add option as a hint to the request listener to throttle the channel associated with
            // the operation
            op.toggleOption(OperationOption.RATE_LIMITED, true);
        }

        if (!rateInfo.options.contains(Option.FAIL)) {
            return false;
        }

        Operation.failLimitExceeded(op, ServiceErrorResponse.ERROR_CODE_HOST_RATE_LIMIT_EXCEEDED);
        Operation nextOp = s.dequeueRequest();
        if (nextOp != null) {
            run(() -> handleRequest(null, nextOp));
        }
        return true;
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

    @Override
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
        if (getOperationTracingLevel() == Level.OFF) {
            return;
        }

        if (this.state.operationTracingLinkExclusionList.contains(op.getUri().getPath())) {
            return;
        }

        for (String excludedPath : this.state.operationTracingLinkExclusionList) {
            if (op.getUri().getPath().startsWith(excludedPath)) {
                return;
            }
        }

        if (getOperationTracingLevel().intValue() <= Level.FINE.intValue()) {
            // include stats for all levels with equal or lower level
            String name = op.getUri().getPath() + ":" + op.getAction();
            ServiceStat st = this.getManagementService().getStat(name);
            // add a statistic for the service and action
            synchronized (name.intern()) {
                if (st == null || st.timeSeriesStats == null) {
                    this.serviceResourceTracker.createTimeSeriesStat(name, 1.0);
                    st = getManagementService().getStat(name);
                }
            }
            getManagementService().adjustStat(st, 1.0);
        }

        if (getOperationTracingLevel() == Level.FINER) {
            // we log only on the specific level, intentionally, to reduce side-effects
            log(Level.INFO, op.toLogString());
        }

        if (getOperationTracingLevel().intValue() > Level.FINEST.intValue()) {
            return;
        }

        if (this.operationIndexServiceUri == null) {
            this.operationIndexServiceUri = UriUtils.buildUri(this, OperationIndexService.class);
        }

        Operation.SerializedOperation tracingOp = Operation.SerializedOperation.create(op);
        sendRequest(Operation.createPost(this.operationIndexServiceUri)
                .setReferer(getUri())
                .setBodyNoCloning(tracingOp));
    }

    void prepareForwardRequest(Operation fwdOp) {
        fwdOp.toggleOption(OperationOption.FORWARDED, true);
        fwdOp.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORWARDED);
        fwdOp.setConnectionTag(ServiceClient.CONNECTION_TAG_FORWARDING);
        fwdOp.toggleOption(NodeSelectorService.FORWARDING_OPERATION_OPTION,
                true);
    }

    private void prepareRequest(Operation op) {
        if (op.getUri() == null) {
            throw new IllegalArgumentException("URI is required");
        }

        if (op.getUri().getPort() != this.state.httpPort
                && op.getUri().getPort() != this.state.httpsPort) {
            // force communication between hosts in the same process to go
            // through sockets. It is less optimal but in production we do not
            // expect multiple hosts per process. In tests, we do expect
            // multiple hosts but they goal is to simulate cross machine or
            // cross process communication
            op.forceRemote();
        }
        if (op.getExpirationMicrosUtc() == 0) {
            long expirationMicros = Utils
                    .fromNowMicrosUtc(this.state.operationTimeoutMicros);
            op.setExpiration(expirationMicros);
        }

        if (op.getCompletion() == null) {
            op.setCompletion((o, e) -> {
                if (e == null) {
                    return;
                }
                if (op.isFailureLoggingDisabled()) {
                    return;
                }
                log(Level.WARNING, "%s (ctx id:%s) to %s, from %s failed: %s", o.getAction(),
                        o.getContextId(),
                        o.getUri(),
                        o.getReferer(),
                        e.getMessage());
            });
        }
    }

    /**
     * Synchronously stops the host and all services attached. Each service is stopped in parallel
     * and a brief expiration window is set allowing it to complete any shutdown tasks
     */
    public void stop() {
        Set<Service> servicesToClose;

        synchronized (this.state) {
            if (!this.state.isStarted || this.state.isStopping) {
                return;
            }
            this.state.isStopping = true;
            servicesToClose = new HashSet<>(this.attachedServices.values());
        }

        this.serviceResourceTracker.close();
        this.serviceMaintTracker.close();
        this.operationTracker.close();
        this.serviceSynchTracker.close();

        ScheduledFuture<?> task = this.maintenanceTask;
        if (task != null) {
            task.cancel(false);
            this.maintenanceTask = null;
        }

        List<Service> privilegedServiceInstances = stopServices(servicesToClose);

        stopPrivilegedServices(privilegedServiceInstances);

        stopCoreServices();

        this.attachedServices.clear();
        this.attachedNamespaceServices.clear();
        this.pendingServiceDeletions.clear();
        this.state.isStarted = false;

        this.authorizationContextCache.clear();
        this.authorizationServiceUri = null;

        removeLogging();

        try {
            this.client.stop();
            this.client = null;
        } catch (Throwable e1) {
        }

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

        this.executor.shutdownNow();
        this.scheduledExecutor.shutdownNow();
        this.serviceScheduledExecutor.shutdownNow();
        this.executor = null;
        this.scheduledExecutor = null;
    }

    private List<Service> stopServices(Set<Service> servicesToClose) {
        int servicesToCloseCount = servicesToClose.size()
                - this.coreServices.size();

        final CountDownLatch latch = new CountDownLatch(servicesToCloseCount);

        final Operation.CompletionHandler removeServiceCompletion = (o, e) -> {
            this.attachedServices.remove(o.getUri().getPath());
            latch.countDown();
        };

        setAuthorizationContext(getSystemAuthorizationContext());

        List<Service> privilegedServiceInstances = new ArrayList<>();

        // first shut down non core services: During their stop processing they
        // might still rely on core services
        for (final Service s : servicesToClose) {
            if (this.coreServices.contains(s.getSelfLink())) {
                // stop core services last
                continue;
            }
            if (this.privilegedServiceTypes.containsKey(s.getClass().getName())) {
                privilegedServiceInstances.add(s);
                // Invoke completion handler so we count down. This avoids a two pass
                // over all services to determine what services are privileged. Its OK that
                // we remove the service from the attached list, here, and in
                // stopPrivilegedServices()
                removeServiceCompletion.handle(Operation.createDelete(s.getUri()), null);
                // stop privileged services last
                continue;
            }
            sendServiceStop(removeServiceCompletion, s);
        }

        log(Level.INFO, "Waiting for DELETE from %d services", servicesToCloseCount);
        waitForServiceStop(latch);
        log(Level.INFO, "All non core services stopped", servicesToCloseCount);
        return privilegedServiceInstances;
    }

    private void stopPrivilegedServices(List<Service> privilegedServiceInstances) {
        if (privilegedServiceInstances.size() == 0) {
            return;
        }
        int servicesToCloseCount;
        servicesToCloseCount = privilegedServiceInstances.size();
        final CountDownLatch pLatch = new CountDownLatch(servicesToCloseCount);
        final Operation.CompletionHandler pc = (o, e) -> {
            pLatch.countDown();
        };

        // now do privileged service shutdown in parallel
        for (Service p : privilegedServiceInstances) {
            sendServiceStop(pc, p);
        }

        log(Level.INFO, "Waiting for DELETE from %d privileged services", servicesToCloseCount);
        waitForServiceStop(pLatch);
        log(Level.INFO, "All privileged services stopped");
    }

    private void stopCoreServices() {
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

        // stopping management service
        Service managementService = getManagementService();
        if (managementService != null && managementService.getSelfLink() != null) {
            stopService(managementService);
        }

        log(Level.INFO, "All core services stopped");
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
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_FORWARDING)
                .setCompletion(removeServiceCompletion)
                .setReferer(getUri());
        try {
            queueOrScheduleRequest(s, delete);
        } catch (Throwable e) {
            log(Level.WARNING, Utils.toString(e));
            removeServiceCompletion.handle(delete, e);
        }
    }

    public static boolean isServiceCreate(Operation op) {
        return op.getAction() == Action.POST
                && op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_CREATED);
    }

    public static boolean isServiceStop(Operation op) {
        return op.getAction() == Action.DELETE
                && op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE);
    }

    public static boolean isServiceDeleteAndStop(Operation op) {
        return op.getAction() == Action.DELETE
                && !op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE);
    }

    public static boolean isServiceAvailable(Service s) {
        return s != null && s.getProcessingStage() == ProcessingStage.AVAILABLE;
    }

    /**
     * Returns value indicating whether the request targets the service itself,
     * or, if ServiceOption.URI_NAMESPACE_OWNER is set, and does not match the self link,
     * targets portion the name space
     */
    public static boolean isForServiceNamespace(Service s, Operation op) {
        return s.hasOption(ServiceOption.URI_NAMESPACE_OWNER)
                && !op.getUri().getPath().equals(s.getSelfLink());
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
            //catches /service/ui
            return true;
        } else if (!serviceUriPath.startsWith(ServiceUriPaths.UI_RESOURCES) &&
                !serviceUriPath.startsWith(ServiceUriPaths.CORE + SERVICE_URI_SUFFIX_UI) &&
                serviceUriPath.indexOf(SERVICE_URI_SUFFIX_UI + UriUtils.URI_PATH_CHAR) > 0) {
            // catches /service/ui/ and /service/ui/whatever
            // exclude well-known services that happen to contain /ui/
            return true;
        } else if (serviceUriPath.endsWith(SERVICE_URI_SUFFIX_AVAILABLE)) {
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
        log(level, 3, () -> String.format(fmt, args));
    }

    public void log(Level level, Supplier<String> messageSupplier) {
        log(level, 3, messageSupplier);
    }

    protected void log(Level level, Integer nestingLevel, String fmt, Object... args) {
        if (this.logPrefix == null) {
            this.logPrefix = getPublicUri().toString();
        }
        Utils.log(this.logger, nestingLevel, this.logPrefix, level, () -> String.format(fmt, args));
    }

    protected void log(Level level, Integer nestingLevel, Supplier<String> messageSupplier) {
        if (this.logPrefix == null) {
            this.logPrefix = getPublicUri().toString();
        }
        Utils.log(this.logger, nestingLevel, this.logPrefix, level, messageSupplier);
    }

    /**
     * Registers a completion that is invoked every time one of the supplied services reaches the
     * available stage. If service start fails for any one, the completion will be called with a
     * failure argument.
     *
     * When {@code checkReplica} flag is on(see other overloading methods), this method checks not
     * only the local node, but also checks the service availability in node group for factory links
     * that produce replicated services.
     *
     * Note that supplying multiple self links will result in multiple completion invocations. The
     * handler provided must track how many times it has been called
     *
     * @see #checkReplicatedServiceAvailable(CompletionHandler, String)
     * @see NodeGroupUtils#registerForReplicatedServiceAvailability(ServiceHost, Operation, String, String)
     * @see NodeGroupUtils#checkServiceAvailability(CompletionHandler, Service)
     */
    public void registerForServiceAvailability(CompletionHandler completion,
            String... servicePaths) {
        registerForServiceAvailability(completion, ServiceUriPaths.DEFAULT_NODE_SELECTOR, false,
                servicePaths);
    }

    public void registerForServiceAvailability(CompletionHandler completion, boolean checkReplica,
            String... servicePaths) {
        registerForServiceAvailability(completion, ServiceUriPaths.DEFAULT_NODE_SELECTOR,
                checkReplica, servicePaths);
    }

    public void registerForServiceAvailability(CompletionHandler completion,
            String nodeSelectorPath, boolean checkReplica, String... servicePaths) {
        if (servicePaths == null || servicePaths.length == 0) {
            throw new IllegalArgumentException("selfLinks are required");
        }
        Operation op = Operation.createPost(null)
                .setCompletion(completion)
                .setExpiration(Utils.fromNowMicrosUtc(getOperationTimeoutMicros()));

        registerForServiceAvailability(op, checkReplica, nodeSelectorPath, servicePaths);
    }

    void registerForServiceAvailability(Operation opTemplate, String... servicePaths) {
        registerForServiceAvailability(opTemplate, false, ServiceUriPaths.DEFAULT_NODE_SELECTOR,
                servicePaths);
    }

    private void registerForServiceAvailability(Operation opTemplate, boolean checkReplica,
            String nodeSelectorPath, String... servicePaths) {
        final boolean doOpClone = servicePaths.length > 1;
        // clone client supplied array since this method mutates it
        final String[] clonedLinks = Arrays.copyOf(servicePaths, servicePaths.length);

        List<String> replicatedServiceLinks = new ArrayList<>();

        synchronized (this.state) {
            for (int i = 0; i < clonedLinks.length; i++) {
                String link = clonedLinks[i];
                Service s = findService(link);

                // service is null if this method is called before even the service is registered
                if (s != null) {
                    if (checkReplica &&
                            s.hasOption(ServiceOption.FACTORY) &&
                            s.hasOption(ServiceOption.REPLICATION)) {
                        // null the link so we do not attempt to invoke the completion below
                        clonedLinks[i] = null;
                        replicatedServiceLinks.add(link);
                        continue;
                    }

                    if (s.getProcessingStage() == Service.ProcessingStage.AVAILABLE) {
                        continue;
                    }

                    // track operation
                    this.operationTracker.trackServiceAvailableCompletion(link, opTemplate,
                            doOpClone);
                } else {
                    final Operation opTemplateClone = getOperationForServiceAvailability(opTemplate,
                            link,
                            doOpClone);
                    if (checkReplica) {
                        // when local service is not yet started and required to check replicated
                        // service, delay the node-group-service-availability-check until local
                        // service becomes available by nesting the logic to the opTemplate.
                        opTemplateClone.nestCompletion(op -> {
                            Service service = findService(op.getUri().getPath());
                            if (service != null
                                    && service.hasOption(ServiceOption.FACTORY)
                                    && service.hasOption(ServiceOption.REPLICATION)) {
                                run(() -> {
                                    NodeGroupUtils
                                            .registerForReplicatedServiceAvailability(this,
                                                    opTemplateClone,
                                                    link, nodeSelectorPath);
                                });
                            } else {
                                opTemplateClone.complete();
                            }
                        });

                    }

                    // Track operation but do not clone again.
                    // Add the operation with the specific nested completion
                    this.operationTracker.trackServiceAvailableCompletion(link, opTemplateClone,
                            false);
                }

                // null the link so we do not attempt to invoke the completion below
                clonedLinks[i] = null;
            }
        }

        for (String link : clonedLinks) {
            if (link == null) {
                continue;
            }

            log(Level.INFO, "%s in stage %s, completing %d (%s)", link, getServiceStage(link),
                    opTemplate.getId(), opTemplate.getContextId());
            final Operation opFinal = opTemplate;
            run(() -> {
                Operation o = getOperationForServiceAvailability(opFinal, link, doOpClone);
                o.complete();
            });
        }

        for (String link : replicatedServiceLinks) {
            Operation o = getOperationForServiceAvailability(opTemplate, link, doOpClone);
            run(() -> {
                NodeGroupUtils
                        .registerForReplicatedServiceAvailability(this, o, link, nodeSelectorPath);
            });
        }
    }

    private Operation getOperationForServiceAvailability(Operation op, String link,
            boolean doClone) {
        Operation o = op;
        if (doClone) {
            o = op.clone().setUri(UriUtils.buildUri(this, link));
        } else if (o.getUri() == null) {
            o.setUri(UriUtils.buildUri(this, link));
        }
        return o;
    }

    boolean hasPendingServiceAvailableCompletions(String selfLink) {
        return this.operationTracker.hasPendingServiceAvailableCompletions(selfLink);
    }

    /**
     * Sets an upper limit, in terms of operations per second, for all operations
     * associated with some context. The context is (tenant, user, referrer) is used
     * to derive the key.
     * To specify advanced options use {@link #setRequestRateLimit(String, RequestRateInfo)}
     */
    public ServiceHost setRequestRateLimit(String key, double operationsPerSecond) {
        RequestRateInfo ri = new RequestRateInfo();
        ri.limit = operationsPerSecond;
        return setRequestRateLimit(key, ri);
    }

    /**
     * See {@link #setRequestRateLimit(String, double)}
     */
    public ServiceHost setRequestRateLimit(String key, RequestRateInfo ri) {
        if (ri.limit <= 0.0) {
            throw new IllegalArgumentException("limit must be a non zero positive number");
        }
        ri = Utils.clone(ri);
        if (ri.timeSeries == null) {
            ri.timeSeries = new TimeSeriesStats(
                    60,
                    TimeUnit.SECONDS.toMillis(1),
                    EnumSet.of(AggregationType.SUM));
        } else if (!ri.timeSeries.aggregationType.contains(AggregationType.SUM)) {
            throw new IllegalArgumentException(
                    "time series must be of type " + AggregationType.SUM);
        }

        if (ri.options == null || ri.options.isEmpty()) {
            ri.options = EnumSet.of(Option.FAIL);
        }

        // overwrite any existing limit
        this.state.requestRateLimits.put(key, ri);
        return this;
    }

    /**
     * Retrieves rate limit configuration for the supplied key
     */
    public RequestRateInfo getRequestRateLimit(String key) {
        RequestRateInfo ri = this.state.requestRateLimits.get(key);
        if (ri == null) {
            return null;
        }
        return Utils.clone(ri);
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
        Service s = findService(servicePath);
        if (s == null) {
            return null;
        }
        return s.getProcessingStage();
    }

    /**
     * Checks if the service associated with the supplied path is started
     * and in processing stage available
     */
    public boolean checkServiceAvailable(String servicePath) {
        Service s = this.findService(servicePath, true);
        return s != null && s.getProcessingStage() == ProcessingStage.AVAILABLE;
    }

    /**
     * @see NodeGroupUtils#checkServiceAvailability(CompletionHandler, ServiceHost, String, String)
     */
    public void checkReplicatedServiceAvailable(CompletionHandler ch, String servicePath) {
        checkReplicatedServiceAvailable(ch, servicePath, ServiceUriPaths.DEFAULT_NODE_SELECTOR);
    }

    public void checkReplicatedServiceAvailable(CompletionHandler ch, String servicePath,
            String nodeSelectorPath) {
        Service s = this.findService(servicePath, true);
        if (s == null) {
            ch.handle(null, new IllegalStateException("service not found"));
            return;
        }
        NodeGroupUtils.checkServiceAvailability(ch, s.getHost(), s.getSelfLink(), nodeSelectorPath);
    }

    public SystemHostInfo getSystemInfo() {
        if (!this.info.properties.isEmpty() && !this.info.ipAddresses.isEmpty()) {
            return Utils.clone(this.info);
        }
        return updateSystemInfo(true);
    }

    public SystemHostInfo updateSystemInfo(boolean enumerateNetworkInterfaces) {

        this.info.availableProcessorCount = Runtime.getRuntime().availableProcessors();
        this.info.osName = Utils.getOsName(this.info);
        this.info.osFamily = Utils.determineOsFamily(this.info.osName);

        updateMemoryAndDiskInfo();

        for (Entry<Object, Object> e : System.getProperties().entrySet()) {
            String k = e.getKey().toString();
            String v = e.getValue().toString();
            this.info.properties.put(k, v);
        }

        this.info.environmentVariables.putAll(System.getenv());

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

    public void updateMemoryAndDiskInfo() {
        Runtime r = Runtime.getRuntime();

        this.info.freeMemoryByteCount = r.freeMemory();
        this.info.totalMemoryByteCount = r.totalMemory();
        this.info.maxMemoryByteCount = r.maxMemory();

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
            clearUriAndLogPrefix();
            return true;
        }

        return address.equals(ipAddresses.get(0));
    }

    private void clearUriAndLogPrefix() {
        this.cachedUri = null;
        this.cachedPublicUriString = null;
        this.logPrefix = null;
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
        OperationContext origContext = OperationContext.getOperationContext();
        this.executor.execute(() -> {
            OperationContext.setFrom(origContext);
            executeRunnableSafe(task);
        });
    }

    /**
     * Executes the task using provided executor
     */
    public void run(ExecutorService executor, Runnable task) {
        if (executor == null || task == null) {
            throw new IllegalStateException("Valid executor/task must be provided");
        }
        if (executor.isShutdown()) {
            throw new IllegalStateException("Stopped");
        }
        OperationContext origContext = OperationContext.getOperationContext();
        executor.execute(() -> {
            OperationContext.setFrom(origContext);
            executeRunnableSafe(task);
        });
    }

    /**
     * Schedules a task using the shared service executor
     */
    public ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
        return schedule(this.serviceScheduledExecutor, task, delay, unit);
    }

    /**
     * Infrastructure use only. Do not use for non core scheduled tasks. The method
     * signature will likely change in the future so the caller is validated against the
     * set of core services.
     */
    public ScheduledFuture<?> scheduleCore(Runnable task, long delay, TimeUnit unit) {
        return schedule(this.scheduledExecutor, task, delay, unit);
    }

    private ScheduledFuture<?> schedule(ScheduledExecutorService e, Runnable task, long delay,
            TimeUnit unit) {
        if (this.isStopping()) {
            throw new IllegalStateException("Stopped");
        }
        if (e.isShutdown()) {
            throw new IllegalStateException("Stopped");
        }

        OperationContext origContext = OperationContext.getOperationContext();
        return e.schedule(() -> {
            OperationContext.setFrom(origContext);
            executeRunnableSafe(task);
        }, delay, unit);
    }

    private void executeRunnableSafe(Runnable task) {
        try {
            task.run();
        } catch (Throwable e) {
            log(Level.SEVERE, "Unhandled exception executing task: %s", Utils.toString(e));
        }
    }

    enum MaintenanceStage {
        UTILS, MEMORY, IO, NODE_SELECTORS, SERVICE
    }

    /**
     * Initiates host periodic maintenance cycle
     */
    private void scheduleMaintenance() {
        Runnable r = () -> {
            OperationContext.setAuthorizationContext(this.getSystemAuthorizationContext());
            this.state.lastMaintenanceTimeUtcMicros = Utils.getSystemNowMicrosUtc();
            long deadline = this.state.lastMaintenanceTimeUtcMicros
                    + this.state.maintenanceIntervalMicros;
            performMaintenanceStage(Operation.createPost(getUri()),
                    MaintenanceStage.UTILS, deadline);
        };

        this.maintenanceTask = schedule(r, getMaintenanceIntervalMicros(), TimeUnit.MICROSECONDS);
    }

    /**
     * Initiates periodic maintenance for a service. Called on service start or when maintenance is
     * dynamically toggled on
     */
    void scheduleServiceMaintenance(Service s) {
        if (!s.hasOption(ServiceOption.PERIODIC_MAINTENANCE)) {
            return;
        }
        this.serviceMaintTracker.schedule(s, Utils.getSystemNowMicrosUtc());
    }

    /**
     * Performs maintenance tasks for the given stage. Only a single instance of this
     * state machine must be active per host, at any time. Maintenance is re-scheduled
     * when the final stage is complete.
     */
    void performMaintenanceStage(Operation post, MaintenanceStage stage, long deadline) {
        try {
            long now = Utils.getSystemNowMicrosUtc();

            switch (stage) {
            case UTILS:
                Utils.performMaintenance();
                stage = MaintenanceStage.MEMORY;
                break;
            case MEMORY:
                this.serviceResourceTracker.performMaintenance(now, deadline);
                stage = MaintenanceStage.IO;
                break;
            case IO:
                performIOMaintenance(post, now, MaintenanceStage.NODE_SELECTORS, deadline);
                return;
            case NODE_SELECTORS:
                performNodeSelectorChangeMaintenance(post, now, MaintenanceStage.SERVICE, true,
                        deadline);
                return;
            case SERVICE:
                this.serviceMaintTracker.performMaintenance(post, deadline);
                stage = null;
                break;
            default:
                stage = null;
                break;
            }

            if (stage == null) {
                if (this.managementService != null) {
                    // update the maintenance count stat for the ServiceHost before, completing
                    // the current maintenance run.
                    this.managementService.adjustStat(
                            Service.STAT_NAME_SERVICE_HOST_MAINTENANCE_COUNT, 1);

                    // Update the count of services that are pending delete on the service host.
                    this.managementService.setStat(
                            ServiceHostManagementService.STAT_NAME_PENDING_SERVICE_DELETION_COUNT,
                            this.pendingServiceDeletions.size());
                }

                post.complete();
                scheduleMaintenance();
                return;
            }
            performMaintenanceStage(post, stage, deadline);
        } catch (Throwable e) {
            log(Level.SEVERE, "Uncaught exception: %s", e.toString());
            post.fail(e);
        }
    }

    private void performNodeSelectorChangeMaintenance(Operation post, long now,
            MaintenanceStage nextStage, boolean isCheckRequired, long deadline) {
        this.serviceSynchTracker.performNodeSelectorChangeMaintenance(post, now, nextStage,
                isCheckRequired, deadline);
    }

    private void performIOMaintenance(Operation post, long now, MaintenanceStage nextStage,
            long deadline) {
        try {
            this.operationTracker.performMaintenance(now);
            performMaintenanceStage(post, nextStage, deadline);
        } catch (Throwable e) {
            log(Level.WARNING, "Exception: %s", Utils.toString(e));
            performMaintenanceStage(post, nextStage, deadline);
        }
    }

    /**
     * Infrastructure use only. Invoked from the service context index service
     */
    public void resumeService(String path, Service resumedService) {
        this.serviceResourceTracker.resumeService(path, resumedService);
    }

    public ServiceHost setOperationTimeOutMicros(long timeoutMicros) {
        this.state.operationTimeoutMicros = timeoutMicros;
        return this;
    }

    public ServiceHost setServiceCacheClearDelayMicros(long delayMicros) {
        this.state.serviceCacheClearDelayMicros = delayMicros;
        return this;
    }

    public long getServiceCacheClearDelayMicros() {
        return this.state.serviceCacheClearDelayMicros;
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
        // If this request doesn't originate from replication (which might happen asynchronously, i.e. through
        // (re-)synchronization after a node group change), don't update the documentAuthPrincipalLink because
        // it will be set to the system user. The specified state is expected to have the documentAuthPrincipalLink
        // set from when it was first saved.
        if (!op.isFromReplication()) {
            state.documentAuthPrincipalLink = (op.getAuthorizationContext() != null)
                    ? op.getAuthorizationContext().getClaims().getSubject() : null;
        }

        if (this.transactionService != null) {
            state.documentTransactionId = op.getTransactionId();
        }
        state.documentUpdateAction = op.getAction().name();

        if (!isServiceIndexed(s)) {
            cacheServiceState(s, state, op);
            op.complete();
            return;
        }

        Service indexService = getIndexServiceForService(s);

        // serialize state and compute signature. The index service will take
        // the serialized state and store as is, and it will index all fields
        // from the document instance, using the description for instructions
        UpdateIndexRequest body = new UpdateIndexRequest();
        body.document = state;
        // retrieve the description through the cached template so its the thread safe,
        // immutable version
        body.description = buildDocumentDescription(s);
        body.serializedDocument = op.getLinkedSerializedState();
        op.linkSerializedState(null);
        if (!op.isFromReplication()) {
            // Do not cache state, in replicas
            cacheServiceState(s, state, op);
        }

        Operation post = Operation.createPost(indexService.getUri())
                .setBodyNoCloning(body)
                .setCompletion((o, e) -> {
                    if (op.getAction() == Action.DELETE) {
                        unmarkAsPendingDelete(s);
                    }
                    if (e != null) {
                        this.serviceResourceTracker.clearCachedServiceState(s, null, op);
                        op.fail(e);
                        return;
                    }
                    op.complete();
                });

        if (op.getAction() == Action.POST
                && op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)) {
            post.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE);
        }

        // Just like we do in loadServiceState, special case co-located indexing service and bypass
        // normal processing path, to reduce latency. The call is still assumed to be asynchronous
        // and the request can be processed in arbitrary thread context.
        indexService.handleRequest(post);
    }

    /**
     * Infrastructure use only
     * @see ServiceSynchronizationTracker#selectServiceOwnerAndSynchState(Service, Operation)
     */
    void selectServiceOwnerAndSynchState(Service s, Operation op) {
        this.serviceSynchTracker.selectServiceOwnerAndSynchState(s, op);
    }

    NodeSelectorService findNodeSelectorService(String path,
            Operation request) {
        if (path == null) {
            path = ServiceUriPaths.DEFAULT_NODE_SELECTOR;
        }

        Service s = this.findService(path);
        if (s == null) {
            request.fail(new ServiceNotFoundException());
            return null;
        }
        return (NodeSelectorService) s;
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
        req.options = SelectAndForwardRequest.BROADCAST_OPTIONS;
        if (excludeThisHost) {
            req.options = SelectAndForwardRequest.BROADCAST_OPTIONS_EXCLUDE_ENTRY_NODE;
            req.options.add(ForwardingOption.EXCLUDE_ENTRY_NODE);
        }
        req.key = key;
        req.targetPath = request.getUri().getPath();
        req.targetQuery = request.getUri().getQuery();
        nss.selectAndForward(request, req);
    }

    private ThreadLocal<SelectAndForwardRequest> selectOwnerRequests = new ThreadLocal<SelectAndForwardRequest>() {

        @Override
        public SelectAndForwardRequest initialValue() {
            return new SelectAndForwardRequest();
        }

    };

    /**
     * Convenience method that issues a {@code SelectOwnerRequest} to the node selector service. If
     * the supplied path is null the default selector will be used
     */
    public void selectOwner(String selectorPath, String key, Operation op) {
        if (isStopping()) {
            op.fail(new CancellationException());
            return;
        }

        SelectAndForwardRequest body = this.selectOwnerRequests.get();
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
        body.options = SelectAndForwardRequest.UNICAST_OPTIONS;
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
        req.options = SelectAndForwardRequest.REPLICATION_OPTIONS;
        req.serviceOptions = serviceOptions;
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

    public void queryServiceUris(EnumSet<ServiceOption> options, boolean matchAllOptions,
            Operation get) {
        queryServiceUris(options, matchAllOptions, get, null);
    }

    /**
     * Queries services in the AVAILABLE stage based on the provided options, excluding all
     * UTILITY services.
     *
     * @param options options that must match
     * @param matchAllOptions true : all options must match,  false : any option must match
     * @param get
     * @param exclusionOptions if not-null, exclude services that have any of the excluded options
     */
    public void queryServiceUris(EnumSet<ServiceOption> options, boolean matchAllOptions,
            Operation get, EnumSet<ServiceOption> exclusionOptions) {
        ServiceDocumentQueryResult r = new ServiceDocumentQueryResult();

        loop: for (Service s : this.attachedServices.values()) {
            if (s.getProcessingStage() != ProcessingStage.AVAILABLE) {
                continue;
            }
            if (s.hasOption(ServiceOption.UTILITY)) {
                continue;
            }

            if (exclusionOptions != null) {
                for (ServiceOption exOp : exclusionOptions) {
                    if (s.hasOption(exOp)) {
                        continue loop;
                    }
                }
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
     * Infrastructure use only
     *
     * Create service document description. The servicePath is used to
     * lookup the service implementation class and its state class type. If the service is not
     * currently attached to the host, an attempt is made to lookup the class types using the
     * parent path, and only if the parent is a factory. Otherwise, the call will return null
     */
    public ServiceDocumentDescription buildDocumentDescription(String servicePath) {
        Service s = findService(servicePath);
        if (s == null) {
            // on demand load or paused services will not be attached, but will still have
            // valid descriptions cached. Look up their description using their parent (factory)
            // link
            String parentPath = UriUtils.getParentPath(servicePath);
            return this.descriptionCachePerFactoryLink.get(parentPath);
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
        String serviceTypeName = s.getClass().getName();
        synchronized (this.descriptionCache) {
            ServiceDocumentDescription desc = this.descriptionCache.get(serviceTypeName);
            if (desc != null) {
                return desc;
            }

            // Description has to be built in three stages:
            // 1) Build the base description and add it to the cache
            desc = this.descriptionBuilder.buildDescription(serviceStateClass, s.getOptions(),
                    RequestRouter.findRequestRouter(s.getOperationProcessingChain()));

            if (s.getOptions().contains(ServiceOption.IMMUTABLE)) {
                if (desc.versionRetentionLimit > ServiceDocumentDescription.DEFAULT_VERSION_RETENTION_LIMIT) {
                    log(Level.WARNING, "Service %s has option %s, forcing retention limit",
                            s.getSelfLink(), ServiceOption.IMMUTABLE);
                }
                // set retention limit to MIN value so index service skips version retention on this
                // document type
                desc.versionRetentionLimit = ServiceDocumentDescription.FIELD_VALUE_DISABLED_VERSION_RETENTION;
            }
            this.descriptionCache.put(serviceTypeName, desc);

            // 2) Call the service's getDocumentTemplate() to allow the service author to modify it
            // We are calling a function inside a lock, which is bad practice. This is however
            // by contract a synchronous function that should be O(1). We also only call it once.
            ServiceDocumentDescription augmentedDesc = s.getDocumentTemplate().documentDescription;
            if (augmentedDesc != null) {
                desc = augmentedDesc;

                // 3) Update the cached entry
                this.descriptionCache.put(serviceTypeName, desc);
            }

            // Cache entry also under the parent (factory) path so we can lookup descriptions even if the
            // service (child) is not loaded. This is common for on demand load services and authorization
            // checks on their documents
            if (s.hasOption(ServiceOption.FACTORY_ITEM) && s.getSelfLink() != null) {
                String parentPath = UriUtils.getParentPath(s.getSelfLink());
                Service factoryService = findService(parentPath);
                if (factoryService != null && factoryService.hasOption(ServiceOption.FACTORY)) {
                    this.descriptionCachePerFactoryLink.put(parentPath, desc);
                }
            }
            return desc;
        }
    }

    public URI getPublicUri() {
        if (this.state.publicUri == null) {
            return getUri();
        }
        return this.state.publicUri;
    }

    public String getPublicUriAsString() {
        if (this.cachedPublicUriString == null) {
            this.cachedPublicUriString = getPublicUri().toString();
            if (this.cachedPublicUriString.endsWith(UriUtils.URI_PATH_CHAR)) {
                this.cachedPublicUriString = this.cachedPublicUriString.substring(0,
                        this.cachedPublicUriString.length() - 1);
            }
        }
        return this.cachedPublicUriString;
    }

    public URI getUri() {
        if (this.cachedUri == null) {
            boolean isSecureConnectionOnly = getCurrentHttpScheme() == HttpScheme.HTTPS_ONLY;
            String scheme = isSecureConnectionOnly ? UriUtils.HTTPS_SCHEME : UriUtils.HTTP_SCHEME;
            int port = isSecureConnectionOnly ? getSecurePort() : getPort();
            this.cachedUri = UriUtils.buildUri(scheme, getPreferredAddress(), port, "", null);
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

    public void setRequestPayloadSizeLimit(int limit) {
        synchronized (this.state) {
            if (isStarted()) {
                throw new IllegalStateException("Already started");
            }
            this.state.requestPayloadSizeLimit = limit;
        }
    }

    public void setResponsePayloadSizeLimit(int limit) {
        synchronized (this.state) {
            if (isStarted()) {
                throw new IllegalStateException("Already started");
            }
            this.state.responsePayloadSizeLimit = limit;
        }
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
     * Infrastructure use only. Only services added as privileged can use this method.
     */
    public void cacheAuthorizationContext(Service s, AuthorizationContext ctx) {
        cacheAuthorizationContext(s, ctx.getToken(), ctx);
    }

    /**
     * Infrastructure use only. Only services added as privileged can use this method.
     */
    public void cacheAuthorizationContext(Service s, String token, AuthorizationContext ctx) {
        if (!this.isPrivilegedService(s)) {
            throw new RuntimeException("Service not allowed to cache authorization token");
        }
        synchronized (this.state) {
            this.authorizationContextCache.put(token, ctx);
            addUserToken(this.userLinkToTokenMap, ctx.getClaims().getSubject(), token);
        }
    }

    /**
     * Infrastructure use only. Only services added as privileged can use this method.
     */
    public void clearAuthorizationContext(Service s, String userLink) {
        if (!this.isPrivilegedService(s)) {
            throw new RuntimeException("Service not allowed to clear authorization token");
        }
        synchronized (this.state) {
            Set<String> tokenSet = this.userLinkToTokenMap.remove(userLink);
            if (tokenSet != null) {
                for (String token : tokenSet) {
                    this.authorizationContextCache.remove(token);
                }
            }
        }
    }

    /**
     * Infrastructure use only. Only services added as privileged can use this method.
     */
    public AuthorizationContext getAuthorizationContext(Service s, String token) {
        if (!this.isPrivilegedService(s)) {
            throw new RuntimeException("Service not allowed to retrieve authorization token");
        }
        return this.authorizationContextCache.get(token);
    }

    private void populateAuthorizationContext(Operation op, Consumer<AuthorizationContext> authorizationContextHandler) {
        getAuthorizationContext(op, authorizationContext -> {
            if (authorizationContext == null) {
                // No (valid) authorization context, fall back to guest context
                authorizationContext = getGuestAuthorizationContext();
            }
            op.setAuthorizationContext(authorizationContext);
            authorizationContextHandler.accept(authorizationContext);
        });
    }

    /**
     * Generate new authorization context for a system user.
     *
     * @return fresh authorization context
     */
    private AuthorizationContext createAuthorizationContext(String userLink) {
        Claims.Builder cb = new Claims.Builder();
        cb.setIssuer(AuthenticationConstants.DEFAULT_ISSUER);
        cb.setSubject(userLink);

        cb.setExpirationTime(Instant.MAX.getEpochSecond());

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
     * Returns an authorization context for a given user.
     *
     * @return authorization context.
     */
    protected AuthorizationContext getAuthorizationContextForSubject(String subject) {
        if (subject.equals(SystemUserService.SELF_LINK)) {
            return getSystemAuthorizationContext();
        } else if (subject.equals(GuestUserService.SELF_LINK)) {
            return getGuestAuthorizationContext();
        }
        return createAuthorizationContext(subject);
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
     * Adds a service to a privileged list, allowing it to operate on authorization
     * context
     */
    protected void addPrivilegedService(Class<? extends Service> serviceType) {
        this.privilegedServiceTypes.put(serviceType.getName(), serviceType);
    }

    protected boolean isPrivilegedService(Service service) {
        // Checks if caller is privileged for auth context calls.
        boolean result = false;

        for (Class<? extends Service> privilegedService : this.privilegedServiceTypes
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
        body.reasons.add(MaintenanceReason.SERVICE_OPTION_TOGGLE);
        if (newOptions != null && newOptions.contains(ServiceOption.DOCUMENT_OWNER)) {
            body.reasons.add(MaintenanceReason.NODE_GROUP_CHANGE);
            s.adjustStat(Service.STAT_NAME_DOCUMENT_OWNER_TOGGLE_ON_MAINT_COUNT, 1);
        }

        if (removedOptions != null && removedOptions.contains(ServiceOption.DOCUMENT_OWNER)) {
            body.reasons.add(MaintenanceReason.NODE_GROUP_CHANGE);
            s.adjustStat(Service.STAT_NAME_DOCUMENT_OWNER_TOGGLE_OFF_MAINT_COUNT, 1);
        }

        if (body.reasons.contains(MaintenanceReason.NODE_GROUP_CHANGE)) {
            s.adjustStat(Service.STAT_NAME_NODE_GROUP_CHANGE_MAINTENANCE_COUNT, 1);
        }

        body.configUpdate = new ServiceConfigUpdateRequest();
        body.configUpdate.addOptions = newOptions;
        body.configUpdate.removeOptions = removedOptions;
        run(() -> {
            OperationContext.setAuthorizationContext(getSystemAuthorizationContext());
            s.handleMaintenance(Operation.createPost(s.getUri()).setBody(body));
        });
    }

    protected HttpScheme getCurrentHttpScheme() {
        boolean isListeningHttp = this.httpListener != null && this.httpListener.isListening();
        boolean isListeningHttps = this.httpsListener != null && this.httpsListener.isListening();

        if (!isListeningHttp && !isListeningHttps) {
            return HttpScheme.NONE;
        } else if (isListeningHttp && isListeningHttps) {
            return HttpScheme.HTTP_AND_HTTPS;
        } else {
            return isListeningHttp ? HttpScheme.HTTP_ONLY : HttpScheme.HTTPS_ONLY;
        }
    }

    /**
     * Returns true if the host name and port in the URI are the same as in the host instance
     */
    boolean isHostEqual(URI remoteService) {
        if (!this.state.isStarted) {
            throw new IllegalStateException("Host not in valid state");
        }

        if (getPublicUri().getPort() == remoteService.getPort()
                && getPublicUri().getHost().equals(remoteService.getHost())
                && getPublicUri().getScheme().equals(remoteService.getScheme())) {
            return true;
        }

        int remotePort = remoteService.getPort();
        if (remotePort == -1) {
            if ("https".equals(remoteService.getScheme())) {
                remotePort = 443;
            } else if ("http".equals(remoteService.getScheme())) {
                remotePort = 80;
            } else {
                // Only http/s is supported
                return false;
            }
        }

        if (getPort() != remotePort && getSecurePort() != remotePort) {
            return false;
        }

        List<String> ipAddresses = this.info.ipAddresses;
        if (ipAddresses.isEmpty()) {
            ipAddresses = getSystemInfo().ipAddresses;
            if (ipAddresses.isEmpty()) {
                throw new IllegalStateException("No IP addresses found in host:" + toString());
            }
        }

        return ipAddresses.contains(remoteService.getHost());
    }

    /**
     * Returns shutdown hook that stops this host.
     * Override this method to change the shutdown hook behavior.
     *
     * @return shutdown hook that stops the host
     */
    public Thread getRuntimeShutdownHook() {
        return this.defaultShutdownHook;
    }

    /**
     * Register host shutdown hook.
     */
    public void registerRuntimeShutdownHook() {
        Runtime.getRuntime().addShutdownHook(getRuntimeShutdownHook());
    }

    /**
     * Remove host shutdown hook.
     * @return
     */
    public boolean unregisterRuntimeShutdownHook() {
        return Runtime.getRuntime().removeShutdownHook(getRuntimeShutdownHook());
    }

    void failRequestServiceAlreadyStarted(String path, Service s, Operation post) {
        ProcessingStage st = ProcessingStage.AVAILABLE;
        if (s != null) {
            st = s.getProcessingStage();
        }

        Exception e;
        if (s != null && s.hasOption(ServiceOption.IMMUTABLE)) {
            // Even though we were able to detect violation of self-link uniqueness
            // in this case, generally we do not try to enforce uniqueness for
            // IMMUTABLE services in all cases. Instead it is the responsibility of
            // the caller to ensure uniqueness of self-links.
            e = new ServiceAlreadyStartedException(path,
                    "Self-link uniqueness not guaranteed for Immutable Services.");
            log(Level.WARNING, e.getMessage());
        } else {
            e = new ServiceAlreadyStartedException(path, st);
        }

        Operation.fail(post, Operation.STATUS_CODE_CONFLICT,
                ServiceErrorResponse.ERROR_CODE_SERVICE_ALREADY_EXISTS,
                e);
    }

}
