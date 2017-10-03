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

import static com.vmware.xenon.common.ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS;
import static com.vmware.xenon.common.serialization.GsonSerializers.getJsonMapperFor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceDocumentDescription.TypeName;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.SystemHostInfo.OsFamily;
import com.vmware.xenon.common.logging.StackAwareLogRecord;
import com.vmware.xenon.common.serialization.GsonSerializers;
import com.vmware.xenon.common.serialization.JsonMapper;
import com.vmware.xenon.common.serialization.KryoSerializers;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Runtime utility functions
 */
public final class Utils {

    public static final String PROPERTY_NAME_PREFIX = "xenon.";
    public static final Charset CHARSET_OBJECT = StandardCharsets.UTF_8;
    public static final String CHARSET = "UTF-8";
    public static final String UI_DIRECTORY_NAME = "ui";
    public static final String PROPERTY_NAME_TIME_COMPARISON = "timeComparisonEpsilonMicros";

    /**
     * Number of IO threads is used for the HTTP selector event processing. Most of the
     * work is done in the context of the service host executor so we just use a couple of threads.
     * Performance work indicates any more threads do not help, rather, they hurt throughput
     */
    public static final int DEFAULT_IO_THREAD_COUNT = Math.min(2, Runtime.getRuntime()
            .availableProcessors());

    /**
     * Number of threads used for the service host executor and shared across service instances.
     * We add to the total count since the executor will also be used to process I/O selector
     * events, which will consume threads. Using much more than the number of processors hurts
     * operation processing throughput.
     */
    public static final int DEFAULT_THREAD_COUNT = Math.max(4, Runtime.getRuntime()
            .availableProcessors());

    /**
     * See {@link #setTimeDriftThreshold(long)}
     */
    public static final long DEFAULT_TIME_DRIFT_THRESHOLD_MICROS = TimeUnit.SECONDS.toMicros(1);

    /**
     * {@link #isReachableByPing} launches a separate ping process to ascertain whether a given IP
     * address is reachable within a specified timeout. This constant extends the timeout for that
     * check to account for the start-up overhead of that process.
     */
    private static final long PING_LAUNCH_TOLERANCE_MS = 50;

    private static final ThreadLocal<CharsetDecoder> decodersPerThread = ThreadLocal
            .withInitial(CHARSET_OBJECT::newDecoder);

    private static final AtomicLong previousTimeValue = new AtomicLong();
    private static long timeComparisonEpsilon = initializeTimeEpsilon();
    private static long timeDriftThresholdMicros = DEFAULT_TIME_DRIFT_THRESHOLD_MICROS;


    private static final ConcurrentMap<String, String> KINDS = new ConcurrentSkipListMap<>();
    private static final ConcurrentMap<String, Class<?>> KIND_TO_TYPE = new ConcurrentSkipListMap<>();


    private static final StringBuilderThreadLocal builderPerThread = new StringBuilderThreadLocal();

    private Utils() {
    }

    /**
     * Registers a specialized {@link JsonMapper} that should be used when serializing instances of
     * the specified class. This is useful when the class in question contains members that might
     * require special handling e.g. custom type adapters.
     *
     * @param clazz
     *            Identifies the class to which the custom serialization should occur. Will not be
     *            applicable for sub-classes or instances of this class embedded as a members in
     *            other (non-registered) types.
     *
     * @param mapper
     *            A {@link JsonMapper} for serializing/de-serializing service documents to/from
     *            JSON.
     */
    public static void registerCustomJsonMapper(Class<?> clazz,
            JsonMapper mapper) {
        GsonSerializers.registerCustomJsonMapper(clazz, mapper);
    }

    /**
     * Registers a thread local variable that supplies {@link Kryo} instances used to serialize
     * documents or objects. The KRYO instance supplied must be identical across all nodes in
     * a node group and behave exactly the same way regardless of service start order.
     *
     * This method must be called before any service host is created inside the process, to avoid
     * non deterministic behavior, where some serialization actions use the build in instances,
     * while others use the user supplied ones
     * @param kryoThreadLocal Thread local variable that supplies the KRYO instance
     * @param isDocumentSerializer True if instance should by used for
     * {@link Utils#toBytes(Object, byte[], int)} and
     * {@link Utils#fromBytes(byte[], int, int)}
     */
    public static void registerCustomKryoSerializer(ThreadLocal<Kryo> kryoThreadLocal,
            boolean isDocumentSerializer) {
        KryoSerializers.register(kryoThreadLocal, isDocumentSerializer);
    }

    public static <T> T clone(T t) {
        return KryoSerializers.clone(t);
    }

    public static <T> T cloneObject(T t) {
        return KryoSerializers.cloneObject(t);
    }

    public static String computeSignature(ServiceDocument s, ServiceDocumentDescription description) {
        if (description == null) {
            throw new IllegalArgumentException("description is required");
        }

        long hash = FNVHash.FNV_OFFSET_MINUS_MSB;

        for (PropertyDescription pd : description.propertyDescriptions.values()) {
            if (pd.indexingOptions != null) {
                if (pd.indexingOptions.contains(PropertyIndexingOption.EXCLUDE_FROM_SIGNATURE)) {
                    continue;
                }
            }

            // Calculate hash of all non-excluded fields, to make sure the following:
            // {a='1', b=null} != {a=null, b='1'}
            Object fieldValue = ReflectionUtils.getPropertyValue(pd, s);
            if (fieldValue == null) {
                hash = FNVHash.compute(-1, hash);
                continue;
            }

            switch (pd.typeName) {
            case BYTES:
                // special case for bytes to avoid base64 encoding
                byte[] bytes = (byte[]) fieldValue;
                hash = FNVHash.compute(bytes, 0, bytes.length, hash);
                break;
            default:
                hash = GsonSerializers.hashJson(fieldValue, hash);
            }
        }

        return Long.toHexString(hash);
    }

    /**
     * See {@link KryoSerializers#getBuffer(int)}
     */
    public static byte[] getBuffer(int capacity) {
        return KryoSerializers.getBuffer(capacity);
    }


    /**
     * See {@link KryoSerializers#serializeObject(Object, byte[], int)}
     */
    public static int toBytes(Object o, byte[] buffer, int position) {
        return KryoSerializers.serializeObject(o, buffer, position);
    }

    /**
     * See {@link KryoSerializers#deserializeObject(byte[], int, int)}
     */
    public static Object fromBytes(byte[] bytes) {
        return KryoSerializers.deserializeObject(bytes, 0, bytes.length);
    }

    /**
     * See {@link KryoSerializers#deserializeObject(byte[], int, int)}
     */
    public static Object fromBytes(byte[] bytes, int position, int length) {
        return KryoSerializers.deserializeObject(bytes, position, length);
    }

    /**
     * Deserializes bytes into ServiceDocument.
     * See {@link KryoSerializers#deserializeDocument(byte[], int, int)}
     */
    public static ServiceDocument fromQueryBinaryDocument(String link, Object binaryData) {
        ServiceDocument serviceDocument = (ServiceDocument) KryoSerializers
                .deserializeDocument((ByteBuffer) binaryData);
        if (serviceDocument.documentSelfLink == null) {
            serviceDocument.documentSelfLink = link;
        }
        if (serviceDocument.documentKind == null) {
            serviceDocument.documentKind = Utils.buildKind(serviceDocument.getClass());
        }
        return serviceDocument;
    }

    public static void performMaintenance() {

    }

    public static String computeHash(CharSequence content) {
        return Long.toHexString(FNVHash.compute(content));
    }

    public static String toJson(Object body) {
        if (body instanceof String) {
            return (String) body;
        }
        StringBuilder content = getBuilder();
        JsonMapper mapper = getJsonMapperFor(body);
        mapper.toJson(body, content);
        return content.toString();
    }

    public static String toJsonHtml(Object body) {
        if (body instanceof String) {
            return (String) body;
        }
        StringBuilder content = getBuilder();
        JsonMapper mapper = getJsonMapperFor(body);
        mapper.toJsonHtml(body, content);
        return content.toString();
    }

    /**
     * Outputs a JSON representation of the given object using useHTMLFormatting to create pretty-printed,
     * HTML-friendly JSON or compact JSON. If hideSensitiveFields is set the JSON will not include fields
     * with the annotation {@link PropertyUsageOption#SENSITIVE}.
     * If hideSensitiveFields is set and the Object is a string with JSON, sensitive fields cannot be discovered will
     * throw an Exception.
     *
     * @deprecated Use {@link #toJson(Set, Object)} instead
     */
    @Deprecated
    public static String toJson(boolean hideSensitiveFields, boolean useHtmlFormatting, Object body)
            throws IllegalArgumentException {
        Set<JsonMapper.JsonOptions> options = new HashSet<>();
        if (hideSensitiveFields) {
            options.add(JsonMapper.JsonOptions.EXCLUDE_SENSITIVE);
        }
        if (!useHtmlFormatting) {
            options.add(JsonMapper.JsonOptions.COMPACT);
        }

        return toJson(options, body);
    }

    /**
     * Outputs {@code body} to a JSON String format based on the provided JSON {@code options}
     *
     * @param options See {@link com.vmware.xenon.common.serialization.JsonMapper.JsonOptions} for
     *                supported JSON output options. This cannot be null.
     * @param body the object to serialize to JSON
     * @return the JSON String
     */
    public static String toJson(Set<JsonMapper.JsonOptions> options, Object body) {
        if (body instanceof String) {
            if (options.contains(JsonMapper.JsonOptions.EXCLUDE_SENSITIVE)) {
                throw new IllegalArgumentException(
                        "Body is already a string, sensitive fields cannot be discovered");
            }
            return (String) body;
        }
        StringBuilder content = getBuilder();
        JsonMapper mapper = getJsonMapperFor(body);

        mapper.toJson(options, body, content);
        return content.toString();
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        return getJsonMapperFor(clazz).fromJson(json, clazz);
    }

    public static <T> T fromJson(Object json, Class<T> clazz) {
        return getJsonMapperFor(clazz).fromJson(json, clazz);
    }

    public static <T> T fromJson(Object json, Type type) {
        return getJsonMapperFor(type).fromJson(json, type);
    }

    public static <T> T getJsonMapValue(Object json, String key, Class<T> valueClazz) {
        JsonElement je = Utils.fromJson(json, JsonElement.class);
        je = je.getAsJsonObject().get(key);
        if (je == null) {
            return null;
        }
        return Utils.fromJson(je, valueClazz);
    }

    public static <T> T getJsonMapValue(Object json, String key, Type valueType) {
        JsonElement je = Utils.fromJson(json, JsonElement.class);
        je = je.getAsJsonObject().get(key);
        if (je == null) {
            return null;
        }
        return Utils.fromJson(je, valueType);
    }

    public static String toString(Throwable t) {
        StringWriter writer = new StringWriter();
        try (PrintWriter printer = new PrintWriter(writer)) {
            t.printStackTrace(printer);
        }

        return writer.toString();
    }

    public static String toString(Map<?, Throwable> exceptions) {
        StringWriter writer = new StringWriter();
        try (PrintWriter printer = new PrintWriter(writer)) {
            for (Throwable t : exceptions.values()) {
                t.printStackTrace(printer);
            }
        }

        return writer.toString();
    }

    public static void log(Class<?> type, String classOrUri, Level level, String fmt,
            Object... args) {
        Logger lg = Logger.getLogger(type.getName());
        log(lg, 3, classOrUri, level, () -> String.format(fmt, args));
    }

    public static void log(Class<?> type, String classOrUri, Level level,
            Supplier<String> messageSupplier) {
        Logger lg = Logger.getLogger(type.getName());
        log(lg, 3, classOrUri, level, messageSupplier);
    }

    public static void log(Logger lg, Integer nestingLevel, String classOrUri, Level level,
            String fmt, Object... args) {
        log(lg, nestingLevel, classOrUri, level, () -> String.format(fmt, args));
    }

    public static void log(Logger lg, Integer nestingLevel, String classOrUri, Level level,
            Supplier<String> messageSupplier) {
        if (!lg.isLoggable(level)) {
            return;
        }
        if (nestingLevel == null) {
            nestingLevel = 2;
        }

        String message = messageSupplier.get();
        StackAwareLogRecord lr = new StackAwareLogRecord(level, message);
        Exception e = new Exception();
        StackTraceElement[] stacks = e.getStackTrace();
        if (stacks.length > nestingLevel) {
            StackTraceElement stack = stacks[nestingLevel];
            lr.setStackElement(stack);
            lr.setSourceMethodName(stack.getMethodName());
        }
        lr.setSourceClassName(classOrUri);
        lr.setLoggerName(lg.getName());
        lg.log(lr);
    }

    public static void logWarning(String fmt, Object... args) {
        Logger.getAnonymousLogger().warning(String.format(fmt, args));
    }

    public static String toDocumentKind(Class<?> type) {
        return type.getCanonicalName().replace('.', ':');
    }

    /**
     * Obtain the canonical name for a class from the entry in the documentKind field
     */
    public static String fromDocumentKind(String kind) {
        return kind.replace(':', '.');
    }

    /**
     * Registers mapping between a type and document kind string the runtime
     * will use for all services with that state type
     */
    public static String registerKind(Class<?> type, String kind) {
        KIND_TO_TYPE.put(kind, type);
        return KINDS.put(type.getName(), kind);
    }

    /**
     * Obtain the class for the specified kind. Only classes registered via
     * {@code Utils#registerKind(Class, String)} will be returned
     */
    public static Class<?> getTypeFromKind(String kind) {
        return KIND_TO_TYPE.get(kind);
    }

    /**
     * Builds a kind string from a type. It uses a cache to lookup the type to kind
     * mapping. The mapping can be overridden with {@code Utils#registerKind(Class, String)}
     */
    public static String buildKind(Class<?> type) {
        return KINDS.computeIfAbsent(type.getName(), name -> toDocumentKind(type));
    }

    public static ServiceErrorResponse toServiceErrorResponse(Throwable e) {
        return ServiceErrorResponse.create(e, Operation.STATUS_CODE_BAD_REQUEST);
    }

    public static ServiceErrorResponse toValidationErrorResponse(Throwable t, Operation op) {
        ServiceErrorResponse rsp = new ServiceErrorResponse();

        if (t instanceof LocalizableValidationException) {
            String localizedMessage = LocalizationUtil.resolveMessage((LocalizableValidationException) t, op);
            rsp.message = localizedMessage;
        } else {
            rsp.message = t.getLocalizedMessage();
        }
        return rsp;
    }

    public static boolean isValidationError(Throwable e) {
        return (e instanceof IllegalArgumentException)
                || (e instanceof LocalizableValidationException);
    }

    /**
     * Compute path to static resources for service.
     *
     * For example: the class "com.vmware.xenon.services.common.ExampleService" is converted to
     * "com/vmware/xenon/services/common/ExampleService".
     *
     * @param klass Service class
     * @return String
     */
    public static String buildServicePath(Class<? extends Service> klass) {
        return klass.getName().replace('.', '/');
    }

    /**
     * Compute URI prefix for static resources of a service.
     *
     * @param klass Service class
     * @return String
     */
    public static String buildUiResourceUriPrefixPath(Class<? extends Service> klass) {
        return UriUtils.buildUriPath(ServiceUriPaths.UI_RESOURCES,
                buildServicePath(klass));
    }

    /**
     * Compute URI prefix for static resources of a service.
     *
     * @param service Service
     * @return String
     */
    public static String buildUiResourceUriPrefixPath(Service service) {
        return buildUiResourceUriPrefixPath(service.getClass());
    }

    /**
     * Compute URI prefix for static resources of a service with custom UI resource path.
     *
     * @param service
     * @return String
     */
    public static String buildCustomUiResourceUriPrefixPath(Service service) {
        return UriUtils.buildUriPath(ServiceUriPaths.UI_RESOURCES,
                service.getDocumentTemplate().documentDescription.userInterfaceResourcePath);
    }

    public static Object setJsonProperty(Object body, String fieldName, String fieldValue) {
        JsonObject jo;
        if (body instanceof JsonObject) {
            jo = (JsonObject) body;
        } else {
            jo = new JsonParser().parse((String) body).getAsJsonObject();
        }
        jo.remove(fieldName);
        if (fieldValue != null) {
            jo.addProperty(fieldName, fieldValue);
        }

        return jo;
    }

    public static String validateServiceOption(EnumSet<ServiceOption> options,
            ServiceOption option) {
        if (!options.contains(option)) {
            return null;
        }

        EnumSet<ServiceOption> reqs = EnumSet.noneOf(ServiceOption.class);
        EnumSet<ServiceOption> antiReqs = EnumSet.noneOf(ServiceOption.class);
        switch (option) {
        case CONCURRENT_UPDATE_HANDLING:
            antiReqs = EnumSet.of(ServiceOption.OWNER_SELECTION,
                    ServiceOption.STRICT_UPDATE_CHECKING);
            break;
        case OWNER_SELECTION:
            reqs = EnumSet.of(ServiceOption.REPLICATION);
            antiReqs = EnumSet.of(ServiceOption.CONCURRENT_UPDATE_HANDLING);
            break;
        case STRICT_UPDATE_CHECKING:
            antiReqs = EnumSet.of(ServiceOption.CONCURRENT_UPDATE_HANDLING);
            break;
        case URI_NAMESPACE_OWNER:
            antiReqs = EnumSet.of(ServiceOption.PERSISTENCE, ServiceOption.REPLICATION);
            break;
        case PERIODIC_MAINTENANCE:
            antiReqs = EnumSet.of(ServiceOption.IMMUTABLE);
            break;
        case PERSISTENCE:
            break;
        case REPLICATION:
            break;
        case DOCUMENT_OWNER:
            break;
        case IDEMPOTENT_POST:
            break;
        case CORE:
            break;
        case FACTORY:
            break;
        case FACTORY_ITEM:
            break;
        case HTML_USER_INTERFACE:
            break;
        case INSTRUMENTATION:
            antiReqs = EnumSet.of(ServiceOption.IMMUTABLE);
            break;
        case LIFO_QUEUE:
            break;
        case NONE:
            break;
        case UTILITY:
            break;
        case IMMUTABLE:
            reqs = EnumSet.of(ServiceOption.PERSISTENCE);
            antiReqs = EnumSet.of(ServiceOption.PERIODIC_MAINTENANCE,
                    ServiceOption.INSTRUMENTATION);
            break;
        case TRANSACTION_PENDING:
            break;
        case STATELESS:
            antiReqs = EnumSet.of(ServiceOption.PERSISTENCE, ServiceOption.REPLICATION,
                    ServiceOption.OWNER_SELECTION, ServiceOption.STRICT_UPDATE_CHECKING);
            break;
        default:
            break;
        }

        if (reqs.isEmpty() && antiReqs.isEmpty()) {
            return null;
        }

        if (!reqs.isEmpty()) {
            EnumSet<ServiceOption> missingReqs = EnumSet.noneOf(ServiceOption.class);
            for (ServiceOption r : reqs) {
                if (!options.contains(r)) {
                    missingReqs.add(r);
                }
            }

            if (!missingReqs.isEmpty()) {
                String error = String
                        .format("%s missing required options: %s", option, missingReqs);
                return error;
            }
        }

        EnumSet<ServiceOption> conflictReqs = EnumSet.noneOf(ServiceOption.class);
        for (ServiceOption r : antiReqs) {
            if (options.contains(r)) {
                conflictReqs.add(r);
            }
        }

        if (!conflictReqs.isEmpty()) {
            String error = String.format("%s conflicts with options: %s", option, conflictReqs);
            return error;
        }

        return null;
    }

    /**
     * Infrastructure use only
     */
    static boolean validateServiceOptions(ServiceHost host, Service service, Operation post) {
        for (ServiceOption o : service.getOptions()) {
            String error = Utils.validateServiceOption(service.getOptions(), o);
            if (error != null) {
                host.log(Level.WARNING, "%s", error);
                post.fail(new IllegalArgumentException(error));
                return false;
            }
        }

        if (service.getMaintenanceIntervalMicros() > 0 &&
                service.getMaintenanceIntervalMicros() < host.getMaintenanceCheckIntervalMicros()) {
            host.setMaintenanceCheckIntervalMicros(service.getMaintenanceIntervalMicros());
        }
        return true;
    }

    /**
     * An alternative to {@link InetAddress#isReachable(int)} which accounts for the Windows
     * implementation of that method NOT using ICMP. This method invokes the "ping" command
     * installed in all Windows implementations since Windows XP. For other operating systems it
     * will fall back on the default implementation of the original method.
     */
    public static boolean isReachable(SystemHostInfo systemInfo, InetAddress addr, long timeoutMs)
            throws IOException {
        if (systemInfo.osFamily == OsFamily.WINDOWS) {
            // windows -> delegate to "ping"
            return isReachableByPing(systemInfo, addr, timeoutMs);
        }

        // non-windows -> fallback on default impl
        return addr.isReachable((int) timeoutMs);
    }

    public static boolean isReachableByPing(SystemHostInfo systemInfo, InetAddress addr,
            long timeoutMs) throws IOException {
        try {
            Process process = new ProcessBuilder("ping",
                    "-n", "1",
                    "-w", Long.toString(timeoutMs),
                    getNormalizedHostAddress(systemInfo, addr))
                    .start();
            boolean completed = process.waitFor(
                    PING_LAUNCH_TOLERANCE_MS + timeoutMs,
                    TimeUnit.MILLISECONDS);
            return completed && process.exitValue() == 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * An alternative to {@link InetAddress#getHostAddress()} that formats particular types of IP
     * address in more universal formats.
     *
     * Specifically, Java formats link-local IPv6 addresses in Linux-friendly manner:
     * {@code <address>%<interface_name>} e.g. {@code fe80:0:0:0:5971:14f6:c8ac:9e8f%eth0}. However,
     * Windows requires a different format for such addresses: {@code <address>%<numeric_scope_id>}
     * e.g. {@code fe80:0:0:0:5971:14f6:c8ac:9e8f%34}. The method
     * {@link SystemHostInfo#determineOsFamily(String)}  detects if
     * the OS on the host} and will adjust the host address accordingly.
     *
     * Otherwise, this will delegate to the original method.
     */
    public static String getNormalizedHostAddress(SystemHostInfo systemInfo, InetAddress addr) {
        String addrStr = addr.getHostAddress();

        // does it require special treatment?
        if (systemInfo.osFamily == OsFamily.WINDOWS
                && addr instanceof Inet6Address
                && addr.isLinkLocalAddress()) {
            // Inet6Address appends the intf name, rather than numeric id -> remedying
            Inet6Address ip6Addr = (Inet6Address) addr;
            int pct = addrStr.lastIndexOf('%');
            if (pct > -1) {
                addrStr = addrStr.substring(0, pct) + '%' + ip6Addr.getScopeId();
            }
        }

        return addrStr;
    }

    /**
     * Infrastructure use. Serializes linked state associated with source operation
     * and sets the result as the body of the target operation
     */
    public static void encodeAndTransferLinkedStateToBody(Operation source, Operation target,
            boolean useBinary) {
        if (useBinary && source.getAction() != Action.POST) {
            try {
                byte[] encodedBody = Utils.encodeBody(source, source.getLinkedState(),
                        Operation.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM, true);
                source.linkSerializedState(encodedBody);
            } catch (Exception e2) {
                Utils.logWarning("Failure binary serializing, will fallback to JSON: %s",
                        Utils.toString(e2));
            }
        }

        if (!source.hasLinkedSerializedState()) {
            target.setContentType(Operation.MEDIA_TYPE_APPLICATION_JSON);
            target.setBodyNoCloning(Utils.toJson(source.getLinkedState()));
        } else {
            target.setContentType(Operation.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM);
            target.setBodyNoCloning(source.getLinkedSerializedState());
        }
    }

    public static byte[] encodeBody(Operation op, boolean isRequest) throws Exception {
        return encodeBody(op, op.getBodyRaw(), op.getContentType(), isRequest);
    }

    public static byte[] encodeBody(Operation op, Object body, String contentType, boolean isRequest)
            throws Exception {
        if (body == null) {
            op.setContentLength(0);
            return null;
        }

        byte[] data = null;
        if (body instanceof String) {
            data = ((String) body).getBytes(Utils.CHARSET);
            op.setContentLength(data.length);
        } else if (body instanceof byte[]) {
            data = (byte[]) body;
            if (contentType == null) {
                op.setContentType(Operation.MEDIA_TYPE_APPLICATION_OCTET_STREAM);
            }
            if (op.getContentLength() == 0 || op.getContentLength() > data.length) {
                op.setContentLength(data.length);
            }
        } else if (Operation.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM.equals(contentType)) {
            Output o = KryoSerializers.serializeAsDocument(
                    body,
                    ServiceClient.MAX_BINARY_SERIALIZED_BODY_LIMIT);
            // incur a memory copy since the byte array can be used across many threads in the
            // I/O path
            data = o.toBytes();
            op.setContentLength(data.length);
        }

        if (data == null) {
            String encodedBody;
            encodedBody = Utils.toJson(body);
            if (op.getAction() != Action.GET && contentType == null) {
                op.setContentType(Operation.MEDIA_TYPE_APPLICATION_JSON);
            }
            data = encodedBody.getBytes(Utils.CHARSET);
            op.setContentLength(data.length);
        }

        // For requests, if encoding is specified as gzip, then compress body
        // For responses, if request accepts gzip body, then compress body and add response header
        boolean gzip = false;
        if (isRequest) {
            String encoding = op.getRequestHeader(Operation.CONTENT_ENCODING_HEADER);
            gzip = Operation.CONTENT_ENCODING_GZIP.equals(encoding);
        } else {
            String encoding = op.getRequestHeader(Operation.ACCEPT_ENCODING_HEADER);
            // encoding can be of form br;q=1.0, gzip;q=0.8, *;q=0.1
            // see https://tools.ietf.org/html/rfc7231#section-5.3.4
            if (encoding != null) {
                String[] encodings = encoding.split(",");
                for (String enc : encodings) {
                    int idx = enc.indexOf(';');
                    if (idx > 0) {
                        enc = enc.substring(0, idx);
                    }
                    if (Operation.CONTENT_ENCODING_GZIP.equals(enc.trim())) {
                        gzip = true;
                        break;
                    }
                }
            }
        }
        if (gzip) {
            data = compressGZip(data);
            op.setContentLength(data.length);
            if (!isRequest) {
                op.addResponseHeader(Operation.CONTENT_ENCODING_HEADER, Operation.CONTENT_ENCODING_GZIP);
            }
        }

        return data;
    }

   /**
     * Compresses byte[] to gzip byte[]
     */
    private static byte[] compressGZip(byte[] input) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream zos = new GZIPOutputStream(out)) {
            zos.write(input, 0, input.length);
        }
        return out.toByteArray();
    }

    /**
     * Decodes the byte buffer, using the content type as a hint. It sets the operation body
     * to the decoded instance. It does not complete the operation.
     */
    public static void decodeBody(Operation op, ByteBuffer buffer, boolean isRequest)
            throws Exception {
        String contentEncodingHeader = null;
        if (!isRequest) {
            contentEncodingHeader = op.getResponseHeaderAsIs(Operation.CONTENT_ENCODING_HEADER);
        } else if (!op.isFromReplication()) {
            contentEncodingHeader = op.getRequestHeaderAsIs(Operation.CONTENT_ENCODING_HEADER);
        }

        boolean compressed = false;
        if (contentEncodingHeader != null) {
            compressed = Operation.CONTENT_ENCODING_GZIP.equals(contentEncodingHeader);
        }

        decodeBody(op, buffer, isRequest, compressed);
    }

    /**
     * See {@link #decodeBody(Operation, ByteBuffer, boolean)}
     */
    public static void decodeBody(
            Operation op, ByteBuffer buffer, boolean isRequest, boolean compressed)
            throws Exception {
        if (op.getContentLength() == 0) {
            op.setContentType(Operation.MEDIA_TYPE_APPLICATION_JSON);
            return;
        }
        if (compressed) {
            buffer = decompressGZip(buffer);
            if (isRequest) {
                op.getRequestHeaders().remove(Operation.CONTENT_ENCODING_HEADER);
            } else {
                op.getResponseHeaders().remove(Operation.CONTENT_ENCODING_HEADER);
            }
        }

        String contentType = op.getContentType();
        boolean isKryoBinary = isContentTypeKryoBinary(contentType);

        if (isKryoBinary) {
            byte[] data = new byte[(int) op.getContentLength()];
            buffer.get(data);
            Object body = KryoSerializers.deserializeDocument(data, 0, data.length);
            if (op.isFromReplication()) {
                // optimization to avoid having to serialize state again, during indexing
                op.linkSerializedState(data);
            }
            op.setBodyNoCloning(body);
            return;
        }

        Object body = decodeIfText(buffer, contentType);
        if (body != null) {
            op.setBodyNoCloning(body);
            return;
        }

        // unrecognized or binary body, use the raw bytes
        byte[] data = new byte[(int) op.getContentLength()];
        buffer.get(data);
        op.setBodyNoCloning(data);
    }

    public static String decodeIfText(ByteBuffer buffer, String contentType)
            throws CharacterCodingException {
        if (contentType == null) {
            return null;
        }

        String body = null;
        if (isContentTypeText(contentType)) {
            CharsetDecoder decoder = decodersPerThread.get().reset();
            body = decoder.decode(buffer).toString();
        } else if (contentType.contains(Operation.MEDIA_TYPE_APPLICATION_X_WWW_FORM_ENCODED)) {
            CharsetDecoder decoder = decodersPerThread.get().reset();
            body = decoder.decode(buffer).toString();
            try {
                body = URLDecoder.decode(body, Utils.CHARSET);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        return body;
    }

    private static ByteBuffer decompressGZip(ByteBuffer bb) throws Exception {
        GZIPInputStream zis = new GZIPInputStream(new ByteBufferInputStream(bb));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            byte[] buffer = Utils.getBuffer(1024);
            int len;
            while ((len = zis.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }
        } finally {
            zis.close();
            out.close();
        }
        return ByteBuffer.wrap(out.toByteArray());
    }

    /**
     * Compresses text to gzip byte buffer.
     */
    public static ByteBuffer compressGZip(String text) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream zos = new GZIPOutputStream(out)) {
            byte[] bytes = text.getBytes(CHARSET);
            zos.write(bytes, 0, bytes.length);
        }

        return ByteBuffer.wrap(out.toByteArray());
    }

    public static boolean isContentTypeKryoBinary(String contentType) {
        return contentType.length() == Operation.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM.length()
                && contentType.charAt(12) == 'k'
                && contentType.charAt(13) == 'r'
                && contentType.charAt(14) == 'y'
                && contentType.charAt(15) == 'o';
    }

    private static boolean isContentTypeText(String contentType) {
        return Operation.MEDIA_TYPE_APPLICATION_JSON.equals(contentType)
                || contentType.contains(Operation.MEDIA_TYPE_APPLICATION_JSON)
                || contentType.contains("text")
                || contentType.contains("css")
                || contentType.contains("script")
                || contentType.contains("html")
                || contentType.contains("xml")
                || contentType.contains("yaml")
                || contentType.contains("yml");
    }

    /**
     * Compute ui resource path for this service.
     * <p>
     * If service has defined the custom path on ServiceDocumentDescription
     * userInterfaceResourcePath field that will be used  else default UI path
     * will be calculated using service path Eg. for ExampleService
     * default path will be ui/com/vmware/xenon/services/common/ExampleService
     *
     * @param s service class for which UI path has to be extracted
     * @return UI resource path object
     */
    public static Path getServiceUiResourcePath(Service s) {
        ServiceDocument sd = s.getDocumentTemplate();
        ServiceDocumentDescription sdd = sd.documentDescription;
        if (sdd != null && sdd.userInterfaceResourcePath != null) {
            String resourcePath = sdd.userInterfaceResourcePath;
            if (!resourcePath.isEmpty()) {
                return Paths.get(resourcePath);
            } else {
                log(Utils.class, Utils.class.getSimpleName(), Level.SEVERE,
                        "UserInterface resource path field empty for service document %s",
                        s.getClass().getSimpleName());
            }
        } else {
            String servicePath = buildServicePath(s.getClass());
            return Paths.get(UI_DIRECTORY_NAME, servicePath);
        }
        return null;
    }

    /**
     * Merges {@code patch} object into the {@code source} object by replacing or updating all {@code source}
     *  fields with non-null {@code patch} fields. Only fields with specified merge policy are merged.
     *
     * NOTE:
     * When {@code patch.documentExpirationTimeMicros} is 0, {@code source.documentExpirationTimeMicros}
     * will NOT be updated.
     * If you need to always update the source, you need explicitly set it.
     * <pre>
     * {@code
     *   Utils.mergeWithState(getStateDescription(), source, patchState);
     *   source.documentExpirationTimeMicros = patchState.documentExpirationTimeMicros;
     * }
     * </pre>
     *
     * @param desc Service document description.
     * @param source Source object.
     * @param patch  Patch object.
     * @param <T>    Object type.
     * @return {@code true} in case there was at least one update. For objects that are not collections
     *  or maps, updates of fields to same values are not considered as updates. New elements are always
     *  added to collections/maps. Elements may replace existing entries based on the collection type
     * @see ServiceDocumentDescription.PropertyUsageOption
     */
    public static <T extends ServiceDocument> boolean mergeWithState(
            ServiceDocumentDescription desc,
            T source, T patch) {
        Class<? extends ServiceDocument> clazz = source.getClass();
        if (!patch.getClass().equals(clazz)) {
            throw new IllegalArgumentException("Source object and patch object types mismatch");
        }
        boolean modified = false;
        for (PropertyDescription prop : desc.propertyDescriptions.values()) {
            if (prop.usageOptions != null &&
                    prop.usageOptions.contains(PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)) {
                Object o = ReflectionUtils.getPropertyValue(prop, patch);
                if (o != null) {

                    // when patch.documentExpirationTimeMicros is 0, do not override source.documentExpirationTimeMicros
                    if (FIELD_NAME_EXPIRATION_TIME_MICROS.equals(prop.accessor.getName())
                            && Long.valueOf(0).equals(o)) {
                        continue;
                    }

                    if ((prop.typeName == TypeName.COLLECTION && !o.getClass().isArray())
                            || prop.typeName == TypeName.MAP) {
                        modified |= ReflectionUtils.setOrUpdatePropertyValue(prop, source, o);
                    } else {
                        if (!o.equals(ReflectionUtils.getPropertyValue(prop, source))) {
                            ReflectionUtils.setPropertyValue(prop, source, o);
                            modified = true;
                        }
                    }
                }
            }
        }
        return modified;
    }

    /**
     * Update the state of collections that are part of the service state
     * @param currentState The current state
     * @param op Operation with the patch request
     * @return
     * @throws IllegalAccessException
     * @throws NoSuchFieldException
     */
    public static <T extends ServiceDocument> boolean mergeWithState(T currentState, Operation op)
            throws NoSuchFieldException, IllegalAccessException {
        ServiceStateCollectionUpdateRequest collectionUpdateRequest =
                op.getBody(ServiceStateCollectionUpdateRequest.class);
        if (ServiceStateCollectionUpdateRequest.KIND.equals(collectionUpdateRequest.kind)) {
            Utils.updateCollections(currentState, collectionUpdateRequest);
            return true;
        }

        ServiceStateMapUpdateRequest mapUpdateRequest =
                op.getBody(ServiceStateMapUpdateRequest.class);
        if (ServiceStateMapUpdateRequest.KIND.equals(mapUpdateRequest.kind)) {
            Utils.updateMaps(currentState, mapUpdateRequest);
            return true;
        }

        return false;
    }

    /**
     * Contains flags describing the result of a state merging operation through the
     * {@link Utils#mergeWithStateAdvanced} method.
     */
    public enum MergeResult {
        SPECIAL_MERGE,   // whether the patch body represented a special update request
                         // (if not set, the patch body is assumed to be a service state)
        STATE_CHANGED    // whether the current state was changed as a result of the merge
    }

    /**
     * Merges the given patch body into the provided current service state. It first checks for
     * patch bodies representing special update requests (such as
     * {@link ServiceStateCollectionUpdateRequest} or others in the future) and if not, assumes
     * the patch body is a new service state and merges it into the current state according to
     * the provided {@link ServiceDocumentDescription} (see
     * {@link Utils#mergeWithState(ServiceDocumentDescription, ServiceDocument, ServiceDocument)}).
     *
     * @param desc Metadata about the service document state
     * @param currentState The current service state
     * @param type Service state type
     * @param op Operation with the patch request
     * @return an EnumSet with information whether the operation represented an update request and
     *         whether the merge changed the current state
     * @throws IllegalAccessException
     * @throws NoSuchFieldException
     */
    public static <T extends ServiceDocument> EnumSet<MergeResult> mergeWithStateAdvanced(
            ServiceDocumentDescription desc, T currentState, Class<T> type, Operation op)
            throws NoSuchFieldException, IllegalAccessException {
        EnumSet<MergeResult> result = EnumSet.noneOf(MergeResult.class);

        // first check for a ServiceStateCollectionUpdateRequest patch body
        ServiceStateCollectionUpdateRequest requestBody =
                op.getBody(ServiceStateCollectionUpdateRequest.class);
        if (ServiceStateCollectionUpdateRequest.KIND.equals(requestBody.kind)) {
            result.add(MergeResult.SPECIAL_MERGE);
            if (Utils.updateCollections(currentState, requestBody)) {
                result.add(MergeResult.STATE_CHANGED);
            }
            return result;
        }

        // check for a ServiceStateMapUpdateRequest patch body
        ServiceStateMapUpdateRequest mapUpdateRequest =
                op.getBody(ServiceStateMapUpdateRequest.class);
        if (ServiceStateMapUpdateRequest.KIND.equals(mapUpdateRequest.kind)) {
            result.add(MergeResult.SPECIAL_MERGE);
            if (Utils.updateMaps(currentState, mapUpdateRequest)) {
                result.add(MergeResult.STATE_CHANGED);
            }
            return result;
        }

        // if not a special update request patch body, assume it is a new service state
        T patchState = op.getBody(type);
        if (Utils.mergeWithState(desc, currentState, patchState)) {
            result.add(MergeResult.STATE_CHANGED);
        }
        return result;
    }

    /**
     * Validates {@code state} object by checking for null value fields.
     *
     * @param desc Service document description.
     * @param state Source object.
     * @param <T>    Object type.
     * @see ServiceDocumentDescription.PropertyUsageOption
     */
    public static <T extends ServiceDocument> void validateState(
            ServiceDocumentDescription desc, T state) {
        for (PropertyDescription prop : desc.propertyDescriptions.values()) {
            if (prop.usageOptions != null &&
                    prop.usageOptions.contains(PropertyUsageOption.REQUIRED)) {
                Object o = ReflectionUtils.getPropertyValue(prop, state);
                if (o == null) {
                    if (prop.usageOptions.contains(PropertyUsageOption.ID)) {
                        ReflectionUtils.setPropertyValue(prop, state, UUID.randomUUID().toString());
                    } else {
                        throw new IllegalArgumentException(
                                prop.accessor.getName() + " is a required field.");
                    }
                }
            }
        }
    }

    private static long initializeTimeEpsilon() {
        return Long.getLong(Utils.PROPERTY_NAME_PREFIX + PROPERTY_NAME_TIME_COMPARISON,
                ServiceHostState.DEFAULT_OPERATION_TIMEOUT_MICROS);
    }

    /**
     * Adds the supplied argument to the value from {@link #getSystemNowMicrosUtc()} and returns
     * an absolute expiration time in the future
     */
    public static long fromNowMicrosUtc(long deltaMicros) {
        return getSystemNowMicrosUtc() + deltaMicros;
    }

    /**
     * Expects an absolute time, in microseconds since Epoch and returns true if the value represents
     * a time before the current system time
     */
    public static boolean beforeNow(long microsUtc) {
        return getSystemNowMicrosUtc() >= microsUtc;
    }

    /**
     * Returns the current time in microseconds, since Unix Epoch. This method can return the
     * same value on consecutive calls. See {@link #getNowMicrosUtc()} for an alternative but
     * with potential for drift from wall clock time
     */
    public static long getSystemNowMicrosUtc() {
        return System.currentTimeMillis() * 1000;
    }

    /**
     * Return wall clock time, in microseconds since Unix Epoch (1/1/1970 UTC midnight). This
     * functions guarantees time always moves forward, but it does not guarantee it does so in fixed
     * intervals.
     *
     * @return
     */
    public static long getNowMicrosUtc() {
        long now = System.currentTimeMillis() * 1000;
        long time = previousTimeValue.getAndIncrement();

        // Only set time if current time is greater than our stored time.
        if (now > time) {
            // This CAS can fail; getAndIncrement() ensures no value is returned twice.
            previousTimeValue.compareAndSet(time + 1, now);
            return previousTimeValue.getAndIncrement();
        } else if (time - now > timeDriftThresholdMicros) {
            throw new IllegalStateException("Time drift is " + (time - now));
        }

        return time;
    }

    /**
     * Infrastructure use only, do *not* use outside tests.
     * Set the upper bound between wall clock time as reported by {@link System#currentTimeMillis()}
     * and the time reported by {@link #getNowMicrosUtc()} (when both converted to micros).
     * The current time value will be reset to latest wall clock time so this call must be avoided
     * at all costs in a production system (it might make {@link #getNowMicrosUtc()} return a
     * smaller value than previous calls
     */
    public static void setTimeDriftThreshold(long micros) {
        timeDriftThresholdMicros = micros;
        previousTimeValue.set(TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
    }

    /**
     * Resets comparison value from default or global property
     */
    public static void resetTimeComparisonEpsilonMicros() {
        timeComparisonEpsilon = initializeTimeEpsilon();
    }

    /**
     * Sets the time interval, in microseconds, for replicated document time comparisons.
     */
    public static void setTimeComparisonEpsilonMicros(long micros) {
        timeComparisonEpsilon = micros;
    }

    /**
     * Gets the time comparison interval, or epsilon.
     * See {@link #setTimeComparisonEpsilonMicros}
     * @return
     */
    public static long getTimeComparisonEpsilonMicros() {
        return timeComparisonEpsilon;
    }

    /**
    * Compares a time value with current time. Both time values are in micros since epoch.
    * Since we can not assume the time came from the same node, we use the concept of a
    * time epsilon: any two time values within epsilon are considered too close to
    * globally order in respect to each other and this method will return true.
    */
    public static boolean isWithinTimeComparisonEpsilon(long timeMicros) {
        return Math.abs(timeMicros - Utils.getSystemNowMicrosUtc()) < timeComparisonEpsilon;
    }

    /**
     * Return a non-null, zero-length thread-local instance.
     * @return
     */
    public static StringBuilder getBuilder() {
        return builderPerThread.get();
    }

    /**
     * Adds/removes elements from specified collections; if both are specified elements are removed
     * before new elements added
     *
     * @param currentState currentState of the service
     * @param patchBody request of processing collections
     * @return {@code true} if the currentState has changed as a result of the call
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    public static <T extends ServiceDocument> boolean updateCollections(T currentState,
            ServiceStateCollectionUpdateRequest patchBody)
            throws NoSuchFieldException, IllegalAccessException {
        boolean hasChanged = false;
        if (patchBody.itemsToRemove != null) {
            for (Entry<String, Collection<Object>> collectionItem :
                    patchBody.itemsToRemove.entrySet()) {
                hasChanged |= processCollection(collectionItem.getValue(), collectionItem.getKey(),
                        currentState, CollectionOperation.REMOVE);
            }
        }
        if (patchBody.itemsToAdd != null) {
            for (Entry<String, Collection<Object>> collectionItem :
                    patchBody.itemsToAdd.entrySet()) {
                hasChanged |= processCollection(collectionItem.getValue(), collectionItem.getKey(),
                        currentState, CollectionOperation.ADD);
            }
        }
        return hasChanged;
    }

    /**
     * Adds/removes elements from specified maps; if both are specified elements are removed
     * before new elements added
     *
     * @param currentState currentState of the service
     * @param patchBody request of processing maps
     * @return {@code true} if the currentState has changed as a result of the call
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    public static <T extends ServiceDocument> boolean updateMaps(T currentState,
            ServiceStateMapUpdateRequest patchBody)
            throws NoSuchFieldException, IllegalAccessException {
        boolean hasChanged = false;
        if (patchBody.keysToRemove != null) {
            for (Entry<String, Collection<Object>> mapItem : patchBody.keysToRemove.entrySet()) {
                hasChanged |= processMapKeyRemoval(mapItem.getValue(), mapItem.getKey(),
                        currentState);
            }
        }
        if (patchBody.entriesToAdd != null) {
            for (Entry<String, Map<Object, Object>> mapItem : patchBody.entriesToAdd.entrySet()) {
                hasChanged |= processMapEntryAddition(mapItem.getValue(), mapItem.getKey(),
                        currentState);
            }
        }
        return hasChanged;
    }

    private enum CollectionOperation {
        ADD, REMOVE
    }

    @SuppressWarnings("unchecked")
    private static <T extends ServiceDocument> boolean processCollection(
            Collection<Object> inputCollection, String collectionName,
            T currentState, CollectionOperation operation)
            throws NoSuchFieldException, IllegalAccessException {
        boolean hasChanged = false;
        if (inputCollection != null && !inputCollection.isEmpty()) {
            Class<? extends ServiceDocument> clazz = currentState.getClass();
            Field field = clazz.getField(collectionName);
            if (field != null && Collection.class.isAssignableFrom(field.getType())) {
                @SuppressWarnings("rawtypes")
                Collection collObj = (Collection) field.get(currentState);
                switch (operation) {
                case ADD:
                    if (collObj == null) {
                        hasChanged = ReflectionUtils.setOrInstantiateCollectionField(currentState,
                                field, inputCollection);
                    } else {
                        hasChanged = collObj.addAll(inputCollection);
                    }
                    break;
                case REMOVE:
                    if (collObj != null) {
                        hasChanged = collObj.removeAll(inputCollection);
                    }
                    break;
                default:
                    break;
                }
            }
        }
        return hasChanged;
    }

    private static <T extends ServiceDocument> boolean processMapKeyRemoval(
            Collection<Object> keysToRemove, String mapName, T currentState)
            throws NoSuchFieldException, IllegalAccessException {
        boolean hasChanged = false;
        if (keysToRemove != null && !keysToRemove.isEmpty()) {
            Class<? extends ServiceDocument> clazz = currentState.getClass();
            Field field = clazz.getField(mapName);
            if (field != null && Map.class.isAssignableFrom(field.getType())) {
                @SuppressWarnings("rawtypes")
                Map mapObj = (Map) field.get(currentState);
                if (mapObj != null) {
                    for (Object key : keysToRemove) {
                        hasChanged |= mapObj.remove(key) != null;
                    }
                }
            }
        }
        return hasChanged;
    }

    private static <T extends ServiceDocument> boolean processMapEntryAddition(
            Map<Object, Object> entriesToAdd, String mapName, T currentState)
            throws NoSuchFieldException, IllegalAccessException {
        boolean hasChanged = false;
        if (entriesToAdd != null && !entriesToAdd.isEmpty()) {
            Class<? extends ServiceDocument> clazz = currentState.getClass();
            Field field = clazz.getField(mapName);
            if (field != null && Map.class.isAssignableFrom(field.getType())) {
                @SuppressWarnings("rawtypes")
                Map mapObj = (Map) field.get(currentState);
                if (mapObj == null) {
                    field.set(currentState, entriesToAdd);
                    hasChanged = true;
                } else {
                    for (Entry<Object, Object> entry : entriesToAdd.entrySet()) {
                        @SuppressWarnings("unchecked")
                        Object oldValue = mapObj.put(entry.getKey(), entry.getValue());
                        if (oldValue == null) {
                            hasChanged |= entry.getValue() != null;
                        } else {
                            hasChanged |= !oldValue.equals(entry.getValue());
                        }
                    }
                }
            }
        }
        return hasChanged;
    }

    /**
     * Generate a v1 UUID: Use the supplied id as the identifier for the
     * location (in space), and the value from {@link Utils#getNowMicrosUtc()} as the
     * point in time. As long as the location id (for example, the local host ID) is
     * unique within the node group, the UUID should be unique within the node group as
     * well.
     */
    public static String buildUUID(String id) {
        return Utils.getBuilder()
                .append(id)
                .append(Long.toHexString(Utils.getNowMicrosUtc()))
                .toString();
    }

    /**
     * Construct common data in {@link ServiceConfiguration}.
     */
    public static <T extends ServiceConfiguration> T buildServiceConfig(T config, Service service) {
        ServiceDocumentDescription desc = service.getHost().buildDocumentDescription(service);

        config.options = service.getOptions();
        config.maintenanceIntervalMicros = service.getMaintenanceIntervalMicros();
        config.versionRetentionLimit = desc.versionRetentionLimit;
        config.versionRetentionFloor = desc.versionRetentionFloor;
        config.peerNodeSelectorPath = service.getPeerNodeSelectorPath();
        config.documentIndexPath = service.getDocumentIndexPath();

        return config;
    }
}
