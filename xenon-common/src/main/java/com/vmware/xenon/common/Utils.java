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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceDocumentDescription.TypeName;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.SystemHostInfo.OsFamily;
import com.vmware.xenon.common.logging.StackAwareLogRecord;
import com.vmware.xenon.common.serialization.BufferThreadLocal;
import com.vmware.xenon.common.serialization.JsonMapper;
import com.vmware.xenon.common.serialization.KryoSerializers.KryoForDocumentThreadLocal;
import com.vmware.xenon.common.serialization.KryoSerializers.KryoForObjectThreadLocal;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Runtime utility functions
 */
public class Utils {
    private static final String CHARSET_UTF_8 = "UTF-8";
    public static final String PROPERTY_NAME_PREFIX = "xenon.";
    public static final String CHARSET = CHARSET_UTF_8;
    public static final String UI_DIRECTORY_NAME = "ui";

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

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
            .availableProcessors() + (DEFAULT_IO_THREAD_COUNT * 2));

    /**
     * {@link #isReachableByPing} launches a separate ping process to ascertain whether a given IP
     * address is reachable within a specified timeout. This constant extends the timeout for that
     * check to account for the start-up overhead of that process.
     */
    private static final long PING_LAUNCH_TOLERANCE_MS = 50;

    private static final KryoForObjectThreadLocal kryoForObjectPerThread = new KryoForObjectThreadLocal();
    private static final KryoForDocumentThreadLocal kryoForDocumentPerThread = new KryoForDocumentThreadLocal();
    private static final BufferThreadLocal bufferPerThread = new BufferThreadLocal();

    private static final JsonMapper JSON = new JsonMapper();
    private static final ConcurrentMap<Class<?>, JsonMapper> CUSTOM_JSON = new ConcurrentHashMap<>();

    private static final Map<String, String> KINDS = new ConcurrentSkipListMap<>();

    private static final StringBuilderThreadLocal builderPerThread = new StringBuilderThreadLocal();

    private static JsonMapper getJsonMapperFor(Type type) {
        if (type instanceof Class) {
            return getJsonMapperFor((Class<?>) type);
        } else if (type instanceof ParameterizedType) {
            Type rawType = ((ParameterizedType) type).getRawType();
            return getJsonMapperFor(rawType);
        } else {
            return JSON;
        }
    }

    private static JsonMapper getJsonMapperFor(Object instance) {
        if (instance == null) {
            return JSON;
        }
        return getJsonMapperFor(instance.getClass());
    }

    private static JsonMapper getJsonMapperFor(Class<?> type) {
        if (type.isArray() && type != byte[].class) {
            type = type.getComponentType();
        }
        return CUSTOM_JSON.getOrDefault(type, JSON);
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
        CUSTOM_JSON.putIfAbsent(clazz, mapper);
    }

    public static <T> T clone(T t) {
        Kryo k = kryoForDocumentPerThread.get();
        T clone = k.copy(t);
        return clone;
    }

    public static <T> T cloneObject(T t) {
        Kryo k = kryoForObjectPerThread.get();
        T clone = k.copy(t);
        k.reset();
        return clone;
    }

    public static String computeSignature(ServiceDocument s,
            ServiceDocumentDescription description) {
        if (description == null) {
            throw new IllegalArgumentException("description is required");
        }

        byte[] buffer = getBuffer(description.serializedStateSizeLimit);
        int position = 0;

        for (PropertyDescription pd : description.propertyDescriptions.values()) {
            if (pd.indexingOptions != null) {
                if (pd.indexingOptions.contains(PropertyIndexingOption.EXCLUDE_FROM_SIGNATURE)) {
                    continue;
                }
            }

            Object fieldValue = ReflectionUtils.getPropertyValue(pd, s);
            if (pd.typeName == TypeName.COLLECTION || pd.typeName == TypeName.MAP
                    || pd.typeName == TypeName.PODO) {
                String content = Utils.toJson(fieldValue);
                position = Utils.toBytes(content, buffer, position);
            } else if (fieldValue != null) {
                position = Utils.toBytes(fieldValue, buffer, position);
            }
        }

        return computeHash(buffer, 0, position);
    }

    public static byte[] getBuffer(int capacity) {
        byte[] buffer = bufferPerThread.get();
        if (buffer.length < capacity) {
            buffer = new byte[capacity];
            bufferPerThread.set(buffer);
        }

        if (buffer.length > capacity * 10) {
            buffer = new byte[capacity];
            bufferPerThread.set(buffer);
        }
        return buffer;
    }

    /**
     * Serializes an arbitrary object into a binary representation, using full
     * reference tracking and the object graph serializer.
     * Must be paired with {@code Utils#fromBytes(byte[], int, int)} or {@code Utils#fromBytes(byte[])}
     */
    public static int toBytes(Object o, byte[] buffer, int position) {
        Kryo k = kryoForObjectPerThread.get();
        Output out = new Output(buffer);
        out.setPosition(position);
        k.writeClassAndObject(out, o);
        return out.position();
    }

    /**
     * Serializes a PODO into a binary representation. The object instance should
     * not contain circular references.
     * Must be paired with {@code Utils#fromDocumentBytes(byte[], int, int)}
     */
    public static int toDocumentBytes(Object o, byte[] buffer, int position) {
        Kryo k = kryoForDocumentPerThread.get();
        Output out = new Output(buffer);
        out.setPosition(position);
        k.writeClassAndObject(out, o);
        return out.position();
    }

    /**
     * @see Utils#toDocumentBytes(Object, byte[], int)
     */
    public static int toBytes(ServiceDocument o, byte[] buffer, int position) {
        Kryo k = kryoForDocumentPerThread.get();
        Output out = new Output(buffer);
        out.setPosition(position);
        k.writeClassAndObject(out, o);
        return out.position();
    }

    /**
     * Deserializes into a native object, using the object graph serializer.
     * Must be paired with {@code Utils#toBytes(Object, byte[], int)}
     */
    public static Object fromBytes(byte[] bytes) {
        return fromBytes(bytes, 0, bytes.length);
    }

    /**
     * Deserializes into a native object, using the object graph serializer.
     * Must be paired with {@code Utils#toBytes(Object, byte[], int)}
     */
    public static Object fromBytes(byte[] bytes, int offset, int length) {
        Kryo k = kryoForObjectPerThread.get();
        Input in = new Input(bytes, offset, length);
        return k.readClassAndObject(in);
    }

    /**
     * Deserializes into a native ServiceDocument derived type, using the document serializer.
     * Must be paired with {@code Utils#toBytes(ServiceDocument, byte[], int)
     */
    public static Object fromDocumentBytes(byte[] bytes, int offset, int length) {
        Kryo k = kryoForDocumentPerThread.get();
        Input in = new Input(bytes, offset, length);
        return k.readClassAndObject(in);
    }

    public static void performMaintenance() {

    }

    public static String computeHash(String content) {
        byte[] source = content.getBytes(Charset.forName(CHARSET_UTF_8));
        return computeHash(source, 0, source.length);
    }

    private static String computeHash(byte[] content, int offset, int length) {
        return Integer.toHexString(MurmurHash3.murmurhash3_x86_32(content, offset, length, 0));
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
     */
    public static String toJson(boolean hideSensitiveFields, boolean useHtmlFormatting, Object body)
            throws IllegalArgumentException {
        if (body instanceof String) {
            if (hideSensitiveFields) {
                throw new IllegalArgumentException(
                        "Body is already a string, sensitive fields cannot be discovered");
            }
            return (String) body;
        }
        StringBuilder content = getBuilder();
        JsonMapper mapper = getJsonMapperFor(body);
        mapper.toJson(hideSensitiveFields, useHtmlFormatting, body, content);
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
        Map<String, JsonElement> runtimeMap = Utils.fromJson(json,
                new TypeToken<Map<String, JsonElement>>() {
                }.getType());
        return Utils.fromJson(runtimeMap.get(key), valueClazz);
    }

    public static <T> T getJsonMapValue(Object json, String key, Type valueType) {
        Map<String, JsonElement> runtimeMap = Utils.fromJson(json,
                new TypeToken<Map<String, JsonElement>>() {
                }.getType());
        return Utils.fromJson(runtimeMap.get(key), valueType);
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

    public static String getCurrentFileDirectory() {
        try {
            return new File(".").getCanonicalPath();
        } catch (IOException e) {
            Logger.getAnonymousLogger().warning(Utils.toString(e));
            return null;
        }
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
        if (nestingLevel == null) {
            nestingLevel = 2;
        }
        if (!lg.isLoggable(level)) {
            return;
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

    private static AtomicLong PREVIOUS_TIME_VALUE = new AtomicLong();
    private static long TIME_COMPARISON_EPSILON_MICROS = initializeTimeEpsilon();
    public static final String PROPERTY_NAME_TIME_COMPARISON = "timeComparisonEpsilonMicros";

    private static long initializeTimeEpsilon() {
        Long l = Long.getLong(Utils.PROPERTY_NAME_PREFIX + PROPERTY_NAME_TIME_COMPARISON,
                ServiceHostState.DEFAULT_OPERATION_TIMEOUT_MICROS);
        return l;
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
        long time = PREVIOUS_TIME_VALUE.getAndIncrement();

        // Only set time if current time is greater than our stored time.
        if (now > time) {
            // This CAS can fail; getAndIncrement() ensures no value is returned twice.
            PREVIOUS_TIME_VALUE.compareAndSet(time + 1, now);
            return PREVIOUS_TIME_VALUE.getAndIncrement();
        }

        return time;
    }

    public static String toDocumentKind(Class<?> type) {
        String name = type.getCanonicalName();
        String kind = name.replace(".", ":");
        return kind;
    }

    /**
     * Registers mapping between a type and document kind string the runtime
     * will use for all services with that state type
     */
    public static String registerKind(Class<?> type, String kind) {
        return KINDS.put(type.getCanonicalName(), kind);
    }

    /**
     * Builds a kind string from a type. It uses a cache to lookup the type to kind
     * mapping. The mapping can be overridden with {@code Utils#registerKind(Class, String)}
     */
    public static String buildKind(Class<?> type) {
        String kind = KINDS.computeIfAbsent(type.getCanonicalName(), (name) -> {
            return toDocumentKind(type);
        });
        return kind;
    }

    public static ServiceErrorResponse toServiceErrorResponse(Throwable e) {
        return ServiceErrorResponse.create(e, Operation.STATUS_CODE_BAD_REQUEST);
    }

    public static String toServiceErrorResponseJson(Throwable e) {
        return Utils.toJson(toServiceErrorResponse(e));
    }

    public static ServiceErrorResponse toValidationErrorResponse(Throwable t) {
        ServiceErrorResponse rsp = new ServiceErrorResponse();
        rsp.message = t.getLocalizedMessage();
        return rsp;
    }

    public static boolean isValidationError(Throwable e) {
        return e instanceof IllegalArgumentException;
    }

    public static String toHexString(byte[] data) {
        //http://stackoverflow.com/a/9855338
        char[] sb = new char[data.length * 2];
        for (int i = 0; i < data.length; i++) {
            int v = data[i] & 0xFF;
            sb[2 * i] = HEX_CHARS[v >>> 4];
            sb[2 * i + 1] = HEX_CHARS[v & 0x0F];
        }

        return new String(sb);
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
        EnumSet<ServiceOption> reqs = null;
        EnumSet<ServiceOption> antiReqs = null;
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
            antiReqs = EnumSet.of(ServiceOption.ON_DEMAND_LOAD);
            break;
        case PERSISTENCE:
            break;
        case REPLICATION:
            break;
        case DOCUMENT_OWNER:
            break;
        case IDEMPOTENT_POST:
            break;
        case FACTORY:
            break;
        case FACTORY_ITEM:
            break;
        case HTML_USER_INTERFACE:
            break;
        case INSTRUMENTATION:
            break;
        case LIFO_QUEUE:
            break;
        case NONE:
            break;
        case UTILITY:
            break;
        case ON_DEMAND_LOAD:
            if (!options.contains(ServiceOption.FACTORY)) {
                reqs = EnumSet.of(ServiceOption.PERSISTENCE);
            }
            antiReqs = EnumSet.of(ServiceOption.PERIODIC_MAINTENANCE);
            break;
        case TRANSACTION_PENDING:
            break;
        default:
            break;
        }

        if (!options.contains(option)) {
            return null;
        }

        if (reqs == null && antiReqs == null) {
            return null;
        }

        if (reqs != null) {
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
                host.log(Level.WARNING, error);
                post.fail(new IllegalArgumentException(error));
                return false;
            }
        }

        if (service.getMaintenanceIntervalMicros() > 0 &&
                service.getMaintenanceIntervalMicros() < host.getMaintenanceIntervalMicros()) {
            host.log(
                    Level.WARNING,
                    "Service maint. interval %d is less than host interval %d, reducing host interval",
                    service.getMaintenanceIntervalMicros(), host.getMaintenanceIntervalMicros());
            host.setMaintenanceIntervalMicros(service.getMaintenanceIntervalMicros());
        }
        return true;
    }

    public static String getOsName(SystemHostInfo systemInfo) {
        return systemInfo.properties.get(SystemHostInfo.PROPERTY_NAME_OS_NAME);
    }

    public static OsFamily determineOsFamily(String osName) {
        osName = osName == null ? "" : osName.toLowerCase(Locale.ENGLISH);
        if (osName.contains("mac")) {
            return OsFamily.MACOS;
        } else if (osName.contains("win")) {
            return OsFamily.WINDOWS;
        } else if (osName.contains("nux")) {
            return OsFamily.LINUX;
        } else {
            return OsFamily.OTHER;
        }
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
     * e.g. {@code fe80:0:0:0:5971:14f6:c8ac:9e8f%34}. This method {@link #determineOsFamily detects if
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
                        Operation.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM);
                source.linkSerializedState(encodedBody);
            } catch (Throwable e2) {
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

    public static byte[] encodeBody(Operation op) throws Throwable {
        return encodeBody(op, op.getBodyRaw(), op.getContentType());
    }

    public static byte[] encodeBody(Operation op, Object body, String contentType)
            throws Throwable {
        byte[] data = null;

        if (body == null) {
            op.setContentLength(0);
            return null;
        }

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
            int limit = ServiceClient.MAX_BINARY_SERIALIZED_BODY_LIMIT;
            if (op.getContentLength() < 512) {
                op.setContentLength(512);
            }
            while (op.getContentLength() <= limit) {
                try {
                    data = new byte[(int) op.getContentLength()];
                    int count = Utils.toDocumentBytes(body, data, 0);
                    op.setContentLength(count);
                    break;
                } catch (KryoException e) {
                    op.setContentLength(op.getContentLength() * 2);
                }
            }
        }

        if (data == null) {
            String encodedBody;
            if (op.getAction() == Action.GET) {
                encodedBody = Utils.toJsonHtml(body);
            } else {
                encodedBody = Utils.toJson(body);
                if (contentType == null) {
                    op.setContentType(Operation.MEDIA_TYPE_APPLICATION_JSON);
                }
            }
            data = encodedBody.getBytes(Utils.CHARSET);
            op.setContentLength(data.length);
        }

        return data;
    }

    public static void decodeBody(Operation op, ByteBuffer buffer) {
        if (op.getContentLength() == 0) {
            op.setContentType(Operation.MEDIA_TYPE_APPLICATION_JSON).complete();
            return;
        }

        try {
            if (Operation.CONTENT_ENCODING_GZIP.equals(op
                    .getResponseHeader(Operation.CONTENT_ENCODING_HEADER))) {
                buffer = decompressGZip(buffer);
                op.getResponseHeaders().remove(Operation.CONTENT_ENCODING_HEADER);
            }

            String contentType = op.getContentType();
            Object body = decodeIfText(buffer, contentType);
            if (body != null) {
                op.setBodyNoCloning(body).complete();
                return;
            }

            // unrecognized or binary body, use the raw bytes
            byte[] data = new byte[(int) op.getContentLength()];
            buffer.get(data);
            if (Operation.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM.equals(contentType)) {
                body = Utils.fromDocumentBytes(data, 0, data.length);
                if (op.isFromReplication()) {
                    // optimization to avoid having to serialize state again, during indexing
                    op.linkSerializedState(data);
                }
            } else {
                body = data;
            }
            op.setBodyNoCloning(body).complete();
        } catch (Throwable e) {
            op.fail(e);
        }
    }

    public static String decodeIfText(ByteBuffer buffer, String contentType)
            throws CharacterCodingException {
        String body = null;
        if (contentType == null) {
            return null;
        }

        if (isContentTypeText(contentType)) {
            body = Charset.forName(Utils.CHARSET).newDecoder().decode(buffer).toString();
        } else if (contentType.contains(Operation.MEDIA_TYPE_APPLICATION_X_WWW_FORM_ENCODED)) {
            body = Charset.forName(Utils.CHARSET).newDecoder().decode(buffer).toString();
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
     * Atomically returns a map element for the specifying key. A new instance of value is created if it is
     * missing. This may be efficiently used for creating map of maps/sets.
     * <p/>
     * This method is thread-safe.
     *
     * @param map  Map to take value from.
     * @param key  Key to use.
     * @param ctor Value constructor. This constructor may be invoked multiple times for the same value, but
     *             only one value is returned.
     * @param <K>  Map key type.
     * @param <V>  Map value type.
     * @return new or existing element value.
     */
    public static <K, V> V atomicGetOrCreate(ConcurrentMap<K, V> map, K key, Callable<V> ctor) {
        V value = map.get(key);
        if (value == null) {
            try {
                value = ctor.call();
            } catch (Exception e) {
                throw new RuntimeException("Element constructor should now throw an exception", e);
            }
            V existing = map.putIfAbsent(key, value);
            if (existing != null) {
                return existing;
            }
        }
        return value;
    }

    /**
     * Merges {@code patch} object into the {@code source} object by replacing or updating all {@code source}
     *  fields with non-null {@code patch} fields. Only fields with specified merge policy are merged.
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
                    if ((prop.typeName == TypeName.COLLECTION && !o.getClass().isArray())
                            || prop.typeName == TypeName.MAP) {
                        ReflectionUtils.setOrUpdatePropertyValue(prop, source, o);
                        modified = true;
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
                                prop.accessor.getName() + " is required.");
                    }
                }
            }
        }
    }

    /**
     * Resets comparison value from default or global property
     */
    public static void resetTimeComparisonEpsilonMicros() {
        TIME_COMPARISON_EPSILON_MICROS = initializeTimeEpsilon();
    }

    /**
     * Sets the time interval, in microseconds, for replicated document time comparisons.
     */
    public static void setTimeComparisonEpsilonMicros(long micros) {
        TIME_COMPARISON_EPSILON_MICROS = micros;
    }

    /**
     * Gets the time comparison interval, or epsilon.
     * See {@link #setTimeComparisonEpsilonMicros}
     * @return
     */
    public static long getTimeComparisonEpsilonMicros() {
        return TIME_COMPARISON_EPSILON_MICROS;
    }

    /**
    * Compares a time value with current time. Both time values are in micros since epoch.
    * Since we can not assume the time came from the same node, we use the concept of a
    * time epsilon: any two time values within epsilon are considered too close to
    * globally order in respect to each other and this method will return true.
    */
    public static boolean isWithinTimeComparisonEpsilon(long timeMicros) {
        long now = Utils.getNowMicrosUtc();
        return Math.abs(timeMicros - now) < TIME_COMPARISON_EPSILON_MICROS;
    }

    public static StringBuilder getBuilder() {
        return builderPerThread.get();

    }
}
