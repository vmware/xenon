/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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

package com.vmware.dcp.common;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import com.vmware.dcp.common.Service.Action;
import com.vmware.dcp.common.Service.ServiceOption;
import com.vmware.dcp.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.dcp.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.dcp.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.dcp.common.ServiceDocumentDescription.TypeName;
import com.vmware.dcp.common.SystemHostInfo.OsFamily;
import com.vmware.dcp.common.serialization.BufferThreadLocal;
import com.vmware.dcp.common.serialization.JsonMapper;
import com.vmware.dcp.common.serialization.KryoSerializers.KryoForDocumentThreadLocal;
import com.vmware.dcp.common.serialization.KryoSerializers.KryoForObjectThreadLocal;
import com.vmware.dcp.services.common.ServiceUriPaths;

class DigestThreadLocal extends ThreadLocal<MessageDigest> {
    @Override
    protected MessageDigest initialValue() {
        return Utils.createDigest();
    }
}

/**
 * Runtime utility functions
 */
public class Utils {
    private static final int BUFFER_INITIAL_CAPACITY = 1 * 1024;
    private static final String CHARSET_UTF_8 = "UTF-8";
    public static final String CHARSET = CHARSET_UTF_8;
    public static final String UI_DIRECTORY_NAME = "ui";

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
    private static final String HASH_NAME_SHA_1 = "SHA-1";
    public static final String DEFAULT_CONTENT_HASH = HASH_NAME_SHA_1;

    public static final int DEFAULT_THREAD_COUNT = Math.max(4, Runtime.getRuntime()
            .availableProcessors());

    /**
     * {@link #isReachableByPing} launches a separate ping process to ascertain whether a given IP
     * address is reachable within a specified timeout. This constant extends the timeout for that
     * check to account for the start-up overhead of that process.
     */
    private static final long PING_LAUNCH_TOLERANCE_MS = 50;

    private static final KryoForObjectThreadLocal kryoForObjectPerThread = new KryoForObjectThreadLocal();
    private static final KryoForDocumentThreadLocal kryoForDocumentPerThread = new KryoForDocumentThreadLocal();
    private static final DigestThreadLocal digestPerThread = new DigestThreadLocal();
    private static final BufferThreadLocal bufferPerThread = new BufferThreadLocal();

    private static final JsonMapper JSON = new JsonMapper();
    private static final ConcurrentMap<Class<?>, JsonMapper> CUSTOM_JSON = new ConcurrentHashMap<>();

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
                    || pd.typeName == TypeName.PODO || pd.typeName == TypeName.ARRAY
                    || pd.typeName == TypeName.BYTES) {
                position = Utils.toBytes(fieldValue, buffer, position);
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

    public static int toBytes(Object o, byte[] buffer, int position) {
        Kryo k = kryoForObjectPerThread.get();
        Output out = new Output(buffer);
        out.setPosition(position);
        k.writeClassAndObject(out, o);
        return out.position();
    }

    public static int toBytes(ServiceDocument o, byte[] buffer, int position) {
        Kryo k = kryoForDocumentPerThread.get();
        Output out = new Output(buffer);
        out.setPosition(position);
        k.writeClassAndObject(out, o);
        return out.position();
    }

    public static Object fromBytes(byte[] bytes) {
        return fromBytes(bytes, 0, bytes.length);
    }

    public static Object fromBytes(byte[] bytes, int offset, int length) {
        Kryo k = kryoForObjectPerThread.get();
        Input in = new Input(bytes, offset, length);
        return k.readClassAndObject(in);
    }

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
        MessageDigest digest = digestPerThread.get();
        digest.update(content, offset, length);
        byte[] hash = digest.digest();
        return Utils.toHexString(hash);
    }

    private static void appendJson(Object obj, Appendable buf) {
        JsonMapper mapper = getJsonMapperFor(obj);
        mapper.toJson(obj, buf);
    }

    public static String toJson(Object body) {
        if (body instanceof String) {
            return (String) body;
        }
        StringBuilder content = new StringBuilder(BUFFER_INITIAL_CAPACITY);
        appendJson(body, content);
        return content.toString();
    }

    public static String toJsonHtml(Object body) {
        return getJsonMapperFor(body).toJsonHtml(body);
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
        try (PrintWriter printer = new PrintWriter(writer);) {
            t.printStackTrace(printer);
        }

        return writer.toString();
    }

    public static String toString(Map<?, Throwable> exceptions) {
        StringWriter writer = new StringWriter();
        try (PrintWriter printer = new PrintWriter(writer);) {
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
        log(lg, 3, classOrUri, level, fmt, args);
    }

    public static void log(Logger lg, Integer nestingLevel, String classOrUri, Level level,
            String fmt,
            Object... args) {
        if (nestingLevel == null) {
            nestingLevel = 2;
        }
        Level l = lg.getLevel();
        if (l == null) {
            l = lg.getParent().getLevel();
        }
        if (l.intValue() > level.intValue()) {
            return;
        }

        LogRecord lr = new LogRecord(level, String.format(fmt, args));
        Exception e = new Exception();
        StackTraceElement[] stack = e.getStackTrace();
        if (stack.length > nestingLevel) {
            lr.setSourceMethodName(stack[nestingLevel].getMethodName());
        }
        lr.setSourceClassName(classOrUri);
        lg.log(lr);
    }

    public static void logWarning(String fmt, Object... args) {
        Logger.getAnonymousLogger().warning(String.format(fmt, args));
    }

    private static AtomicLong prevTime = new AtomicLong();
    private static long timeComparisonEpsilonMicros = TimeUnit.SECONDS.toMicros(120);

    /**
     * Return wall clock time, in microseconds since Unix Epoch (1/1/1970 UTC midnight). This
     * functions guarantees time always moves forward, but it does not guarantee it does so in fixed
     * intervals.
     *
     * @return
     */
    public static long getNowMicrosUtc() {
        long now = System.currentTimeMillis() * 1000;
        long time = prevTime.getAndIncrement();

        // Only set time if current time is greater than our stored time.
        if (now > time) {
            // This CAS can fail; getAndIncrement() ensures no value is returned twice.
            prevTime.compareAndSet(time + 1, now);
            return prevTime.getAndIncrement();
        }

        return time;
    }

    public static String buildKind(Class<?> type) {
        return type.getCanonicalName().replace(".", ":");
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
     * For example: the class "com.vmware.dcp.services.common.ExampleService" is converted to
     * "com/vmware/dcp/services/common/ExampleService".
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
        JsonObject jo = null;
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

    public static void setTimeComparisonEpsilonMicros(long micros) {
        timeComparisonEpsilonMicros = micros;
    }

    public static long getTimeComparisonEpsilonMicros() {
        return timeComparisonEpsilonMicros;
    }

    public static String validateServiceOption(EnumSet<ServiceOption> options, ServiceOption option) {
        EnumSet<ServiceOption> reqs = null;
        EnumSet<ServiceOption> antiReqs = null;
        switch (option) {
        case CONCURRENT_UPDATE_HANDLING:
            antiReqs = EnumSet.of(ServiceOption.EAGER_CONSISTENCY, ServiceOption.OWNER_SELECTION,
                    ServiceOption.STRICT_UPDATE_CHECKING);
            break;
        case EAGER_CONSISTENCY:
            reqs = EnumSet.of(ServiceOption.REPLICATION, ServiceOption.OWNER_SELECTION);
            antiReqs = EnumSet.of(ServiceOption.CONCURRENT_UPDATE_HANDLING);
            break;
        case OWNER_SELECTION:
            reqs = EnumSet.of(ServiceOption.REPLICATION);
            antiReqs = EnumSet.of(ServiceOption.CONCURRENT_UPDATE_HANDLING);
            break;
        case STRICT_UPDATE_CHECKING:
            antiReqs = EnumSet.of(ServiceOption.CONCURRENT_UPDATE_HANDLING);
            break;
        case PERIODIC_MAINTENANCE:
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

        if (antiReqs != null) {
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
        }

        return null;
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
     * e.g. {@code fe80:0:0:0:5971:14f6:c8ac:9e8f%34}. This method {@link #isWindowsHost detects if
     * the caller is a Windows host} and will adjust the host address accordingly.
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

    public static byte[] encodeBody(Operation op) throws Throwable {
        byte[] data = null;
        String contentType = op.getContentType();

        if (!op.hasBody()) {
            op.setContentLength(0);
            return null;
        }

        Object body = op.getBodyRaw();
        if (body instanceof String) {
            data = ((String) body).getBytes(Utils.CHARSET);
        } else if (body instanceof byte[]) {
            data = (byte[]) body;
            if (contentType == null) {
                op.setContentType(Operation.MEDIA_TYPE_APPLICATION_OCTET_STREAM);
            }
        }

        if (data == null) {
            if (contentType == null || contentType.contains(Operation.MEDIA_TYPE_APPLICATION_JSON)) {
                String encodedBody = null;
                if (op.getAction() == Action.GET) {
                    encodedBody = Utils.toJsonHtml(body);
                } else {
                    encodedBody = Utils.toJson(body);
                    if (contentType == null) {
                        op.setContentType(Operation.MEDIA_TYPE_APPLICATION_JSON);
                    }
                }
                data = encodedBody.getBytes(Utils.CHARSET);
            } else {
                throw new IllegalArgumentException("Unrecognized content type: " + contentType);
            }
        }

        op.setContentLength(data.length);
        return data;
    }

    public static void decodeBody(Operation op, ByteBuffer buffer) {
        Object body = null;

        if (op.getContentLength() == 0) {
            op.setContentType(Operation.MEDIA_TYPE_APPLICATION_JSON).complete();
            return;
        }

        try {
            String contentType = op.getContentType();
            body = decodeIfText(buffer, contentType);
            if (body == null) {
                // unrecognized or binary body, use the raw bytes
                byte[] data = new byte[(int) op.getContentLength()];
                buffer.get(data);
                body = data;
            }
            op.setBodyNoCloning(body).complete();
        } catch (Throwable e) {
            op.fail(e);
        }
    }

    public static MessageDigest createDigest() {
        try {
            return MessageDigest.getInstance(DEFAULT_CONTENT_HASH);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static String decodeIfText(ByteBuffer buffer, String contentType)
            throws CharacterCodingException {
        String body = null;
        if (contentType == null) {
            return null;
        }

        if (contentType.contains(Operation.MEDIA_TYPE_APPLICATION_JSON)
                || contentType.contains("text")
                || contentType.contains("css")
                || contentType.contains("script")
                || contentType.contains("html")
                || contentType.contains("xml")) {
            body = Charset.forName(Utils.CHARSET).newDecoder().decode(buffer).toString();
        }

        return body;
    }

    /**
     * Compute ui resource path for this service.
     * <p>
     * If service has defined the custom path on ServiceDocumentDescription
     * userInterfaceResourcePath field that will be used  else default UI path
     * will be calculated using service path Eg. for ExampleService
     * default path will be ui/com/vmware/dcp/services/common/ExampleService
     *
     * @param type service class for which UI path has to be extracted
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
     * Merges {@code patch} object into the {@code source} object by replacing all {@code source} fields with non-null
     * {@code patch} fields. Only fields with specified merge policy are merged.
     *
     * @param desc Service document description.
     * @param source Source object.
     * @param patch  Patch object.
     * @param <T>    Object type.
     * @return {@code true} in case there was at least one update. Updates of fields to same values are not considered
     *      as updates).
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
                    if (!o.equals(ReflectionUtils.getPropertyValue(prop, source))) {
                        ReflectionUtils.setPropertyValue(prop, source, o);
                        modified = true;
                    }
                }
            }
        }
        return modified;
    }
}
