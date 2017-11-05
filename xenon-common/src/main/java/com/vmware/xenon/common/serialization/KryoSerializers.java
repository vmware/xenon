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

package com.vmware.xenon.common.serialization;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.esotericsoftware.kryo.serializers.VersionFieldSerializer;
import org.objenesis.strategy.StdInstantiatorStrategy;

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.config.XenonConfiguration;

public final class KryoSerializers {
    private static boolean KRYO_HANDLE_BUILTIN_COLLECTIONS = XenonConfiguration.bool(
            "kryo",
            "handleBuiltInCollections",
            true
    );


    /**
     * Binary serialization thread local instances that track object references
     */
    public static class KryoForObjectThreadLocal extends ThreadLocal<Kryo> {
        @Override
        protected Kryo initialValue() {
            return KryoSerializers.create(true);
        }
    }

    /**
     * Binary serialization thread local instances that do not track object references, used
     * for document and operation body serialization
     */
    public static class KryoForDocumentThreadLocal extends ThreadLocal<Kryo> {
        @Override
        protected Kryo initialValue() {
            return KryoSerializers.create(false);
        }
    }

    private KryoSerializers() {
    }

    private static final ThreadLocal<Kryo> kryoForObjectPerThread = new KryoForObjectThreadLocal();
    private static final ThreadLocal<Kryo> kryoForDocumentPerThread = new KryoForDocumentThreadLocal();
    private static ThreadLocal<Kryo> kryoForObjectPerThreadCustom;
    private static ThreadLocal<Kryo> kryoForDocumentPerThreadCustom;

    public static final long THREAD_LOCAL_BUFFER_LIMIT_BYTES = 1024 * 1024;
    private static final int DEFAULT_BUFFER_SIZE_BYTES = 4096;
    private static final BufferThreadLocal bufferPerThread = new BufferThreadLocal();

    public static Kryo create(boolean isObjectSerializer) {
        Kryo k = new Kryo();
        // handle classes with missing default constructors
        k.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        // supports addition of fields if the @since annotation is used
        k.setDefaultSerializer(VersionFieldSerializer.class);
        // Custom serializers for Java 8 date/time
        k.addDefaultSerializer(ZonedDateTime.class, ZonedDateTimeSerializer.INSTANCE);
        k.addDefaultSerializer(Instant.class, InstantSerializer.INSTANCE);
        k.addDefaultSerializer(ZoneId.class, ZoneIdSerializer.INSTANCE);
        // Add non-cloning serializers for all immutable types bellow
        k.addDefaultSerializer(UUID.class, UUIDSerializer.INSTANCE);
        k.addDefaultSerializer(URI.class, URISerializer.INSTANCE);

        k.addDefaultSerializer(ByteBuffer.class, ByteBufferSerializer.INSTANCE);

        if (KRYO_HANDLE_BUILTIN_COLLECTIONS) {
            configureJdkCollections(k);
        }

        if (!isObjectSerializer) {
            // For performance reasons, and to avoid memory use, assume documents do not
            // require object graph serialization with duplicate or recursive references
            k.setReferences(false);
            k.setCopyReferences(false);
            // documentSerialized must nullify certain fields.
            k.setDefaultSerializer(FieldNullifyingVersionFieldSerializer.class);
        } else {
            // To avoid monotonic increase of memory use, due to reference tracking, we must
            // reset after each use.
            k.setAutoReset(true);
        }
        return k;
    }

    private static void configureJdkCollections(Kryo kryo) {
        // write singleton as arraylists of size 1
        CollectionSerializer emptyOrSingletonSerializer = new CollectionSerializer() {
            @SuppressWarnings("rawtypes")
            @Override
            protected Collection<?> create(Kryo kryo, Input input, Class<Collection> type) {
                return newCollection(type);
            }

            @SuppressWarnings("rawtypes")
            private Collection<?> newCollection(Class<Collection> origType) {
                if (NavigableSet.class.isAssignableFrom(origType)) {
                    return new TreeSet<>();
                } else if (Set.class.isAssignableFrom(origType)) {
                    return new HashSet<>();
                } else {
                    return new ArrayList<>(1);
                }
            }

            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            protected Collection<?> createCopy(Kryo kryo, Collection original) {
                return newCollection((Class<Collection>) original.getClass());
            }
        };

        MapSerializer emptyOrSingletonMapSerializer = new MapSerializer(){
            @SuppressWarnings("rawtypes")
            @Override
            protected Map<?, ?> create(Kryo kryo, Input input, Class<Map> type) {
                return newMap(type);
            }

            @SuppressWarnings("rawtypes")
            private Map<?, ?> newMap(Class<Map> origType) {
                if (NavigableMap.class.isAssignableFrom(origType)) {
                    return new TreeMap<>();
                } else {
                    return new HashMap<>();
                }
            }

            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            protected Map<?, ?> createCopy(Kryo kryo, Map original) {
                return newMap((Class<Map>) original.getClass());
            }
        };

        // empty collections
        kryo.addDefaultSerializer(Collections.EMPTY_LIST.getClass(), emptyOrSingletonSerializer);
        kryo.addDefaultSerializer(Collections.EMPTY_SET.getClass(), emptyOrSingletonSerializer);
        kryo.addDefaultSerializer(Collections.emptyNavigableSet().getClass(), emptyOrSingletonSerializer);

        // empty maps
        kryo.addDefaultSerializer(Collections.EMPTY_MAP.getClass(), emptyOrSingletonMapSerializer);
        kryo.addDefaultSerializer(Collections.emptyNavigableMap().getClass(), emptyOrSingletonMapSerializer);

        // singletons
        kryo.addDefaultSerializer(Arrays.asList().getClass(), emptyOrSingletonSerializer);
        kryo.addDefaultSerializer(Collections.singleton(kryo).getClass(), emptyOrSingletonSerializer);
        kryo.addDefaultSerializer(Collections.singletonList(kryo).getClass(), emptyOrSingletonSerializer);
        kryo.addDefaultSerializer(Collections.singletonMap("", kryo).getClass(), emptyOrSingletonMapSerializer);
    }

    public static void register(ThreadLocal<Kryo> kryoThreadLocal, boolean isDocumentSerializer) {
        if (isDocumentSerializer) {
            kryoForDocumentPerThreadCustom = kryoThreadLocal;
        } else {
            kryoForObjectPerThreadCustom = kryoThreadLocal;
        }
    }

    private static Kryo getKryoThreadLocalForDocuments() {
        ThreadLocal<Kryo> tl = kryoForDocumentPerThreadCustom;
        if (tl == null) {
            tl = kryoForDocumentPerThread;
        }
        return tl.get();
    }

    private static Kryo getKryoThreadLocalForObjects() {
        ThreadLocal<Kryo> tl = kryoForObjectPerThreadCustom;
        if (tl == null) {
            tl = kryoForObjectPerThread;
        }
        return tl.get();
    }

    /**
     * Serializes a {@code ServiceDocument} into a binary representation.
     * The document should not contain circular references.
     * Must be paired with {@code KryoSerializers#deserializeDocument(byte[], int, int)}
     */
    public static Output serializeDocument(ServiceDocument o, int maxSize) {
        return serializeAsDocument(o, maxSize);
    }

    /**
     * See {@link #serializeDocument(ServiceDocument, byte[], int)}
     */
    public static Output serializeAsDocument(Object o, int maxSize) {
        Kryo k = getKryoThreadLocalForDocuments();
        Output out = new Output(getBuffer(DEFAULT_BUFFER_SIZE_BYTES), maxSize);
        k.writeClassAndObject(out, o);
        return out;
    }

    /**
     * Infrastructure use only. Uses custom serialization that will write nulls for select built-in
     * fields that can be reconstructed from other index data
     */
    public static Output serializeDocumentForIndexing(Object o, int maxSize) {
        Kryo k = getKryoThreadLocalForDocuments();
        byte[] buffer = getBuffer(DEFAULT_BUFFER_SIZE_BYTES);
        OutputWithRoot out = new OutputWithRoot(buffer, maxSize, o);
        k.writeClassAndObject(out, o);
        return out;
    }
    /**
     * Serializes an arbitrary object into a binary representation, using full
     * reference tracking and the object graph serializer.
     * Must be paired with {@code KryoSerializers#fromBytes(byte[], int, int)} or
     * {@code KryoSerializers#fromBytes(byte[])}
     */
    public static int serializeObject(Object o, byte[] buffer, int position) {
        Kryo k = getKryoThreadLocalForObjects();
        Output out = new Output(buffer, buffer.length);
        out.setPosition(position);
        k.writeClassAndObject(out, o);
        return out.position();
    }

    /**
     * @see #serializeObject(Object, byte[], int)
     */
    public static ByteBuffer serializeObject(Object o, int maxSize) {
        Kryo k = getKryoThreadLocalForObjects();
        Output out = new Output(DEFAULT_BUFFER_SIZE_BYTES, maxSize);
        k.writeClassAndObject(out, o);
        return ByteBuffer.wrap(out.getBuffer(), 0, out.position());
    }

    public static <T> T clone(T t) {
        Kryo k = getKryoThreadLocalForDocuments();
        return k.copy(t);
    }

    public static <T> T cloneObject(T t) {
        Kryo k = getKryoThreadLocalForObjects();
        T clone = k.copy(t);
        k.reset();
        return clone;
    }

    public static byte[] getBuffer(int capacity) {
        if (capacity > THREAD_LOCAL_BUFFER_LIMIT_BYTES) {
            // do not cache larger buffers
            return new byte[capacity];
        }
        byte[] buffer = bufferPerThread.get();
        if (buffer.length < capacity || buffer.length > capacity * 10) {
            buffer = new byte[capacity];
            bufferPerThread.set(buffer);
        }
        return buffer;
    }

    /**
     * @see #deserializeObject(byte[], int, int)
     */
    public static Object deserializeObject(ByteBuffer bb) {
        return deserializeObject(bb.array(), bb.position(), bb.limit());
    }

    /**
     * Deserializes into a native object, using the object graph serializer.
     * Must be paired with {@link #serializeObject(Object, byte[], int)}
     */
    public static Object deserializeObject(byte[] bytes, int position, int length) {
        Kryo k = getKryoThreadLocalForObjects();
        Input in = new Input(bytes, position, length);
        return k.readClassAndObject(in);
    }

    /**
     * Deserializes into a native ServiceDocument derived type, using the document serializer.
     * Must be paired with {@link #serializeDocument(ServiceDocument, byte[], int)}
     */
    public static Object deserializeDocument(byte[] bytes, int position, int length) {
        Kryo k = getKryoThreadLocalForDocuments();
        Input in = new Input(bytes, position, length);
        return k.readClassAndObject(in);
    }

    /**
     * Deserializes ByteBuffer into a native ServiceDocument derived type, using the document
     * serializer.
     * @see #deserializeDocument(byte[], int, int)
     */
    public static Object deserializeDocument(ByteBuffer bb) {
        return deserializeDocument(bb.array(), bb.position(), bb.limit());
    }
}
