/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
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

import java.util.HashSet;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * This class prevents deserialization of certain classes known to be a security risk.
 */
public class NonSerializingSerializer extends Serializer<Object> {
    private static final NonSerializingSerializer INSTANCE = new NonSerializingSerializer();

    // known vulnerable classes described here:
    // https://www.github.com/mbechler/marshalsec/blob/master/marshalsec.pdf?raw=true
    // add more jvm backdoor classes here
    private static final Set<Class<?>> BLACKLISTED = new HashSet<>();

    static {
        addBlacklisted("java.util.ServiceLoader$LazyIterator");
        addBlacklisted("com.sun.jndi.rmi.registry.BindingEnumeration");
        addBlacklisted("com.sun.jndi.toolkit.dir.LazySearchEnumerationImpl");
        addBlacklisted("javax.imageio.ImageIO$ContainsFilter");
        addBlacklisted("javax.script.ScriptEngineManager");
        addBlacklisted("org.springframework.aop.aspectj.autoproxy.AspectJAwareAdvisorAutoProxyCreator$PartiallyComparableAdvisorHolder");
        addBlacklisted("org.springframework.aop.support.AbstractBeanFactoryPointcutAdvisor");
        addBlacklisted("org.apache.xbean.naming.context.ContextUtil$ReadOnlyBinding");
        addBlacklisted("javax.naming.spi.ContinuationContext");
        addBlacklisted("org.apache.commons.beanutils.BeanComparator");
        addBlacklisted("org.codehaus.groovy.runtime.MethodClosure");
    }

    private static void addBlacklisted(String className) {
        try {
            Class<?> cls = NonSerializingSerializer.class.getClassLoader().loadClass(className);
            BLACKLISTED.add(cls);
        } catch (ClassNotFoundException ignore) {

        }
    }

    private NonSerializingSerializer() {

    }

    @Override
    public void write(Kryo kryo, Output output, Object object) {
        // do nothing
    }

    @Override
    public Object read(Kryo kryo, Input input, Class<Object> type) {
        // must throw: some collections don't support null elements
        throw new IllegalStateException("Kryo byte stream contains non-deserializable objects");
    }

    public static void apply(Kryo k) {
        for (Class<?> cls : NonSerializingSerializer.BLACKLISTED) {
            k.addDefaultSerializer(cls, INSTANCE);
        }
    }
}
