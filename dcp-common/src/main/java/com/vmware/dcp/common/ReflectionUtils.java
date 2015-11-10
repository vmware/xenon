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

package com.vmware.dcp.common;

import java.lang.reflect.Constructor;

import com.vmware.dcp.common.ServiceDocumentDescription.PropertyDescription;

public class ReflectionUtils {
    public static <T> T instantiate(Class<T> clazz) {
        try {
            Constructor<T> ctor = clazz.getDeclaredConstructor();
            if (!ctor.isAccessible()) {
                ctor.setAccessible(true);
            }
            return ctor.newInstance();
        } catch (Throwable e) {
            Utils.logWarning("Reflection error: %s", Utils.toString(e));
        }
        return null;
    }

    public static Object getPropertyValue(PropertyDescription pd, Object instance) {
        try {
            return pd.accessor.get(instance);
        } catch (Throwable e) {
            Utils.logWarning("Reflection error: %s", Utils.toString(e));
        }
        return null;
    }

    public static void setPropertyValue(PropertyDescription pd, Object instance, Object value) {
        try {
            pd.accessor.set(instance, value);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
