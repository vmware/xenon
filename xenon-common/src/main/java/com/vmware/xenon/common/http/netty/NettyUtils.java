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

package com.vmware.xenon.common.http.netty;

import io.netty.handler.ssl.OpenSsl;

import com.vmware.xenon.common.Utils;

public class NettyUtils {

    private static final String ENABLE_ALPN_PROPERTY_NAME = Utils.PROPERTY_NAME_PREFIX + "enableAlpn";

    private static Boolean enableAlpn = null;

    public static boolean isAlpnEnabled() {
        if (enableAlpn == null) {
            String property = System.getProperty(ENABLE_ALPN_PROPERTY_NAME);
            enableAlpn = (property != null) ? Boolean.parseBoolean(property)
                    : OpenSsl.isAlpnSupported();
        }
        return enableAlpn;
    }

    private NettyUtils() {
    }
}
