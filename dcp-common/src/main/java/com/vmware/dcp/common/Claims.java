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

import java.util.Collections;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

import com.vmware.dcp.common.jwt.Rfc7519Claims;

/**
 * DCP specific JWT claims.
 *
 * Extends basic JWT claims to include DCP specific claims.
 */
public class Claims extends Rfc7519Claims {
    @SerializedName("props")
    private Map<String, String> properties;

    private static final Map<String, String> emptyProperties =
            Collections.unmodifiableMap(Collections.emptyMap());

    /**
     * Returns properties associated with {@link Claims} object.
     *
     * It returns an empty map if the properties are unset.
     *
     * @return unmodifiable property map
     */
    public Map<String, String> getProperties() {
        if (this.properties == null) {
            return emptyProperties;
        } else {
            return Collections.unmodifiableMap(this.properties);
        }
    }

    /**
     * Rfc7519Builder for the {@link Claims} object.
     *
     * {@see Rfc7519Claims}
     */
    public static class Builder extends Rfc7519Builder<Claims> {
        public Builder() {
            super(Claims.class);
        }

        public Builder setProperties(Map<String, String> properties) {
            getInstance().properties = properties;
            return this;
        }
    }
}
