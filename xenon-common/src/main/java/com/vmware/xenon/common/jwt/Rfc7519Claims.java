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

package com.vmware.xenon.common.jwt;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.gson.annotations.SerializedName;

/**
 * JSON Web Token (JWT) Rfc7519Claims.
 *
 * The fields in this class are only accessible through getter functions.
 * This guarantees that it is not possible to tamper with the set of claims after they
 * have been deserialized from a signed cookie, without replacing the object itself.
 *
 * See: https://tools.ietf.org/html/rfc7519
 */
public class Rfc7519Claims {
    /**
     * Registered claim names (section 4.1).
     * All claims are optional.
     */
    @SerializedName("iss")
    private String issuer;
    @SerializedName("sub")
    private String subject;
    @SerializedName("aud")
    private Set<String> audience;
    @SerializedName("exp")
    private Long expirationTime;
    @SerializedName("nbf")
    private Long notBefore;
    @SerializedName("iat")
    private Long issuedAt;
    @SerializedName("jti")
    private String jwtId;

    public String getIssuer() {
        return this.issuer;
    }

    public String getSubject() {
        return this.subject;
    }

    public Set<String> getAudience() {
        return Collections.unmodifiableSet(this.audience);
    }

    public Long getExpirationTime() {
        return this.expirationTime;
    }

    public Long getNotBefore() {
        return this.notBefore;
    }

    public Long getIssuedAt() {
        return this.issuedAt;
    }

    public String getJwtId() {
        return this.jwtId;
    }

    /**
     * Rfc7519Builder for the {@link Rfc7519Claims} object.
     *
     * Allows any code to construct a {@link Rfc7519Claims} object that is immutable after construction.
     *
     * {@see Rfc7519Claims}
     */
    public static class Rfc7519Builder<T extends Rfc7519Claims> {
        private Class<T> clazz;
        private Rfc7519Claims claims;

        public Rfc7519Builder(Class<T> clazz) {
            this.clazz = clazz;
            initialize();
        }

        /**
         * Initializes builder with fresh instance of {@link Rfc7519Claims} object to create.
         *
         * The new instance MUST be created in this class instead of being passed in externally,
         * or it would be possible to modify an existing {@link Rfc7519Claims} instance, which violates
         * its immutability requirement.
         */
        protected void initialize() {
            this.claims = null;

            try {
                this.claims = this.clazz.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                // Throw runtime exception; this is a user error, not guarding against it.
                throw new RuntimeException(e);
            }
        }

        /**
         * Returns temporary {@link Rfc7519Claims} object.
         *
         * @return claims object
         */
        @SuppressWarnings(value = "unchecked")
        protected T getInstance() {
            return (T) this.claims;
        }

        /**
         * Returns constructed {@link Rfc7519Claims} object.
         *
         * Reinitializes builder so that future changes are not reflected in the returned object.
         *
         * @return constructed claims object.
         */
        public T getResult() {
            T result = getInstance();
            initialize();
            return result;
        }

        public Rfc7519Builder<T> setIssuer(String issuer) {
            this.claims.issuer = issuer;
            return this;
        }

        public Rfc7519Builder<T> setSubject(String subject) {
            this.claims.subject = subject;
            return this;
        }

        public Rfc7519Builder<T> setAudience(Set<String> audience) {
            // Make copy to make sure changed made by the caller don't propagate.
            this.claims.audience = new HashSet<>(audience);
            return this;
        }

        public Rfc7519Builder<T> setExpirationTime(Long expirationTime) {
            this.claims.expirationTime = expirationTime;
            return this;
        }

        public Rfc7519Builder<T> setNotBefore(Long notBefore) {
            this.claims.notBefore = notBefore;
            return this;
        }

        public Rfc7519Builder<T> setIssuedAt(Long issuedAt) {
            this.claims.issuedAt = issuedAt;
            return this;
        }

        public Rfc7519Builder<T> setJwtId(String jwtId) {
            this.claims.jwtId = jwtId;
            return this;
        }
    }
}
