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

import java.net.URI;
import java.util.Map;

/**
 * Document describing the <service>/subscriptions REST API
 */
public class ServiceSubscriptionState extends ServiceDocument {
    public static class ServiceSubscriber extends ServiceDocument {
        public static final long NOTIFICATION_FAILURE_LIMIT = 5;

        public static ServiceSubscriber create(boolean replayState) {
            ServiceSubscriber r = new ServiceSubscriber();
            r.replayState = replayState;
            return r;
        }

        public URI reference;
        public Long notificationLimit;
        public boolean replayState;
        public boolean usePublicUri;

        public Long notificationCount;
        public Long initialFailedNotificationTimeMicros;
        public Long failedNotificationCount;

        public ServiceSubscriber setExpiration(long expirationTimeMicros) {
            this.documentExpirationTimeMicros = expirationTimeMicros;
            return this;
        }

        public ServiceSubscriber setReplayState(boolean replayState) {
            this.replayState = replayState;
            return this;
        }

        public ServiceSubscriber setNotificationLimit(long limit) {
            this.notificationLimit = limit;
            return this;
        }

        public ServiceSubscriber setUsePublicUri(boolean usePublicUri) {
            this.usePublicUri = usePublicUri;
            return this;
        }

        public ServiceSubscriber setSubscriberReference(URI reference) {
            this.reference = reference;
            return this;
        }
    }

    public Map<URI, ServiceSubscriber> subscribers;
}
