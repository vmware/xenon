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

import java.util.EnumSet;

import com.vmware.xenon.common.Service.ServiceOption;

/**
 * Configuration update request body. Allows a client to dynamically update service instance
 * configuration (including a sub set of the capabilities) through the /config utility service
 */
public class ServiceConfigUpdateRequest {
    public static final String KIND = Utils.buildKind(ServiceConfigUpdateRequest.class);

    public static ServiceConfigUpdateRequest create() {
        ServiceConfigUpdateRequest r = new ServiceConfigUpdateRequest();
        r.kind = KIND;
        return r;
    }

    public EnumSet<ServiceOption> addOptions;
    public EnumSet<ServiceOption> removeOptions;
    public Integer operationQueueLimit;
    public Long epoch;
    public Long maintenanceIntervalMicros;
    public String kind;

}