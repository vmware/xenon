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

package com.vmware.xenon.services.common;

import com.vmware.xenon.common.Utils;


public class SynchronizationRequest {
    public static final String KIND = Utils.buildKind(SynchronizationRequest.class);

    public static SynchronizationRequest create() {
        SynchronizationRequest r = new SynchronizationRequest();
        r.kind = KIND;
        return r;
    }

    public String kind;

    /**
     * SelfLink of factory or stateful child-service to be synchronized.
     */
    public String documentSelfLink;
}
