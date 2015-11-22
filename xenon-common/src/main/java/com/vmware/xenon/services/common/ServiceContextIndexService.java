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

import java.util.EnumSet;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;

public class ServiceContextIndexService extends LuceneBlobIndexService {
    public static Operation createPost(ServiceHost host, String key, Object blob) {
        return createPost(host, SELF_LINK, key, blob);
    }

    public static Operation createGet(ServiceHost host, String key) {
        return createGet(host, SELF_LINK, key);
    }

    public static final String SELF_LINK = ServiceUriPaths.CORE_SERVICE_CONTEXT_INDEX;
    public static final String FILE_PATH = "lucene-service-context-index";

    public ServiceContextIndexService() {
        super(EnumSet.of(BlobIndexOption.CREATE, BlobIndexOption.SINGLE_USE_KEYS),
                FILE_PATH);
    }
}