/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common.net;

import java.util.Map;

public interface HttpResponse extends ContentAware {
    /**
     * Preferably use {@link #getRequestHeader(String)} for retrieving entries.
     * Preferably use {@link #addRequestHeader(String, String)} for adding entries.
     */
    Map<String, String> getResponseHeaders();

    boolean hasResponseHeaders();

    String getResponseHeader(String headerName);

    HttpResponse addResponseHeader(String name, String value);
}
