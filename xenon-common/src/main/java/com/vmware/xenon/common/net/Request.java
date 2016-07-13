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

import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulResponse;

/**
 * Xenon request.
 */
public interface Request extends HttpRequest {

    AuthorizationContext getAuthorizationContext();

    String getTransactionId();

    boolean isWithinTransaction();

    String getContextId();

    long getId();

    /**
     * Checks if a directive is present. Lower case strings must be used.
     */
    boolean hasPragmaDirective(String directive);

    Request addPragmaDirective(String directive);

    /**
     * Removes a directive. Lower case strings must be used
     */
    Request removePragmaDirective(String directive);

    /**
     * Infrastructure use only. ???
     *
     * Value indicating whether this operation was created to apply locally a remote update
     */
    boolean isFromReplication();

    boolean isNotification();

    boolean isRemote();

    /**
     * Infrastructure use only.
     *
     * Value indicating whether this operation is sharing a connection
     */
    boolean isConnectionSharing();

    /**
     * Infrastructure use only.
     */
    Request setConnectionSharing(boolean enable);

    /**
     * Creates a default response to this request.
     * @return
     */
    Response createResponse();

    <T extends ServiceDocument> StatefulResponse<T> createResponse(T state);
}
