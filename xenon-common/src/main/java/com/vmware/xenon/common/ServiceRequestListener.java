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

import java.io.IOException;
import java.net.URI;

public interface ServiceRequestListener {
    public static final String PROPERTY_NAME_RESPONSE_PAYLOAD_SIZE =
            Utils.PROPERTY_NAME_PREFIX + "ServiceRequestListener.RESPONSE_PAYLOAD_SIZE_LIMIT";

    /**
     * Default maximum size limit of a response payload that can be returned by a Xenon host.
     */
    public static final int RESPONSE_PAYLOAD_SIZE_LIMIT = Integer.getInteger(
            PROPERTY_NAME_RESPONSE_PAYLOAD_SIZE, 1024 * 1024 * 64);

    long getActiveClientCount();

    int getPort();

    void setSSLContextFiles(URI certFile, URI keyFile) throws Throwable;

    void setSSLContextFiles(URI certFile, URI keyFile, String keyPassphrase) throws Throwable;

    void start(int port, String bindAddress) throws Throwable;

    void handleMaintenance(Operation op);

    void stop() throws IOException;

    boolean isSSLConfigured();

    boolean isListening();

    void setResponsePayloadSizeLimit(int responsePayloadSizeLimit);

    int getResponsePayloadSizeLimit();
}
