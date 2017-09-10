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

package com.vmware.xenon.services.samples;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Simple stateful service used by {@link WordCountingSampleService} and {@link LocalWordCountingSampleService}.<br/>
 * The service is designed to store and retrieve documents with no additional logic unless the
 * document id is equal to {@link MockDocumentsService#ERROR_ID}. In that case the get operation
 * intentionally fails with RuntimeException so we can demonstrate error handling in the services.
 */
public class MockDocumentsService extends StatefulService {
    public static final String FACTORY_LINK = ServiceUriPaths.SAMPLES + "/documents";

    public static final String ERROR_ID = "error";

    public static Service createFactory() {
        return FactoryService.create(MockDocumentsService.class);
    }

    public static class Document extends ServiceDocument {
        public String contents;
    }

    public MockDocumentsService() {
        super(Document.class);
    }

    public static String contentsMapper(Operation operation) {
        return operation.getBody(Document.class).contents;
    }

    @Override
    public void handleGet(Operation get) {
        if (ERROR_ID.equals(this.getSelfId())) {
            get.fail(new RuntimeException("something went wrong!"));
            return;
        }
        super.handleGet(get);
    }
}
