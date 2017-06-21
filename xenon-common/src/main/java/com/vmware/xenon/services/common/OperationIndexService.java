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

import java.util.concurrent.TimeUnit;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.SerializedOperation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.Utils;

public class OperationIndexService extends LuceneDocumentIndexService {

    public static final String SELF_LINK = ServiceUriPaths.CORE_OPERATION_INDEX;

    public static final String FILE_PATH = "lucene-operation-index";

    public static final long DEFAULT_EXPIRATION_INTERVAL_MICROS = TimeUnit.DAYS.toMicros(1);

    public OperationIndexService() {
        super(FILE_PATH);
    }

    @Override
    public void handleRequest(Operation op) {

        Action action = op.getAction();

        if (action == Action.DELETE) {
            // by pass base class queuing and allow delete directly
            try {
                super.handleDeleteImpl(op);
            } catch (Exception e) {
                op.fail(e);
            }
            return;
        }

        if (!op.hasBody() || action != Action.POST) {

            // do not accept backup and restore request
            if (action == Action.PATCH) {
                ServiceDocument sd = (ServiceDocument) op.getBodyRaw();
                if (sd.documentKind != null &&
                        (sd.documentKind.equals(BackupRequest.KIND) || sd.documentKind.equals(RestoreRequest.KIND))) {
                    op.fail(new IllegalStateException("backup and restore request are not supported"));
                    return;
                }
            }

            super.handleRequest(op);
            return;
        }

        SerializedOperation state = op.getBody(SerializedOperation.class);

        if (state.documentKind != null
                && !state.documentKind.equals(SerializedOperation.KIND)) {
            op.fail(new IllegalArgumentException("documentKind is not SerializedOperation"));
            return;
        }

        state.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(
                DEFAULT_EXPIRATION_INTERVAL_MICROS);

        state.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        if (state.documentSelfLink == null) {
            state.documentSelfLink = state.documentUpdateTimeMicros + "";
        }

        state.documentUpdateAction = action.toString();

        UpdateIndexRequest req = new UpdateIndexRequest();
        req.description = SerializedOperation.DESCRIPTION;
        req.document = state;

        op.setBodyNoCloning(req);
        super.handleRequest(op);
    }

}
