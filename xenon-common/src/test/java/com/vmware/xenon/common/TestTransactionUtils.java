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

package com.vmware.xenon.common;

import java.util.UUID;

import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.TransactionFactoryService;
import com.vmware.xenon.services.common.TransactionService;
import com.vmware.xenon.services.common.TransactionService.ResolutionRequest;
import com.vmware.xenon.services.common.TransactionService.TransactionServiceState;

public class TestTransactionUtils {

    private TestTransactionUtils() {
    }

    /**
     * Create a new transaction with a random transaction id
     */
    public static String newTransaction(VerificationHost host) throws Throwable {
        String txid = UUID.randomUUID().toString();

        TestContext ctx = host.testCreate(1);
        TransactionServiceState initialState = new TransactionServiceState();
        initialState.documentSelfLink = txid;
        initialState.options = new TransactionService.Options();
        initialState.options.allowErrorsCauseAbort = false;
        Operation post = Operation
                .createPost(UriUtils.buildUri(host, TransactionFactoryService.class))
                .setBody(initialState).setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }
                    ctx.completeIteration();
                });
        host.send(post);
        host.testWait(ctx);

        return txid;
    }

    /**
     * Commit a transaction with the specified id
     */
    public static boolean commit(VerificationHost host, String txid) throws Throwable {
        TestContext ctx = host.testCreate(1);
        boolean[] succeeded = new boolean[1];
        Operation commit = createCommitOperation(host, txid)
                .setCompletion((o, e) -> {
                    succeeded[0] = e == null;
                    ctx.completeIteration();
                });
        host.send(commit);
        host.testWait(ctx);

        return succeeded[0];
    }

    /**
     * Create a commit operation
     */
    public static Operation createCommitOperation(VerificationHost host, String txid) {
        ResolutionRequest body = new ResolutionRequest();
        body.resolutionKind = TransactionService.ResolutionKind.COMMIT;
        return Operation
                .createPatch(UriUtils.buildTransactionResolutionUri(host, txid))
                .setBody(body);
    }

    /**
     * Abort a transaction
     */
    public static boolean abort(VerificationHost host, String txid) throws Throwable {
        TestContext ctx = host.testCreate(1);
        boolean[] succeeded = new boolean[1];
        Operation abort = createAbortOperation(host, txid)
                .setCompletion((o, e) -> {
                    succeeded[0] = e == null;
                    ctx.completeIteration();
                });
        host.send(abort);
        host.testWait(ctx);

        return succeeded[0];
    }

    /**
     * Create an abort operation
     */
    public static Operation createAbortOperation(VerificationHost host, String txid) {
        ResolutionRequest body = new ResolutionRequest();
        body.resolutionKind = TransactionService.ResolutionKind.ABORT;
        return Operation
                .createPatch(UriUtils.buildTransactionResolutionUri(host, txid))
                .setBody(body);
    }

}
