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

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationProcessingChain;
import com.vmware.xenon.common.RequestRouter;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.xenon.services.common.SimpleTransactionService.SimpleTransactionServiceState;
import com.vmware.xenon.services.common.SimpleTransactionService.TransactionalRequestFilter;
import com.vmware.xenon.services.common.TestSimpleTransactionServiceMultiHost.BankAccountService.BankAccountServiceRequest;
import com.vmware.xenon.services.common.TestSimpleTransactionServiceMultiHost.BankAccountService.BankAccountServiceState;

public class TestSimpleTransactionServiceMultiHost {

    static final int RETRIES_IN_CASE_OF_CONFLICTS = 5;
    // upper bound on random sleep between retries
    static final int SLEEP_BETWEEN_RETRIES_MILLIS = 2000;

    /**
     * Command line argument specifying default number of accounts
     */
    public int accountCount = 20;

    /**
     * Command line argument specifying default number of in process service hosts
     */
    public int nodeCount = 3;

    private long baseAccountId;

    /**
     * Default entry node for test operations
     */
    private VerificationHost host;

    /**
     * Host that is used to set up the node group
     */
    private VerificationHost nodeGroupSetupHost;

    private String buildAccountId(int i) {
        return this.baseAccountId + "-" + String.valueOf(i);
    }

    @Before
    public void setUp() throws Exception {
        try {
            this.baseAccountId = Utils.getNowMicrosUtc();
            this.nodeGroupSetupHost = VerificationHost.create(0);
            this.nodeGroupSetupHost.start();

            this.nodeGroupSetupHost.setUpPeerHosts(this.nodeCount);
            this.nodeGroupSetupHost.joinNodesAndVerifyConvergence(this.nodeCount);
            for (VerificationHost h : this.nodeGroupSetupHost.getInProcessHostMap().values()) {
                setUpHostWithAdditionalServices(h);
            }

            this.host = this.nodeGroupSetupHost.getPeerHost();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() {
        this.nodeGroupSetupHost.tearDownInProcessPeers();
        this.nodeGroupSetupHost.tearDown();
    }

    private void setUpHostWithAdditionalServices(VerificationHost h) throws Throwable {
        h.setTransactionService(null);
        if (h.getServiceStage(SimpleTransactionFactoryService.SELF_LINK) == null) {
            h.startServiceAndWait(SimpleTransactionFactoryService.class,
                    SimpleTransactionFactoryService.SELF_LINK);
            h.startServiceAndWait(BankAccountFactoryService.class,
                    BankAccountFactoryService.SELF_LINK);
        }
    }

    @Test
    public void testBasicCRUD() throws Throwable {
        // create ACCOUNT accounts in a single transaction, commit, query and verify count
        String txid = newTransaction();
        createAccounts(txid, this.accountCount);
        commit(txid);
        countAccounts(null, this.accountCount);

        // deposit 100 in each account in a single transaction, commit and verify balances
        txid = newTransaction();
        depositToAccounts(txid, this.accountCount, 100.0);
        commit(txid);

        for (int i = 0; i < this.accountCount; i++) {
            verifyAccountBalance(null, buildAccountId(i), 100.0);
        }

        // delete ACCOUNT accounts in a single transaction, commit, query and verify count == 0
        txid = newTransaction();
        deleteAccounts(txid, this.accountCount);
        commit(txid);
        countAccounts(null, 0);
    }

    private String newTransaction() throws Throwable {
        String txid = UUID.randomUUID().toString();

        // this section is required until IDEMPOTENT_POST is used
        TestContext ctx = this.host.testCreate(1);
        SimpleTransactionServiceState initialState = new SimpleTransactionServiceState();
        initialState.documentSelfLink = txid;
        Operation post = Operation
                .createPost(getTransactionFactoryUri())
                .setBody(initialState).setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }
                    ctx.completeIteration();
                });
        this.host.send(post);
        this.host.testWait(ctx);

        return txid;
    }

    private void commit(String transactionId) throws Throwable {
        TestContext ctx = this.host.testCreate(1);
        Operation patch = SimpleTransactionService.TxUtils.buildCommitRequest(this.host,
                transactionId);
        patch.setCompletion((o, e) -> {
            if (operationFailed(o, e)) {
                ctx.failIteration(e);
                return;
            }
            ctx.completeIteration();
        });
        this.host.send(patch);
        this.host.testWait(ctx);
    }

    private void createAccounts(String transactionId, int accounts) throws Throwable {
        createAccounts(transactionId, accounts, 0.0);
    }

    private void createAccounts(String transactionId, int accounts, double initialBalance) throws Throwable {
        TestContext ctx = this.host.testCreate(accounts);
        for (int i = 0; i < accounts; i++) {
            createAccount(transactionId, buildAccountId(i), initialBalance, ctx);
        }
        this.host.testWait(ctx);
    }

    private void createAccount(String transactionId, String accountId, double initialBalance, TestContext ctx)
            throws Throwable {
        boolean independentTest = ctx == null;
        if (independentTest) {
            ctx = this.host.testCreate(1);
        }
        BankAccountServiceState initialState = new BankAccountServiceState();
        initialState.documentSelfLink = accountId;
        initialState.balance = initialBalance;
        TestContext finalCtx = ctx;
        Operation post = Operation
                .createPost(getAccountFactoryUri())
                .setBody(initialState).setCompletion((o, e) -> {
                    if (operationFailed(o, e)) {
                        finalCtx.failIteration(e);
                        return;
                    }
                    finalCtx.completeIteration();
                });
        if (transactionId != null) {
            post.setTransactionId(transactionId);
        }
        this.host.send(post);
        if (independentTest) {
            this.host.testWait(ctx);
        }
    }

    private void deleteAccounts(String transactionId, int accounts) throws Throwable {
        TestContext ctx = this.host.testCreate(accounts);
        for (int i = 0; i < accounts; i++) {
            Operation delete = Operation
                    .createDelete(buildAccountUri(buildAccountId(i)))
                    .setCompletion((o, e) -> {
                        if (operationFailed(o, e)) {
                            ctx.failIteration(e);
                            return;
                        }
                        ctx.completeIteration();
                    });
            if (transactionId != null) {
                delete.setTransactionId(transactionId);
            }
            this.host.send(delete);
        }
        this.host.testWait(ctx);
    }

    private void countAccounts(String transactionId, long expected) throws Throwable {
        Query.Builder queryBuilder = Query.Builder.create().addKindFieldClause(BankAccountServiceState.class)
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK,
                        BankAccountFactoryService.SELF_LINK + UriUtils.URI_PATH_CHAR + this.baseAccountId + UriUtils.URI_WILDCARD_CHAR,
                        MatchType.WILDCARD);
        if (transactionId != null) {
            queryBuilder.addFieldClause(ServiceDocument.FIELD_NAME_TRANSACTION_ID, transactionId);
        }
        QueryTask task = QueryTask.Builder.createDirectTask().setQuery(queryBuilder.build()).addOption(QueryOption.BROADCAST).build();
        this.host.createQueryTaskService(task, false, true, task, null);
        assertEquals(expected, task.results.documentCount.longValue());
    }

    private void depositToAccounts(String transactionId, int accounts, double amountToDeposit)
            throws Throwable {
        TestContext ctx = this.host.testCreate(accounts);
        for (int i = 0; i < accounts; i++) {
            depositToAccount(transactionId, buildAccountId(i), amountToDeposit, ctx);
        }
        this.host.testWait(ctx);
    }

    private void depositToAccount(String transactionId, String accountId, double amountToDeposit,
            TestContext ctx)
            throws Throwable {
        Throwable[] ex = new Throwable[1];
        boolean independentTest = ctx == null;
        if (independentTest) {
            ctx = this.host.testCreate(1);
        }
        Operation patch = createDepositOperation(transactionId, accountId, amountToDeposit);
        TestContext finalCtx = ctx;
        patch.setCompletion((o, e) -> {
            if (operationFailed(o, e)) {
                // TODO: SimpleTransactionService should set STATUS_CODE_CONFLICT on transaction conflict
                if (o.getStatusCode() == Operation.STATUS_CODE_CONFLICT) {
                    ex[0] = e;
                    finalCtx.completeIteration();
                } else {
                    finalCtx.failIteration(e);
                }
                return;
            }
            finalCtx.completeIteration();
        });
        this.host.send(patch);
        if (independentTest) {
            this.host.testWait(ctx);
        }

        if (ex[0] != null) {
            throw ex[0];
        }
    }

    private Operation createDepositOperation(String transactionId, String accountId, double amount) {
        BankAccountServiceRequest body = new BankAccountServiceRequest();
        body.kind = BankAccountServiceRequest.Kind.DEPOSIT;
        body.amount = amount;
        Operation patch = Operation
                .createPatch(buildAccountUri(accountId))
                .setBody(body);
        if (transactionId != null) {
            patch.setTransactionId(transactionId);
        }

        return patch;
    }

    private void verifyAccountBalance(String transactionId, String accountId, double expectedBalance)
            throws Throwable {
        double balance = getAccount(transactionId, accountId).balance;
        assertEquals(expectedBalance, balance, 0);
    }

    private BankAccountServiceState getAccount(String transactionId, String accountId)
            throws Throwable {
        Throwable[] ex = new Throwable[1];
        BankAccountServiceState[] responses = new BankAccountServiceState[1];
        TestContext ctx = this.host.testCreate(1);
        Operation get = Operation
                .createGet(buildAccountUri(accountId))
                .setCompletion((o, e) -> {
                    if (operationFailed(o, e)) {
                        // TODO: SimpleTransactionService should set STATUS_CODE_CONFLICT on transaction conflict
                        if (o.getStatusCode() == Operation.STATUS_CODE_CONFLICT) {
                            ex[0] = e;
                            ctx.completeIteration();
                        } else {
                            ctx.failIteration(e);
                        }
                        return;
                    }
                    responses[0] = o.getBody(BankAccountServiceState.class);
                    ctx.completeIteration();
                });
        if (transactionId != null) {
            get.setTransactionId(transactionId);
        }
        this.host.send(get);
        this.host.testWait(ctx);

        if (ex[0] != null) {
            throw ex[0];
        }

        return responses[0];
    }

    private URI getTransactionFactoryUri() {
        return UriUtils.buildUri(this.host, SimpleTransactionFactoryService.class);
    }

    private URI getAccountFactoryUri() {
        return UriUtils.buildUri(this.host, BankAccountFactoryService.class);
    }

    private URI buildAccountUri(String accountId) {
        return UriUtils.extendUri(getAccountFactoryUri(), accountId);
    }

    private boolean operationFailed(Operation o, Throwable e) {
        return e != null || o.getStatusCode() == Operation.STATUS_CODE_CONFLICT;
    }

    public static class BankAccountFactoryService extends FactoryService {

        public static final String SELF_LINK = ServiceUriPaths.SAMPLES + "/bank-accounts";

        public BankAccountFactoryService() {
            super(BankAccountService.BankAccountServiceState.class);
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            return new BankAccountService();
        }

        @Override
        public OperationProcessingChain getOperationProcessingChain() {
            if (super.getOperationProcessingChain() != null) {
                return super.getOperationProcessingChain();
            }

            OperationProcessingChain opProcessingChain = new OperationProcessingChain(this);
            opProcessingChain.add(new TransactionalRequestFilter(this));
            setOperationProcessingChain(opProcessingChain);
            return opProcessingChain;
        }
    }

    public static class BankAccountService extends StatefulService {

        public static class BankAccountServiceState extends ServiceDocument {
            static final String KIND = Utils.buildKind(BankAccountServiceState.class);
            public double balance;
        }

        public static class BankAccountServiceRequest {
            public static enum Kind {
                DEPOSIT, WITHDRAW
            }

            public Kind kind;
            public double amount;
        }

        public BankAccountService() {
            super(BankAccountServiceState.class);
            super.toggleOption(ServiceOption.PERSISTENCE, true);
            super.toggleOption(ServiceOption.REPLICATION, true);
            super.toggleOption(ServiceOption.OWNER_SELECTION, true);
            super.toggleOption(ServiceOption.CONCURRENT_GET_HANDLING, false);
        }

        @Override
        public OperationProcessingChain getOperationProcessingChain() {
            if (super.getOperationProcessingChain() != null) {
                return super.getOperationProcessingChain();
            }

            RequestRouter myRouter = new RequestRouter();
            myRouter.register(
                    Action.PATCH,
                    new RequestRouter.RequestBodyMatcher<BankAccountServiceRequest>(
                            BankAccountServiceRequest.class, "kind",
                            BankAccountServiceRequest.Kind.DEPOSIT),
                    this::handlePatchForDeposit, "Deposit");
            myRouter.register(
                    Action.PATCH,
                    new RequestRouter.RequestBodyMatcher<BankAccountServiceRequest>(
                            BankAccountServiceRequest.class, "kind",
                            BankAccountServiceRequest.Kind.WITHDRAW),
                    this::handlePatchForWithdraw, "Withdraw");
            OperationProcessingChain opProcessingChain = new OperationProcessingChain(this);
            opProcessingChain.add(new TransactionalRequestFilter(this));
            opProcessingChain.add(myRouter);
            setOperationProcessingChain(opProcessingChain);
            return opProcessingChain;
        }

        @Override
        public void handleStart(Operation start) {
            try {
                validateState(start);
                start.complete();
            } catch (Exception e) {
                start.fail(e);
            }
        }

        void handlePatchForDeposit(Operation patch) {
            BankAccountServiceState currentState = getState(patch);
            BankAccountServiceRequest body = patch.getBody(BankAccountServiceRequest.class);

            currentState.balance += body.amount;

            setState(patch, currentState);
            patch.setBody(currentState);
            patch.complete();
        }

        void handlePatchForWithdraw(Operation patch) {
            BankAccountServiceState currentState = getState(patch);
            BankAccountServiceRequest body = patch.getBody(BankAccountServiceRequest.class);

            if (body.amount > currentState.balance) {
                patch.fail(Operation.STATUS_CODE_BAD_REQUEST);
                return;
            }
            currentState.balance -= body.amount;

            setState(patch, currentState);
            patch.setBody(currentState);
            patch.complete();
        }

        private void validateState(Operation start) {
            if (!start.hasBody()) {
                throw new IllegalArgumentException(
                        "attempt to initialize service with an empty state");
            }

            BankAccountServiceState state = start.getBody(BankAccountServiceState.class);
            if (state.balance < 0) {
                throw new IllegalArgumentException("balance cannot be negative");
            }
        }

    }

}
