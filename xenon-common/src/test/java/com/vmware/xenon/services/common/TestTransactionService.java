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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationContext;
import com.vmware.xenon.common.OperationProcessingChain;
import com.vmware.xenon.common.RequestRouter;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Occurance;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.xenon.services.common.TestTransactionService.BankAccountService.BankAccountServiceRequest;
import com.vmware.xenon.services.common.TestTransactionService.BankAccountService.BankAccountServiceState;
import com.vmware.xenon.services.common.TransactionService.ResolutionRequest;
import com.vmware.xenon.services.common.TransactionService.TransactionServiceState;

public class TestTransactionService extends BasicReusableHostTestCase {

    /**
     * Parameter that specifies the number of accounts to create
     */
    public int accountCount = 10;

    private long baseAccountId;

    @Before
    public void prepare() throws Throwable {
        this.baseAccountId = Utils.getNowMicrosUtc();
        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        this.host.waitForServiceAvailable(TransactionFactoryService.SELF_LINK);
        if (this.host.getServiceStage(BankAccountService.FACTORY_LINK) == null) {
            Service bankAccountFactory = FactoryService.create(BankAccountService.class,
                    BankAccountServiceState.class);
            this.host.startServiceAndWait(bankAccountFactory, BankAccountService.FACTORY_LINK,
                    new BankAccountServiceState());
        }
        this.host.setOperationTimeOutMicros(TimeUnit.SECONDS.toMicros(1000));
    }

    /**
     * Test only the stateless asynchronous transaction resolution service
     *
     * @throws Throwable
     */
    @Test
    public void transactionResolution() throws Throwable {
        ExampleService.ExampleServiceState verifyState;
        List<URI> exampleURIs = new ArrayList<>();
        this.host.createExampleServices(this.host, 1, exampleURIs, null);

        String txid = newTransaction();

        ExampleServiceState initialState = new ExampleServiceState();
        initialState.name = "zero";
        initialState.counter = 0L;
        updateExampleService(txid, exampleURIs.get(0), initialState);

        boolean committed = commit(txid, 1);
        assertTrue(committed);

        verifyState = this.host.getServiceState(null, ExampleServiceState.class,
                exampleURIs.get(0));
        assertEquals(initialState.name, verifyState.name);
        assertEquals(null, verifyState.documentTransactionId);
    }

    /**
     * Test a number of scenarios in the happy, single-instance transactions. Testing a single transactions allows
     * us to invoke the coordinator interface directly, without going through "resolution" interface -- eventually
     * though, even single tests should go through this interface, since the current setup causes races.
     * @throws Throwable
     */
    @Test
    public void singleUpdate() throws Throwable {
        // used to verify current state
        ExampleServiceState verifyState;
        List<URI> exampleURIs = new ArrayList<>();
        // create example service documents across all nodes
        this.host.createExampleServices(this.host, 1, exampleURIs, null);

        // 0 -- no transaction
        ExampleServiceState initialState = new ExampleServiceState();
        initialState.name = "zero";
        initialState.counter = 0L;
        updateExampleService(null, exampleURIs.get(0), initialState);
        // This should be equal to the current state -- since we did not use transactions
        verifyState = this.host.getServiceState(null, ExampleServiceState.class,
                exampleURIs.get(0));
        assertEquals(verifyState.name, initialState.name);

        // 1 -- tx1
        String txid1 = newTransaction();
        ExampleServiceState newState = new ExampleServiceState();
        newState.name = "one";
        newState.counter = 1L;
        updateExampleService(txid1, exampleURIs.get(0), newState);

        // get outside a transaction -- ideally should get old version -- for now, it should fail
        host.toggleNegativeTestMode(true);
        this.host.getServiceState(null, ExampleServiceState.class, exampleURIs.get(0));
        host.toggleNegativeTestMode(false);

        // get within a transaction -- the callback should bring latest
        verifyExampleServiceState(txid1, exampleURIs.get(0), newState);

        // now commit
        boolean committed = commit(txid1, 2);
        assertTrue(committed);
        // This should be equal to the newest state -- since the transaction committed
        verifyState = this.host.getServiceState(null, ExampleServiceState.class,
                exampleURIs.get(0));
        assertEquals(verifyState.name, newState.name);

        // 2 -- tx2
        String txid2 = newTransaction();
        ExampleServiceState abortState = new ExampleServiceState();
        abortState.name = "two";
        abortState.counter = 2L;
        updateExampleService(txid2, exampleURIs.get(0), abortState);
        // This should be equal to the latest committed state -- since the txid2 is still in-progress
        verifyState = this.host.getServiceState(null, ExampleServiceState.class,
                exampleURIs.get(0));
        assertEquals(verifyState.name, newState.name);

        // now abort
        boolean aborted = abort(txid2, 1);
        assertTrue(aborted);
        // This should be equal to the previous state -- since the transaction committed
        verifyState = this.host.getServiceState(null, ExampleServiceState.class,
                exampleURIs.get(0));
        // TODO re-enable when abort logic is debugged
        assertEquals(verifyState.name, newState.name);
    }

    @Test
    public void testBasicCRUD() throws Throwable {
        // create ACCOUNT accounts in a single transaction, commit, query and verify count
        String txid = newTransaction();
        createAccounts(txid, this.accountCount);
        boolean committed = commit(txid, this.accountCount);
        assertTrue(committed);
        countAccounts(null, this.accountCount);

        // deposit a different amount to each account in a single transaction, commit and verify balances
        txid = newTransaction();
        TestContext ctx = testCreate(this.accountCount);
        for (int i = 0; i < this.accountCount; i++) {
            depositToAccount(txid, buildAccountId(i), i, ctx);
        }
        testWait(ctx);
        committed = commit(txid, this.accountCount);
        assertTrue(committed);
        for (int i = 0; i < this.accountCount; i++) {
            verifyAccountBalance(null, buildAccountId(i), i);
        }

        // delete ACCOUNT accounts in a single transaction, commit, query and verify count == 0
        txid = newTransaction();
        deleteAccounts(txid, this.accountCount);
        committed = commit(txid, this.accountCount);
        assertTrue(committed);
        countAccounts(null, 0);
    }

    @Test
    public void testTransactionContextFlow() throws Throwable {
        // stateless service that creates a bank account
        // with the transactionId on the parent operation
        // and one without
        StatelessService childService = new StatelessService() {
            @Override
            public void handlePost(Operation postOp) {
                try {
                    createAccount(null, buildAccountId(0), 0.0, null);
                    OperationContext.setTransactionId(null);
                    createAccount(null, buildAccountId(1), 0.0, null);
                } catch (Throwable e) {
                    postOp.fail(e);
                    return;
                }
                postOp.complete();
            }
        };
        String servicePath = UUID.randomUUID().toString();
        Operation startOp = Operation.createPost(UriUtils.buildUri(this.host, servicePath));
        this.host.startService(startOp, childService);
        // create two bank accounts
        String txid = newTransaction();
        TestContext ctx = testCreate(1);
        Operation postOp = Operation.createPost(UriUtils.buildUri(this.host, servicePath))
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }
                    if (OperationContext.getTransactionId() == null) {
                        ctx.failIteration(new IllegalStateException("transactionId not set"));
                        return;
                    }
                    ctx.completeIteration();
                });
        postOp.setTransactionId(txid);
        this.host.send(postOp);
        testWait(ctx);
        // only one account should be visible at this stage within the transaction
        countAccounts(txid, 1);
        countAccounts(null, 1);
        boolean committed = commit(txid, 1);
        assertTrue(committed);
        // verify that two accounts are created (one as part of the transaction and one without)
        countAccounts(null, 2);
        this.baseAccountId = Utils.getNowMicrosUtc();
        txid = newTransaction();
        postOp = Operation.createPost(UriUtils.buildUri(this.host, servicePath));
        postOp.setTransactionId(txid);
        this.host.sendAndWaitExpectSuccess(postOp);
        // transaction is still in progress, the account just created must be visible
        countAccounts(txid, 1);
        boolean aborted = abort(txid, 1);
        assertTrue(aborted);
        // verify that the account created without a transaction context is still present
        countAccounts(null, 1);
    }

    @Test
    public void testVisibilityWithinTransaction() throws Throwable {
        String txid = newTransaction();
        for (int i = 0; i < this.accountCount; i++) {
            String accountId = buildAccountId(i);
            createAccount(txid, accountId, null);
            countAccounts(txid, i + 1);
            depositToAccount(txid, accountId, 100.0, null);
            verifyAccountBalance(txid, accountId, 100.0);
        }
        boolean aborted = abort(txid, 3 * this.accountCount);
        assertTrue(aborted);
        countAccounts(null, 0);
    }

    @Test
    public void testShortTransactions() throws Throwable {
        for (int i = 0; i < this.accountCount; i++) {
            String txid = newTransaction();
            String accountId = buildAccountId(i);
            createAccount(txid, accountId, null);
            if (i % 2 == 0) {
                depositToAccount(txid, accountId, 100.0, null);
                boolean committed = commit(txid, 2);
                assertTrue(committed);
            } else {
                boolean aborted = abort(txid, 2);
                assertTrue(aborted);
            }
        }
        countAccounts(null, this.accountCount / 2);
        sumAccounts(null, 100.0 * this.accountCount / 2);
    }

    @Test
    public void testSingleClientMultipleActiveTransactions() throws Throwable {
        String[] txids = new String[this.accountCount];

        for (int i = 0; i < this.accountCount; i++) {
            txids[i] = newTransaction();
            this.host.log("Created transaction %s", txids[i]);
            String accountId = buildAccountId(i);
            double initialBalance = i % 2 == 0 ? 100.0 : 0;
            createAccount(txids[i], accountId, initialBalance, null);
        }

        String interferrer = newTransaction();
        this.host.log("Created interferer transaction %s", interferrer);
        for (int i = 0; i < this.accountCount; i++) {
            String accountId = buildAccountId(i);
            BankAccountServiceState account = getAccount(interferrer, accountId);
            assertNull(account);
        }

        for (int i = 0; i < this.accountCount; i++) {
            String accountId = buildAccountId(i);
            BankAccountServiceState account = getAccount(txids[i], accountId);
            double expectedBalance = i % 2 == 0 ? 100.0 : 0;
            assertEquals(expectedBalance, account.balance, 0);
        }

        for (int i = 0; i < this.accountCount; i++) {
            int pendingOps = 3;
            boolean aborted = abort(txids[i], pendingOps);
            assertTrue(aborted);
        }

        boolean aborted = abort(interferrer, this.accountCount);
        assertTrue(aborted);

        countAccounts(null, 0);
    }

    @Test
    public void testSingleClientMultiDocumentTransactions() throws Throwable {
        String txid = newTransaction();
        createAccounts(txid, this.accountCount, 100.0);
        boolean committed = commit(txid, this.accountCount);
        assertTrue(committed);

        int numOfTransfers = this.accountCount / 3;
        String[] txids = newTransactions(numOfTransfers);
        Random rand = new Random();
        for (int k = 0; k < numOfTransfers; k++) {
            int i = rand.nextInt(this.accountCount);
            int j = rand.nextInt(this.accountCount);
            if (i == j) {
                j = (j + 1) % this.accountCount;
            }
            int amount = 1 + rand.nextInt(3);
            withdrawFromAccount(txids[k], buildAccountId(i), amount, null);
            depositToAccount(txids[k], buildAccountId(i), amount, null);
        }

        for (int k = 0; k < numOfTransfers; k++) {
            if (k % 5 == 0) {
                boolean aborted = abort(txids[k], 2);
                assertTrue(aborted);
            } else {
                // we don't assert here as we expect some commits to fail the race and abort.
                // the test just verifies that no funds are lost.
                commit(txids[k], 2);
            }
        }

        sumAccounts(null, 100.0 * this.accountCount);

        deleteAccounts(null, this.accountCount);
        countAccounts(null, 0);
    }

    @Test
    public void testSingleClientMultiDocumentConcurrentTransactions() throws Throwable {
        String txid = newTransaction();
        createAccounts(txid, this.accountCount, 100.0);
        boolean committed = commit(txid, this.accountCount);
        assertTrue(committed);

        int numOfTransfers = this.accountCount / 3;
        String[] txids = newTransactions(numOfTransfers);
        sendWithdrawDepositOperationPairs(txids, numOfTransfers, null);
        sumAccounts(null, 100.0 * this.accountCount);

        deleteAccounts(null, this.accountCount);
        countAccounts(null, 0);
    }

    private void sendWithdrawDepositOperationPairs(String[] txids, int numOfTransfers,
            TestContext ctx) throws Throwable {
        boolean independentTest = ctx == null;
        if (independentTest) {
            ctx = testCreate(numOfTransfers);
        }

        Collection<Operation> requests = new ArrayList<Operation>(numOfTransfers);
        Random rand = new Random();
        for (int k = 0; k < numOfTransfers; k++) {
            final String tid = txids[k];
            int i = rand.nextInt(this.accountCount);
            int j = rand.nextInt(this.accountCount);
            if (i == j) {
                j = (j + 1) % this.accountCount;
            }
            final int final_j = j;
            TestContext finalCtx = ctx;
            int amount = 1 + rand.nextInt(3);
            this.host.log("Transaction %s: Transferring $%d from %d to %d", tid, amount, i,
                    final_j);
            Operation withdraw = createWithdrawOperation(tid, buildAccountId(i), amount);
            withdraw.setCompletion((o, e) -> {
                if (e != null) {
                    this.host.log("Transaction %s: failed to withdraw, aborting...", tid);
                    Operation abort = createAbortOperation(tid, 1);
                    abort.setCompletion((op, ex) -> {
                        if (independentTest) {
                            finalCtx.completeIteration();
                        }
                    });
                    this.host.send(abort);
                    return;
                }
                Operation deposit = createDepositOperation(tid, buildAccountId(final_j), amount);
                deposit.setCompletion((op, ex) -> {
                    if (ex != null) {
                        this.host.log("Transaction %s: failed to deposit, aborting...", tid);
                        Operation abort = createAbortOperation(tid, 2);
                        abort.setCompletion((op2, ex2) -> {
                            if (independentTest) {
                                finalCtx.completeIteration();
                            }
                        });
                        this.host.send(abort);
                        return;
                    }
                    this.host.log("Transaction %s: Committing", tid);
                    Operation commit = this.createCommitOperation(tid, 2);
                    commit.setCompletion((op2, ex2) -> {
                        if (ex2 != null) {
                            this.host.log(
                                    "Transaction %s: failed to commit (probably due to a race)",
                                    tid);
                        }
                        if (independentTest) {
                            finalCtx.completeIteration();
                        }
                    });
                    this.host.send(commit);
                });
                this.host.send(deposit);
            });
            requests.add(withdraw);
        }

        for (Operation withdraw : requests) {
            this.host.send(withdraw);
        }
        if (independentTest) {
            testWait(ctx);
        }
    }

    private String[] newTransactions(int numOfTransactions) throws Throwable {
        String[] txids = new String[numOfTransactions];
        for (int k = 0; k < numOfTransactions; k++) {
            txids[k] = newTransaction();
        }

        return txids;
    }

    private String newTransaction() throws Throwable {
        String txid = UUID.randomUUID().toString();

        TestContext ctx = testCreate(1);
        TransactionServiceState initialState = new TransactionServiceState();
        initialState.documentSelfLink = txid;
        initialState.options = new TransactionService.Options();
        initialState.options.allowErrorsCauseAbort = false;
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
        testWait(ctx);

        return txid;
    }

    private boolean commit(String txid, int pendingOperations) throws Throwable {
        TestContext ctx = testCreate(1);
        boolean[] succeeded = new boolean[1];
        Operation commit = createCommitOperation(txid, pendingOperations)
                .setCompletion((o, e) -> {
                    succeeded[0] = e == null;
                    ctx.completeIteration();
                });
        this.host.send(commit);
        testWait(ctx);

        return succeeded[0];
    }

    private Operation createCommitOperation(String txid, int pendingOperations) {
        ResolutionRequest body = new ResolutionRequest();
        body.resolutionKind = TransactionService.ResolutionKind.COMMIT;
        body.pendingOperations = pendingOperations;
        return Operation
                .createPost(UriUtils.buildTransactionResolutionUri(this.host, txid))
                .setBody(body);
    }

    private boolean abort(String txid, int pendingOperations) throws Throwable {
        TestContext ctx = testCreate(1);
        boolean[] succeeded = new boolean[1];
        Operation abort = createAbortOperation(txid, pendingOperations)
                .setCompletion((o, e) -> {
                    succeeded[0] = e == null;
                    ctx.completeIteration();
                });
        this.host.send(abort);
        testWait(ctx);

        return succeeded[0];
    }

    private Operation createAbortOperation(String txid, int pendingOperations) {
        ResolutionRequest body = new ResolutionRequest();
        body.resolutionKind = TransactionService.ResolutionKind.ABORT;
        body.pendingOperations = pendingOperations;
        return Operation
                .createPost(UriUtils.buildTransactionResolutionUri(this.host, txid))
                .setBody(body);
    }

    private void updateExampleService(String txid, URI exampleServiceUri,
            ExampleServiceState exampleServiceState) throws Throwable {
        TestContext ctx = testCreate(1);
        Operation put = Operation
                .createPut(exampleServiceUri)
                .setTransactionId(txid)
                .setBody(exampleServiceState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }
                    ctx.completeIteration();
                });
        this.host.send(put);
        testWait(ctx);
    }

    private void verifyExampleServiceState(String txid, URI exampleServiceUri,
            ExampleServiceState exampleServiceState) throws Throwable {
        TestContext ctx = testCreate(1);
        Operation operation = Operation
                .createGet(exampleServiceUri)
                .setTransactionId(txid)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }

                    ExampleServiceState rsp = o.getBody(ExampleServiceState.class);
                    assertEquals(exampleServiceState.name, rsp.name);
                    ctx.completeIteration();
                });
        this.host.send(operation);
        testWait(ctx);
    }

    private void createAccounts(String transactionId, int accounts) throws Throwable {
        createAccounts(transactionId, accounts, 0.0);
    }

    private void createAccounts(String transactionId, int accounts, double initialBalance)
            throws Throwable {
        TestContext ctx = testCreate(accounts);
        for (int i = 0; i < accounts; i++) {
            createAccount(transactionId, buildAccountId(i), initialBalance, ctx);
        }
        testWait(ctx);
    }

    public void createAccount(String transactionId, String accountId, TestContext ctx)
            throws Throwable {
        createAccount(transactionId, accountId, 0.0, ctx);
    }

    private void createAccount(String transactionId, String accountId, double initialBalance,
            TestContext ctx)
            throws Throwable {
        boolean independentTest = ctx == null;
        if (independentTest) {
            ctx = testCreate(1);
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
            testWait(ctx);
        }
    }

    private void deleteAccounts(String transactionId, int accounts) throws Throwable {
        TestContext ctx = testCreate(accounts);
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
        testWait(ctx);
    }

    private void countAccounts(String transactionId, long expected) throws Throwable {
        Query.Builder queryBuilder = Query.Builder.create()
                .addKindFieldClause(BankAccountServiceState.class)
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK,
                        BankAccountService.FACTORY_LINK + UriUtils.URI_PATH_CHAR
                                + this.baseAccountId + UriUtils.URI_WILDCARD_CHAR,
                        MatchType.WILDCARD);
        if (transactionId != null) {
            queryBuilder.addFieldClause(ServiceDocument.FIELD_NAME_TRANSACTION_ID, transactionId);
        } else {
            queryBuilder.addFieldClause(ServiceDocument.FIELD_NAME_TRANSACTION_ID, "*",
                    MatchType.WILDCARD, Occurance.MUST_NOT_OCCUR);
        }
        QueryTask task = QueryTask.Builder.createDirectTask().setQuery(queryBuilder.build())
                .build();
        this.host.createQueryTaskService(task, false, true, task, null);
        if (expected != task.results.documentCount.longValue()) {
            this.host.log("Number of accounts found is different than expected:");
            for (String serviceSelfLink : task.results.documentLinks) {
                String accountId = UriUtils.getLastPathSegment(serviceSelfLink);
                this.host.log(
                        "Found account: %s, service stage: %s. Trying to access account with txid %s...",
                        accountId, this.host.getServiceStage(serviceSelfLink), transactionId);
                try {
                    BankAccountServiceState state = getAccount(transactionId, accountId);
                    if (state != null) {
                        this.host.log("Got account, documentUpdateAction=%s",
                                state.documentUpdateAction);
                    } else {
                        this.host.log("Failed to access account");
                    }
                } catch (Exception e) {
                    this.host.log("Failed to access account: %s", e);
                }
            }
        }
        assertEquals(expected, task.results.documentCount.longValue());
    }

    public void sumAccounts(String transactionId, double expected) throws Throwable {
        Query.Builder queryBuilder = Query.Builder.create()
                .addKindFieldClause(BankAccountServiceState.class)
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK,
                        BankAccountService.FACTORY_LINK + UriUtils.URI_PATH_CHAR
                                + this.baseAccountId + UriUtils.URI_WILDCARD_CHAR,
                        MatchType.WILDCARD);
        if (transactionId != null) {
            queryBuilder.addFieldClause(ServiceDocument.FIELD_NAME_TRANSACTION_ID, transactionId);
        }
        QueryTask task = QueryTask.Builder.createDirectTask().setQuery(queryBuilder.build())
                .build();
        this.host.createQueryTaskService(task, false, true, task, null);
        double sum = 0;
        for (String serviceSelfLink : task.results.documentLinks) {
            String accountId = UriUtils.getLastPathSegment(serviceSelfLink);
            BankAccountServiceState account = getAccount(transactionId, accountId);
            sum += account.balance;
        }
        assertEquals(expected, sum, 0);
    }

    private void depositToAccount(String transactionId, String accountId, double amountToDeposit,
            TestContext ctx)
            throws Throwable {
        boolean independentTest = ctx == null;
        if (independentTest) {
            ctx = testCreate(1);
        }
        Operation patch = createDepositOperation(transactionId, accountId, amountToDeposit);
        TestContext finalCtx = ctx;
        patch.setCompletion((o, e) -> {
            if (operationFailed(o, e)) {
                finalCtx.failIteration(e);
                return;
            }
            finalCtx.completeIteration();
        });
        this.host.send(patch);
        if (independentTest) {
            testWait(ctx);
        }
    }

    private Operation createDepositOperation(String transactionId, String accountId,
            double amount) {
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

    public void withdrawFromAccount(String transactionId, String accountId,
            double amountToWithdraw,
            TestContext ctx)
            throws Throwable {
        Throwable[] ex = new Throwable[1];
        boolean independentTest = ctx == null;
        if (independentTest) {
            ctx = testCreate(1);
        }
        Operation patch = createWithdrawOperation(transactionId, accountId, amountToWithdraw);
        TestContext finalCtx = ctx;
        patch.setCompletion((o, e) -> {
            if (operationFailed(o, e)) {
                if (o.getStatusCode() == Operation.STATUS_CODE_BAD_REQUEST) {
                    ex[0] = new IllegalArgumentException();
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
            testWait(ctx);
        }

        if (ex[0] != null) {
            throw ex[0];
        }
    }

    private Operation createWithdrawOperation(String transactionId, String accountId,
            double amount) {
        BankAccountServiceRequest body = new BankAccountServiceRequest();
        body.kind = BankAccountServiceRequest.Kind.WITHDRAW;
        body.amount = amount;
        Operation patch = Operation
                .createPatch(buildAccountUri(accountId))
                .setBody(body);
        if (transactionId != null) {
            patch.setTransactionId(transactionId);
        }

        return patch;
    }

    private void verifyAccountBalance(String transactionId, String accountId,
            double expectedBalance)
            throws Throwable {
        double balance = getAccount(transactionId, accountId).balance;
        assertEquals(expectedBalance, balance, 0);
    }

    private BankAccountServiceState getAccount(String transactionId, String accountId)
            throws Throwable {
        BankAccountServiceState[] responses = new BankAccountServiceState[1];
        TestContext ctx = testCreate(1);
        Operation get = Operation
                .createGet(buildAccountUri(accountId))
                .setCompletion((o, e) -> {
                    if (operationFailed(o, e)) {
                        if (o.getStatusCode() == Operation.STATUS_CODE_NOT_FOUND) {
                            responses[0] = null;
                            ctx.completeIteration();
                            return;
                        }
                        ctx.failIteration(e);
                        return;
                    }
                    responses[0] = o.getBody(BankAccountServiceState.class);
                    ctx.completeIteration();
                });
        if (transactionId != null) {
            get.setTransactionId(transactionId);
        }
        this.host.send(get);
        testWait(ctx);

        return responses[0];
    }

    private URI getTransactionFactoryUri() {
        return UriUtils.buildUri(this.host, TransactionFactoryService.class);
    }

    private URI getAccountFactoryUri() {
        return UriUtils.buildUri(this.host, BankAccountService.FACTORY_LINK);
    }

    private URI buildAccountUri(String accountId) {
        return UriUtils.extendUri(getAccountFactoryUri(), accountId);
    }

    private boolean operationFailed(Operation o, Throwable e) {
        return e != null;
    }

    private String buildAccountId(int i) {
        return this.baseAccountId + "-" + String.valueOf(i);
    }

    public static class BankAccountService extends StatefulService {

        public static final String FACTORY_LINK = ServiceUriPaths.SAMPLES + "/bank-accounts";

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
            opProcessingChain.add(myRouter);
            setOperationProcessingChain(opProcessingChain);
            return opProcessingChain;
        }

        @Override
        public void handleStart(Operation start) {
            if (!ServiceHost.isServiceCreate(start)) {
                logInfo("Starting service due to synchronization");
            }
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
