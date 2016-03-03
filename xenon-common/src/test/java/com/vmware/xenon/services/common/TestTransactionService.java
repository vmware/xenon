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
import static org.junit.Assert.assertTrue;


import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


import org.junit.Before;
import org.junit.Test;


import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationProcessingChain;
import com.vmware.xenon.common.RequestRouter;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.xenon.services.common.TestTransactionService.BankAccountService.BankAccountServiceRequest;
import com.vmware.xenon.services.common.TestTransactionService.BankAccountService.BankAccountServiceState;
import com.vmware.xenon.services.common.TransactionService.ResolutionRequest;
import com.vmware.xenon.services.common.TransactionService.TransactionServiceState;

public class TestTransactionService extends BasicReusableHostTestCase {

    /**
     * Parameter that specifies the number of accounts to create
     */
    public int accountCount = 20;

    private long baseAccountId;

    @Before
    public void prepare() throws Throwable {
        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        this.host.waitForServiceAvailable(TransactionFactoryService.SELF_LINK);
        if (this.host.getServiceStage(BankAccountService.FACTORY_LINK) == null) {
            Service bankAccountFactory = FactoryService.create(BankAccountService.class, BankAccountServiceState.class);
            this.host.startServiceAndWait(bankAccountFactory, BankAccountService.FACTORY_LINK, new BankAccountServiceState());
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

        verifyState = this.host.getServiceState(null, ExampleServiceState.class, exampleURIs.get(0));
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
        verifyState = this.host.getServiceState(null, ExampleServiceState.class, exampleURIs.get(0));
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
        verifyState = this.host.getServiceState(null, ExampleServiceState.class, exampleURIs.get(0));
        assertEquals(verifyState.name, newState.name);

        // 2 -- tx2
        String txid2 = newTransaction();
        ExampleServiceState abortState = new ExampleServiceState();
        abortState.name = "two";
        abortState.counter = 2L;
        updateExampleService(txid2, exampleURIs.get(0), abortState);
        // This should be equal to the latest committed state -- since the txid2 is still in-progress
        verifyState = this.host.getServiceState(null, ExampleServiceState.class, exampleURIs.get(0));
        assertEquals(verifyState.name, newState.name);

        // now abort
        boolean aborted = abort(txid2, 1);
        assertTrue(aborted);
        // This should be equal to the previous state -- since the transaction committed
        verifyState = this.host.getServiceState(null, ExampleServiceState.class, exampleURIs.get(0));
        // TODO re-enable when abort logic is debugged
        //assertEquals(verifyState.name, newState.name);
    }

    @Test
    public void testBasicCRUD() throws Throwable {
        // create ACCOUNT accounts in a single transaction, commit, query and verify count
        String txid = newTransaction();
        createAccounts(txid, this.accountCount);
        boolean committed = commit(txid, this.accountCount);
        assertTrue(committed);
        countAccounts(null, this.accountCount);

        // deposit 100 in each account in a single transaction, commit and verify balances
        txid = newTransaction();
        depositToAccounts(txid, this.accountCount, 100.0);
        committed = commit(txid, this.accountCount);
        assertTrue(committed);
        for (int i = 0; i < this.accountCount; i++) {
            verifyAccountBalance(null, buildAccountId(i), 100.0);
        }

        // delete ACCOUNT accounts in a single transaction, commit, query and verify count == 0
        txid = newTransaction();
        deleteAccounts(txid, this.accountCount);
        committed =  commit(txid, this.accountCount);
        assertTrue(committed);
        countAccounts(null, 0);
    }

    private String newTransaction() throws Throwable {
        String txid = UUID.randomUUID().toString();

        this.host.testStart(1);
        TransactionServiceState initialState = new TransactionServiceState();
        initialState.documentSelfLink = txid;
        Operation post = Operation
                .createPost(getTransactionFactoryUri())
                .setBody(initialState).setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    this.host.completeIteration();
                });
        this.host.send(post);
        this.host.testWait();

        return txid;
    }

    private boolean commit(String txid, int pendingOperations) throws Throwable {
        this.host.testStart(1);
        ResolutionRequest body = new ResolutionRequest();
        body.kind = TransactionService.ResolutionKind.COMMIT;
        body.pendingOperations = pendingOperations;
        boolean[] succeeded = new boolean[1];
        Operation commit = Operation
                .createPost(UriUtils.buildTransactionResolutionUri(this.host, txid))
                .setBody(body)
                .setCompletion((o, e) -> {
                    succeeded[0] = e == null;
                    this.host.completeIteration();
                });
        this.host.send(commit);
        this.host.testWait();

        return succeeded[0];
    }

    private boolean abort(String txid, int pendingOperations) throws Throwable {
        this.host.testStart(1);
        ResolutionRequest body = new ResolutionRequest();
        body.kind = TransactionService.ResolutionKind.ABORT;
        body.pendingOperations = pendingOperations;
        boolean[] succeeded = new boolean[1];
        Operation abort = Operation
                .createPost(UriUtils.buildTransactionResolutionUri(this.host, txid))
                .setBody(body)
                .setCompletion((o, e) -> {
                    succeeded[0] = e == null;
                    this.host.completeIteration();
                });
        this.host.send(abort);
        this.host.testWait();

        return succeeded[0];
    }

    private void updateExampleService(String txid, URI exampleServiceUri, ExampleServiceState exampleServiceState) throws Throwable {
        this.host.testStart(1);
        Operation put = Operation
                .createPut(exampleServiceUri)
                .setTransactionId(txid)
                .setBody(exampleServiceState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    this.host.completeIteration();
                });
        this.host.send(put);
        this.host.testWait();
    }

    private void verifyExampleServiceState(String txid, URI exampleServiceUri, ExampleServiceState exampleServiceState) throws Throwable {
        Operation operation = Operation
                .createGet(exampleServiceUri)
                .setTransactionId(txid)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    ExampleServiceState rsp = o.getBody(ExampleServiceState.class);
                    assertEquals(exampleServiceState.name, rsp.name);
                    this.host.completeIteration();
                });
        this.host.testStart(1);
        this.host.send(operation);
        this.host.testWait();
    }

    private void createAccounts(String transactionId, int accounts) throws Throwable {
        createAccounts(transactionId, accounts, 0.0);
    }

    private void createAccounts(String transactionId, int accounts, double initialBalance) throws Throwable {
        this.host.testStart(accounts);
        for (int i = 0; i < accounts; i++) {
            createAccount(transactionId, buildAccountId(i), initialBalance, false);
        }
        this.host.testWait();
    }

    public void createAccount(String transactionId, String accountId, boolean independentTest)
            throws Throwable {
        createAccount(transactionId, accountId, 0.0, independentTest);
    }

    private void createAccount(String transactionId, String accountId, double initialBalance, boolean independentTest)
            throws Throwable {
        if (independentTest) {
            this.host.testStart(1);
        }
        BankAccountServiceState initialState = new BankAccountServiceState();
        initialState.documentSelfLink = accountId;
        initialState.balance = initialBalance;
        Operation post = Operation
                .createPost(getAccountFactoryUri())
                .setBody(initialState).setCompletion((o, e) -> {
                    if (operationFailed(o, e)) {
                        this.host.failIteration(e);
                        return;
                    }
                    this.host.completeIteration();
                });
        if (transactionId != null) {
            post.setTransactionId(transactionId);
        }
        this.host.send(post);
        if (independentTest) {
            this.host.testWait();
        }
    }

    private void deleteAccounts(String transactionId, int accounts) throws Throwable {
        this.host.testStart(accounts);
        for (int i = 0; i < accounts; i++) {
            Operation delete = Operation
                    .createDelete(buildAccountUri(buildAccountId(i)))
                    .setCompletion((o, e) -> {
                        if (operationFailed(o, e)) {
                            this.host.failIteration(e);
                            return;
                        }
                        this.host.completeIteration();
                    });
            if (transactionId != null) {
                delete.setTransactionId(transactionId);
            }
            this.host.send(delete);
        }
        this.host.testWait();
    }

    private void countAccounts(String transactionId, long expected) throws Throwable {
        Query.Builder queryBuilder = Query.Builder.create().addKindFieldClause(BankAccountServiceState.class)
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK,
                        BankAccountService.FACTORY_LINK + UriUtils.URI_PATH_CHAR + this.baseAccountId + UriUtils.URI_WILDCARD_CHAR,
                        MatchType.WILDCARD);
        if (transactionId != null) {
            queryBuilder.addFieldClause(ServiceDocument.FIELD_NAME_TRANSACTION_ID, transactionId);
        }
        QueryTask task = QueryTask.Builder.createDirectTask().setQuery(queryBuilder.build()).build();
        this.host.createQueryTaskService(task, false, true, task, null);
        assertEquals(expected, task.results.documentCount.longValue());
    }

    public void sumAccounts(String transactionId, double expected) throws Throwable {
        Query.Builder queryBuilder = Query.Builder.create().addKindFieldClause(BankAccountServiceState.class)
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK,
                        BankAccountService.FACTORY_LINK + UriUtils.URI_PATH_CHAR + this.baseAccountId + UriUtils.URI_WILDCARD_CHAR,
                        MatchType.WILDCARD);
        // we need to sum up the account balances in a logical 'snapshot'. right now the only way to do this
        // is using a transaction, so if transactionId is null we're creating a new transaction
        boolean createNewTransaction = transactionId == null;
        if (createNewTransaction) {
            transactionId = newTransaction();
            this.host.log("Created new transaction %s for snapshot read", transactionId);
        } else {
            queryBuilder.addFieldClause(ServiceDocument.FIELD_NAME_TRANSACTION_ID, transactionId);
        }
        QueryTask task = QueryTask.Builder.createDirectTask().setQuery(queryBuilder.build()).build();
        this.host.createQueryTaskService(task, false, true, task, null);
        double sum = 0;
        for (String serviceSelfLink : task.results.documentLinks) {
            String accountId = serviceSelfLink.substring(serviceSelfLink.lastIndexOf('/') + 1);
            this.host.log("Reading account %s", accountId);
            BankAccountServiceState account = getAccount(transactionId, accountId);
            sum += account.balance;
            this.host.log("Read account %s, runnin sum=%f", accountId, sum);
        }
        if (createNewTransaction) {
            commit(transactionId, task.results.documentLinks.size());
        }
        assertEquals(expected, sum, 0);
    }

    private void depositToAccounts(String transactionId, int accounts, double amountToDeposit)
            throws Throwable {
        this.host.testStart(accounts);
        for (int i = 0; i < accounts; i++) {
            depositToAccount(transactionId, buildAccountId(i), amountToDeposit, false);
        }
        this.host.testWait();
    }

    private void depositToAccount(String transactionId, String accountId, double amountToDeposit,
            boolean independentTest)
            throws Throwable {
        if (independentTest) {
            this.host.testStart(1);
        }
        Operation patch = createDepositOperation(transactionId, accountId, amountToDeposit);
        patch.setCompletion((o, e) -> {
            if (operationFailed(o, e)) {
                this.host.failIteration(e);
                return;
            }
            this.host.completeIteration();
        });
        this.host.send(patch);
        if (independentTest) {
            this.host.testWait();
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

    public void withdrawFromAccount(String transactionId, String accountId,
            double amountToWithdraw,
            boolean independentTest)
            throws Throwable {
        Throwable[] ex = new Throwable[1];
        if (independentTest) {
            this.host.testStart(1);
        }
        Operation patch = createWithdrawOperation(transactionId, accountId, amountToWithdraw);
        patch.setCompletion((o, e) -> {
            if (operationFailed(o, e)) {
                if (o.getStatusCode() == Operation.STATUS_CODE_BAD_REQUEST) {
                    ex[0] = new IllegalArgumentException();
                    this.host.completeIteration();
                } else {
                    this.host.failIteration(e);
                }
                return;
            }
            this.host.completeIteration();
        });
        this.host.send(patch);
        if (independentTest) {
            this.host.testWait();
        }

        if (ex[0] != null) {
            throw ex[0];
        }
    }

    private Operation createWithdrawOperation(String transactionId, String accountId, double amount) {
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

    private void verifyAccountBalance(String transactionId, String accountId, double expectedBalance)
            throws Throwable {
        double balance = getAccount(transactionId, accountId).balance;
        assertEquals(expectedBalance, balance, 0);
    }

    private BankAccountServiceState getAccount(String transactionId, String accountId)
            throws Throwable {
        BankAccountServiceState[] responses = new BankAccountServiceState[1];
        this.host.testStart(1);
        Operation get = Operation
                .createGet(buildAccountUri(accountId))
                .setCompletion((o, e) -> {
                    if (operationFailed(o, e)) {
                        this.host.failIteration(e);
                        return;
                    }
                    responses[0] = o.getBody(BankAccountServiceState.class);
                    this.host.completeIteration();
                });
        if (transactionId != null) {
            get.setTransactionId(transactionId);
        }
        this.host.send(get);
        this.host.testWait();

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
