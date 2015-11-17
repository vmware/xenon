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

package com.vmware.dcp.services.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Phaser;

import org.junit.Before;
import org.junit.Test;

import com.vmware.dcp.common.BasicTestCase;
import com.vmware.dcp.common.FactoryService;
import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.OperationProcessingChain;
import com.vmware.dcp.common.RequestRouter;
import com.vmware.dcp.common.Service;
import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.StatefulService;
import com.vmware.dcp.common.UriUtils;
import com.vmware.dcp.common.Utils;
import com.vmware.dcp.services.common.QueryTask.Query;
import com.vmware.dcp.services.common.SimpleTransactionService.SimpleTransactionServiceState;
import com.vmware.dcp.services.common.SimpleTransactionService.TransactionalRequestFilter;
import com.vmware.dcp.services.common.TestSimpleTransactionService.BankAccountService.BankAccountServiceRequest;
import com.vmware.dcp.services.common.TestSimpleTransactionService.BankAccountService.BankAccountServiceState;

public class TestSimpleTransactionService extends BasicTestCase {

    static final int ACCOUNTS = 20;

    @Before
    public void setUp() throws Exception {
        try {
            this.host.setTransactionService(null);
            this.host.startServiceAndWait(SimpleTransactionFactoryService.class,
                    SimpleTransactionFactoryService.SELF_LINK);
            this.host.startServiceAndWait(BankAccountFactoryService.class,
                    BankAccountFactoryService.SELF_LINK);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testBasicCRUD() throws Throwable {
        // create ACCOUNT accounts in a single transaction, commit, query and verify count
        String txid = newTransaction();
        createAccounts(txid, ACCOUNTS);
        commit(txid);
        countAccounts(null, ACCOUNTS);

        // deposit 100 in each account in a single transaction, commit and verify balances
        txid = newTransaction();
        depositToAccounts(txid, ACCOUNTS, 100.0);
        commit(txid);

        for (int i = 0; i < ACCOUNTS; i++) {
            verifyAccountBalance(null, String.valueOf(i), 100.0);
        }

        // delete ACCOUNT accounts in a single transaction, commit, query and verify count == 0
        txid = newTransaction();
        deleteAccounts(txid, ACCOUNTS);
        commit(txid);
        countAccounts(null, 0);
    }

    @Test
    public void testVisibilityWithinTransaction() throws Throwable {
        String txid = newTransaction();
        for (int i = 0; i < ACCOUNTS; i++) {
            String accountId = String.valueOf(i);
            createAccount(txid, accountId, true);
            countAccounts(txid, i + 1);
            depositToAccount(txid, accountId, 100.0, true);
            verifyAccountBalance(txid, accountId, 100.0);
        }
        abort(txid);
        countAccounts(null, 0);
    }

    @Test
    public void testShortTransactions() throws Throwable {
        for (int i = 0; i < ACCOUNTS; i++) {
            String txid = newTransaction();
            String accountId = String.valueOf(i);
            createAccount(txid, accountId, true);
            if (i % 2 == 0) {
                depositToAccount(txid, accountId, 100.0, true);
                commit(txid);
            } else {
                abort(txid);
            }
        }
        countAccounts(null, ACCOUNTS / 2);
        sumAccounts(null, 100.0 * ACCOUNTS / 2);
    }

    @Test
    public void testSingleClientMultipleActiveTransactions() throws Throwable {
        String[] txids = new String[ACCOUNTS];
        for (int i = 0; i < ACCOUNTS; i++) {
            txids[i] = newTransaction();
            String accountId = String.valueOf(i);
            createAccount(txids[i], accountId, true);
            if (i % 2 == 0) {
                depositToAccount(txids[i], accountId, 100.0, true);
            }
        }

        for (int i = 0; i < ACCOUNTS; i++) {
            String accountId = String.valueOf(i);
            for (int j = 0; j <= i; j++) {
                BankAccountServiceState account = null;
                boolean txConflict = false;
                try {
                    account = getAccount(txids[j], accountId);
                } catch (IllegalStateException e) {
                    txConflict = true;
                }
                if (j != i) {
                    assertTrue(txConflict);
                    continue;
                }
                if (i % 2 == 0) {
                    assertEquals(100.0, account.balance, 0);
                } else {
                    assertEquals(0, account.balance, 0);
                }
            }
        }

        for (int i = 0; i < ACCOUNTS; i++) {
            commit(txids[i]);
        }
        countAccounts(null, ACCOUNTS);
        sumAccounts(null, 100.0 * ACCOUNTS / 2);

        deleteAccounts(null, ACCOUNTS);
        countAccounts(null, 0);
    }

    @Test
    public void testSingleClientMultiDocumentTransactions() throws Throwable {
        String txid = newTransaction();
        for (int i = 0; i < ACCOUNTS; i++) {
            String accountId = String.valueOf(i);
            createAccount(txid, accountId, true);
            depositToAccount(txid, accountId, 100.0, true);
        }
        commit(txid);

        String[] txids = new String[ACCOUNTS / 3];
        Random rand = new Random();
        for (int k = 0; k < ACCOUNTS / 3; k++) {
            int i = rand.nextInt(ACCOUNTS);
            int j = rand.nextInt(ACCOUNTS);
            if (i == j) {
                j = (j + 1) % ACCOUNTS;
            }
            int amount = 1 + rand.nextInt(3);
            txids[k] = newTransaction();
            try {
                withdrawFromAccount(txids[k], String.valueOf(i), amount, true);
                depositToAccount(txids[k], String.valueOf(j), amount, true);
            } catch (IllegalStateException e) {
                abort(txids[k]);
                txids[k] = null;
                continue;
            }
        }

        for (int k = 0; k < ACCOUNTS / 3; k++) {
            if (txids[k] == null) {
                continue;
            }
            if (k % 5 == 0) {
                abort(txids[k]);
            } else {
                commit(txids[k]);
            }
        }

        sumAccounts(null, 100.0 * ACCOUNTS);

        deleteAccounts(null, ACCOUNTS);
        countAccounts(null, 0);
    }

    @Test
    public void testSingleClientMultiDocumentConcurrentTransactions() throws Throwable {
        String txid = newTransaction();
        for (int i = 0; i < ACCOUNTS; i++) {
            String accountId = String.valueOf(i);
            createAccount(txid, accountId, true);
            depositToAccount(txid, accountId, 100.0, true);
        }
        commit(txid);

        int numOfTransfers = ACCOUNTS / 3;
        String[] txids = new String[numOfTransfers];
        for (int k = 0; k < numOfTransfers; k++) {
            txids[k] = newTransaction();
        }

        // each transaction will issue at least 2 operations: withdraw, (potentially deposit) and commit/abort
        Phaser phaser = new Phaser(numOfTransfers * 2);
        Collection<Operation> requests = new ArrayList<Operation>(numOfTransfers);
        Random rand = new Random();
        for (int k = 0; k < numOfTransfers; k++) {
            final String tid = txids[k];
            int i = rand.nextInt(ACCOUNTS);
            int j = rand.nextInt(ACCOUNTS);
            if (i == j) {
                j = (j + 1) % ACCOUNTS;
            }
            final int final_j = j;
            int amount = 1 + rand.nextInt(3);
            Operation withdraw = createWithdrawOperation(tid, String.valueOf(i), amount);
            withdraw.setCompletion((o, e) -> {
                phaser.arrive();
                if (e != null) {
                    Operation abort = SimpleTransactionService.TxUtils.buildAbortRequest(this.host, tid);
                    abort.setCompletion((op, ex) -> {
                        phaser.arrive();
                    });
                    this.host.send(abort);
                    return;
                }
                phaser.register();
                Operation deposit = createDepositOperation(tid, String.valueOf(final_j), amount);
                deposit.setCompletion((op, ex) -> {
                    phaser.arrive();
                    if (ex != null) {
                        Operation abort = SimpleTransactionService.TxUtils.buildAbortRequest(this.host, tid);
                        abort.setCompletion((op2, ex2) -> {
                            phaser.arrive();
                        });
                        this.host.send(abort);
                        return;
                    }
                    Utils.logWarning("Transaction %s: Committing", tid);
                    Operation commit = SimpleTransactionService.TxUtils.buildCommitRequest(this.host, tid);
                    commit.setCompletion((op2, ex2) -> {
                        phaser.arrive();
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
        phaser.awaitAdvance(0);

        sumAccounts(null, 100.0 * ACCOUNTS);

        deleteAccounts(null, ACCOUNTS);
        countAccounts(null, 0);
    }

    private String newTransaction() throws Throwable {
        String txid = UUID.randomUUID().toString();

        // this section is required until IDEMPOTENT_POST is used
        this.host.testStart(1);
        SimpleTransactionServiceState initialState = new SimpleTransactionServiceState();
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

    private void commit(String transactionId) throws Throwable {
        this.host.testStart(1);
        Operation patch = SimpleTransactionService.TxUtils.buildCommitRequest(this.host,
                transactionId);
        patch.setCompletion((o, e) -> {
            if (operationFailed(o, e)) {
                this.host.failIteration(e);
                return;
            }
            this.host.completeIteration();
        });
        this.host.send(patch);
        this.host.testWait();
    }

    private void abort(String transactionId) throws Throwable {
        this.host.testStart(1);
        Operation patch = SimpleTransactionService.TxUtils.buildAbortRequest(this.host,
                transactionId);
        patch.setCompletion((o, e) -> {
            if (operationFailed(o, e)) {
                this.host.failIteration(e);
                return;
            }
            this.host.completeIteration();
        });
        this.host.send(patch);
        this.host.testWait();
    }

    private void createAccounts(String transactionId, int accounts) throws Throwable {
        this.host.testStart(accounts);
        for (int i = 0; i < accounts; i++) {
            createAccount(transactionId, String.valueOf(i), false);
        }
        this.host.testWait();
    }

    private void createAccount(String transactionId, String accountId, boolean independentTest)
            throws Throwable {
        if (independentTest) {
            this.host.testStart(1);
        }
        BankAccountServiceState initialState = new BankAccountServiceState();
        initialState.documentSelfLink = accountId;
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
                    .createDelete(buildAccountUri(String.valueOf(i)))
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
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        Query documentKindClause = new Query()
                .setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(BankAccountServiceState.KIND);
        if (transactionId == null) {
            q.query = documentKindClause;
        } else {
            Query transactionClause = new Query().setTermPropertyName(
                    ServiceDocument.FIELD_NAME_TRANSACTION_ID)
                    .setTermMatchValue(transactionId);
            q.query.addBooleanClause(documentKindClause).addBooleanClause(transactionClause);
        }
        QueryTask task = QueryTask.create(q).setDirect(true);
        this.host.createQueryTaskService(task, false, true, task, null);
        long count = 0;
        for (String serviceSelfLink : task.results.documentLinks) {
            String accountId = serviceSelfLink.substring(serviceSelfLink.lastIndexOf('/') + 1);
            try {
                BankAccountServiceState account = getAccount(transactionId, accountId);
                if (transactionId == null && account.documentTransactionId != null) {
                    continue;
                }
                count++;
            } catch (IllegalStateException ex) {
                continue;
            }

        }
        assertEquals(expected, count);
    }

    private void sumAccounts(String transactionId, double expected) throws Throwable {
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        Query documentKindClause = new Query()
                .setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(BankAccountServiceState.KIND);
        if (transactionId == null) {
            q.query = documentKindClause;
        } else {
            Query transactionClause = new Query().setTermPropertyName(
                    ServiceDocument.FIELD_NAME_TRANSACTION_ID)
                    .setTermMatchValue(transactionId);
            q.query.addBooleanClause(documentKindClause).addBooleanClause(transactionClause);
        }
        QueryTask task = QueryTask.create(q).setDirect(true);
        this.host.createQueryTaskService(task, false, true, task, null);
        double sum = 0;
        for (String serviceSelfLink : task.results.documentLinks) {
            String accountId = serviceSelfLink.substring(serviceSelfLink.lastIndexOf('/') + 1);
            try {
                BankAccountServiceState account = getAccount(transactionId, accountId);
                if (transactionId == null && account.documentTransactionId != null) {
                    continue;
                }
                sum += account.balance;
            } catch (IllegalStateException ex) {
                continue;
            }
        }
        assertEquals(expected, sum, 0);
    }

    private void depositToAccounts(String transactionId, int accounts, double amountToDeposit)
            throws Throwable {
        this.host.testStart(accounts);
        for (int i = 0; i < accounts; i++) {
            depositToAccount(transactionId, String.valueOf(i), amountToDeposit, false);
        }
        this.host.testWait();
    }

    private void depositToAccount(String transactionId, String accountId, double amountToDeposit,
            boolean independentTest)
            throws Throwable {
        Throwable[] ex = new Throwable[1];
        if (independentTest) {
            this.host.testStart(1);
        }
        Operation patch = createDepositOperation(transactionId, accountId, amountToDeposit);
        patch.setCompletion((o, e) -> {
            if (operationFailed(o, e)) {
                if (e instanceof IllegalStateException) {
                    ex[0] = e;
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

    private void withdrawFromAccount(String transactionId, String accountId,
            double amountToWithdraw,
            boolean independentTest)
            throws Throwable {
        Throwable[] ex = new Throwable[1];
        if (independentTest) {
            this.host.testStart(1);
        }
        BankAccountServiceRequest body = new BankAccountServiceRequest();
        body.kind = BankAccountServiceRequest.Kind.WITHDRAW;
        body.amount = amountToWithdraw;
        Operation patch = createWithdrawOperation(transactionId, accountId, amountToWithdraw);
        patch.setCompletion((o, e) -> {
            if (operationFailed(o, e)) {
                if (e instanceof IllegalStateException) {
                    ex[0] = e;
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
        Throwable[] ex = new Throwable[1];
        BankAccountServiceState[] responses = new BankAccountServiceState[1];
        this.host.testStart(1);
        Operation get = Operation
                .createGet(buildAccountUri(accountId))
                .setCompletion((o, e) -> {
                    if (operationFailed(o, e)) {
                        if (e instanceof IllegalStateException) {
                            ex[0] = e;
                            this.host.completeIteration();
                        } else {
                            this.host.failIteration(e);
                        }
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
                patch.fail(new IllegalArgumentException("Not enough funds to withdraw"));
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