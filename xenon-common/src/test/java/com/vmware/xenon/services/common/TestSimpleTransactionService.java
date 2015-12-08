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
import java.util.Random;
import java.util.UUID;

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
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.xenon.services.common.SimpleTransactionService.SimpleTransactionServiceState;
import com.vmware.xenon.services.common.SimpleTransactionService.TransactionalRequestFilter;
import com.vmware.xenon.services.common.TestSimpleTransactionService.BankAccountService.BankAccountServiceRequest;
import com.vmware.xenon.services.common.TestSimpleTransactionService.BankAccountService.BankAccountServiceState;

public class TestSimpleTransactionService extends BasicReusableHostTestCase {

    static final int RETRIES_IN_CASE_OF_CONFLICTS = 5;
    // upper bound on random sleep between retries
    static final int SLEEP_BETWEEN_RETRIES_MILLIS = 2000;

    /**
     * Parameter that specifies the number of accounts to create
     */
    public int accountCount = 20;

    private long baseAccountId;

    private String buildAccountId(int i) {
        return this.baseAccountId + "-" + String.valueOf(i);
    }

    @Before
    public void setUp() throws Exception {
        try {
            this.baseAccountId = Utils.getNowMicrosUtc();
            this.host.setTransactionService(null);
            if (this.host.getServiceStage(SimpleTransactionFactoryService.SELF_LINK) == null) {
                this.host.startServiceAndWait(SimpleTransactionFactoryService.class,
                        SimpleTransactionFactoryService.SELF_LINK);
                this.host.startServiceAndWait(BankAccountFactoryService.class,
                        BankAccountFactoryService.SELF_LINK);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
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

    @Test
    public void testVisibilityWithinTransaction() throws Throwable {
        String txid = newTransaction();
        for (int i = 0; i < this.accountCount; i++) {
            String accountId = buildAccountId(i);
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
        for (int i = 0; i < this.accountCount; i++) {
            String txid = newTransaction();
            String accountId = buildAccountId(i);
            createAccount(txid, accountId, true);
            if (i % 2 == 0) {
                depositToAccount(txid, accountId, 100.0, true);
                commit(txid);
            } else {
                abort(txid);
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
            String accountId = buildAccountId(i);
            createAccount(txids[i], accountId, true);
            if (i % 2 == 0) {
                depositToAccount(txids[i], accountId, 100.0, true);
            }
        }

        for (int i = 0; i < this.accountCount; i++) {
            String accountId = buildAccountId(i);
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

        for (int i = 0; i < this.accountCount; i++) {
            commit(txids[i]);
        }
        countAccounts(null, this.accountCount);
        sumAccounts(null, 100.0 * this.accountCount / 2);

        deleteAccounts(null, this.accountCount);
        countAccounts(null, 0);
    }

    @Test
    public void testSingleClientMultiDocumentTransactions() throws Throwable {
        String txid = newTransaction();
        createAccounts(txid, this.accountCount, 100.0);
        commit(txid);

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
            try {
                withdrawFromAccount(txids[k], buildAccountId(i), amount, true);
                depositToAccount(txids[k], buildAccountId(i), amount, true);
            } catch (IllegalStateException e) {
                abort(txids[k]);
                txids[k] = null;
            }
        }

        for (int k = 0; k < numOfTransfers; k++) {
            if (txids[k] == null) {
                continue;
            }
            if (k % 5 == 0) {
                abort(txids[k]);
            } else {
                commit(txids[k]);
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
        commit(txid);

        int numOfTransfers = this.accountCount / 3;
        String[] txids = newTransactions(numOfTransfers);
        sendWithdrawDepositOperationPairs(txids, numOfTransfers, true);
        sumAccounts(null, 100.0 * this.accountCount);

        deleteAccounts(null, this.accountCount);
        countAccounts(null, 0);
    }

    @Test
    public void testAtomicVisibilityNonTransactional() throws Throwable {
        String txid = newTransaction();
        createAccounts(txid, this.accountCount, 100.0);
        commit(txid);

        int numOfTransfers = this.accountCount / 3;
        String[] txids = newTransactions(numOfTransfers);

        try {
            sendWithdrawDepositOperationPairs(txids, numOfTransfers, false);
        } catch (Throwable t) {
            assertNull(t);
        }

        sumAccounts(null, 100.0 * this.accountCount);
    }

    private void sendWithdrawDepositOperationPairs(String[] txids, int numOfTransfers, boolean independentTest) throws Throwable {
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
            int amount = 1 + rand.nextInt(3);
            this.host
                    .log("Transaction %s: Transferring $%d from %d to %d", tid, amount, i, final_j);
            Operation withdraw = createWithdrawOperation(tid, buildAccountId(i), amount);
            withdraw.setCompletion((o, e) -> {
                if (e != null) {
                    this.host.log("Transaction %s: failed to withdraw, aborting...", tid);
                    Operation abort = SimpleTransactionService.TxUtils.buildAbortRequest(this.host,
                            tid);
                    abort.setCompletion((op, ex) -> {
                        if (independentTest) {
                            this.host.completeIteration();
                        }
                    });
                    this.host.send(abort);
                    return;
                }
                Operation deposit = createDepositOperation(tid, buildAccountId(final_j), amount);
                deposit.setCompletion((op, ex) -> {
                    if (ex != null) {
                        this.host.log("Transaction %s: failed to deposit, aborting...", tid);
                        Operation abort = SimpleTransactionService.TxUtils.buildAbortRequest(
                                this.host, tid);
                        abort.setCompletion((op2, ex2) -> {
                            if (independentTest) {
                                this.host.completeIteration();
                            }
                        });
                        this.host.send(abort);
                        return;
                    }
                    this.host.log("Transaction %s: Committing", tid);
                    Operation commit = SimpleTransactionService.TxUtils.buildCommitRequest(
                            this.host, tid);
                    commit.setCompletion((op2, ex2) -> {
                        if (independentTest) {
                            this.host.completeIteration();
                        }
                    });
                    this.host.send(commit);
                });
                this.host.send(deposit);
            });
            requests.add(withdraw);
        }

        if (independentTest) {
            this.host.testStart(numOfTransfers);
        }
        for (Operation withdraw : requests) {
            this.host.send(withdraw);
        }
        if (independentTest) {
            this.host.testWait();
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
        createAccounts(transactionId, accounts, 0.0);
    }

    private void createAccounts(String transactionId, int accounts, double initialBalance) throws Throwable {
        this.host.testStart(accounts);
        for (int i = 0; i < accounts; i++) {
            createAccount(transactionId, buildAccountId(i), initialBalance, false);
        }
        this.host.testWait();
    }

    private void createAccount(String transactionId, String accountId, boolean independentTest)
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
                        BankAccountFactoryService.SELF_LINK + UriUtils.URI_PATH_CHAR + this.baseAccountId + UriUtils.URI_WILDCARD_CHAR,
                        MatchType.WILDCARD);
        if (transactionId != null) {
            queryBuilder.addFieldClause(ServiceDocument.FIELD_NAME_TRANSACTION_ID, transactionId);
        }
        QueryTask task = QueryTask.Builder.createDirectTask().setQuery(queryBuilder.build()).build();
        this.host.createQueryTaskService(task, false, true, task, null);
        assertEquals(expected, task.results.documentCount.longValue());
    }

    private void sumAccounts(String transactionId, double expected) throws Throwable {
        Query.Builder queryBuilder = Query.Builder.create().addKindFieldClause(BankAccountServiceState.class)
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK,
                        BankAccountFactoryService.SELF_LINK + UriUtils.URI_PATH_CHAR + this.baseAccountId + UriUtils.URI_WILDCARD_CHAR,
                        MatchType.WILDCARD);
        if (transactionId != null) {
            queryBuilder.addFieldClause(ServiceDocument.FIELD_NAME_TRANSACTION_ID, transactionId);
        }
        QueryTask task = QueryTask.Builder.createDirectTask().setQuery(queryBuilder.build()).build();
        this.host.createQueryTaskService(task, false, true, task, null);
        double sum = 0;
        for (String serviceSelfLink : task.results.documentLinks) {
            String accountId = serviceSelfLink.substring(serviceSelfLink.lastIndexOf('/') + 1);
            for (int i = 0; i < RETRIES_IN_CASE_OF_CONFLICTS; i++) {
                try {
                    BankAccountServiceState account = getAccount(transactionId, accountId);
                    sum += account.balance;
                    break;
                } catch (IllegalStateException ex) {
                    this.host.log("Could not read account %s probably due to a transactional conflict", accountId);
                    Thread.sleep(new Random().nextInt(SLEEP_BETWEEN_RETRIES_MILLIS));
                    if (i == RETRIES_IN_CASE_OF_CONFLICTS - 1) {
                        this.host.log("Giving up reading account %s", accountId);
                    } else {
                        this.host.log("Retrying reading account %s", accountId);
                    }
                }
            }
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