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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationContext;
import com.vmware.xenon.common.OperationProcessingChain;
import com.vmware.xenon.common.RequestRouter;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Occurance;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
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
     * Command line argument specifying default number of accounts
     */
    public int accountCount = 20;

    /**
     * Command line argument specifying default number of in process service hosts
     */
    public int nodeCount = 3;

    /**
     * A base component of generated account ids to provide uniqueness
     */
    private long baseAccountId;

    /**
     * Default host for test operations. This is the reusable host in case of
     * single-host tests and a peer host in the case of multi-hosts tests.
     */
    private VerificationHost defaultHost;

    /**
     * Controls whether the current test is a multi-host test.
     * This is explicitly set by {@link #setUpMultiHost()} and unset by
     * {@link #tearDownMultiHost()}.
     */
    private boolean multiHostTest = false;

    private String buildAccountId(int i) {
        return this.baseAccountId + "-" + String.valueOf(i);
    }

    @Before
    public void setUp() throws Exception {
        try {
            this.baseAccountId = Utils.getNowMicrosUtc();
            setUpHostWithAdditionalServices(this.host);
            this.defaultHost = this.host;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private void setUpMultiHost() throws Throwable {
        this.multiHostTest = true;
        this.host.setUpPeerHosts(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            setUpHostWithAdditionalServices(h);
        }

        this.defaultHost = this.host.getPeerHost();
        this.defaultHost.waitForReplicatedFactoryServiceAvailable(getTransactionFactoryUri());
        this.defaultHost.waitForReplicatedFactoryServiceAvailable(getAccountFactoryUri());
    }

    @After
    public void tearDownMultiHost() {
        if (this.multiHostTest) {
            this.host.tearDownInProcessPeers();
            this.defaultHost = this.host;
            this.multiHostTest = false;
        }
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

    @Test
    public void testTransactionContextFlow() throws Throwable {
        // stateless service that creates a bank account
        // with the transactionId on the parent operation
        // and one without
        StatelessService childService = new StatelessService() {
            @Override
            public void handlePost(Operation postOp) {
                try {
                    createAccount(null, buildAccountId(0), 0.0, true);
                    OperationContext.setTransactionId(null);
                    createAccount(null, buildAccountId(1), 0.0, true);
                } catch (Throwable e) {
                    postOp.fail(e);
                    return;
                }
                postOp.complete();
            }
        };
        String servicePath = UUID.randomUUID().toString();
        Operation startOp = Operation.createPost(UriUtils.buildUri(this.defaultHost, servicePath));
        this.defaultHost.startService(startOp, childService);
        // create two bank accounts
        String txid = newTransaction();
        TestContext ctx = testCreate(1);
        Operation postOp = Operation.createPost(UriUtils.buildUri(this.defaultHost, servicePath))
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }
                    // the transaction id here is what is set in the op; setting it
                    // to null in the stateless service should not be reflected here
                    if (OperationContext.getTransactionId() == null) {
                        ctx.failIteration(new IllegalStateException("transactionId not set"));
                        return;
                    }
                    ctx.completeIteration();
                });
        postOp.setTransactionId(txid);
        this.defaultHost.send(postOp);
        this.defaultHost.testWait(ctx);
        // only one account should be visible at this stage within the transaction
        countAccounts(txid, 1);
        countAccounts(null, 1);
        commit(txid);
        // verify that two accounts are created (one as part of the transaction and one without)
        countAccounts(null, 2);
        this.baseAccountId = Utils.getNowMicrosUtc();
        txid = newTransaction();
        postOp = Operation.createPost(UriUtils.buildUri(this.defaultHost, servicePath));
        postOp.setTransactionId(txid);
        this.defaultHost.sendAndWaitExpectSuccess(postOp);
        // transaction is still in progress, the account just created must be visible
        countAccounts(txid, 1);
        abort(txid);
        // verify that the account created without a transaction context is still present
        countAccounts(null, 1);
    }

    @Test
    public void testBasicCRUDMultiHost() throws Throwable {
        setUpMultiHost();
        testBasicCRUD();
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
    public void testAtomicVisibilityTransactional() throws Throwable {
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

    @Test
    public void testTransactionWithFailedOperations() throws Throwable {
        // create accounts, each with an initial balance of 100
        String txid = newTransaction();
        createAccounts(txid, this.accountCount, 100.0);
        commit(txid);
        countAccounts(null, this.accountCount);

        // try to withdraw more than balance (should fail) from odd accounts
        txid = newTransaction();
        for (int i = 0; i < this.accountCount; i++) {
            verifyAccountBalance(null, buildAccountId(i), 100.0);
            double amountToWithdraw = i % 2 == 0 ? 100.0 : 101.0;
            try {
                this.defaultHost.log("trying to withdraw %f from account %d", amountToWithdraw, i);
                withdrawFromAccount(txid, buildAccountId(i), amountToWithdraw, true);
            } catch (IllegalArgumentException ex) {
                assertTrue(i % 2 != 0);
            }
        }
        abort(txid);

        // verify balances
        for (int i = 0; i < this.accountCount; i++) {
            verifyAccountBalance(null, buildAccountId(i), 100.0);
        }

        // delete accounts
        txid = newTransaction();
        deleteAccounts(txid, this.accountCount);
        commit(txid);
        countAccounts(null, 0);
    }

    @Test
    public void testHostRestartMidTransaction() throws Throwable {
        // create a separate host for this test
        VerificationHost vhost = VerificationHost.create(0);
        try {
            vhost.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(250));
            vhost.start();
            setUpHostWithAdditionalServices(vhost);
            this.defaultHost = vhost;

            // create accounts in a new transaction, do not commit yet
            String txid = newTransaction();
            createAccounts(txid, this.accountCount, 100.0);

            // restart host
            this.host.stopHostAndPreserveState(vhost);
            boolean restarted = VerificationHost.restartStatefulHost(vhost);
            if (!restarted) {
                this.host.log(Level.WARNING, "Could not restart host, skipping test...");
                return;
            }
            setUpHostWithAdditionalServices(vhost);
            vhost.waitForReplicatedFactoryServiceAvailable(getAccountFactoryUri());
            vhost.waitForReplicatedFactoryServiceAvailable(getTransactionFactoryUri());

            // verify accounts can be used
            for (int i = 0; i < this.accountCount; i++) {
                withdrawFromAccount(txid, buildAccountId(i), 30.0, true);
                verifyAccountBalance(txid, buildAccountId(i), 70.0);
            }

            // now commit...and verify count
            commit(txid);
            countAccounts(null, this.accountCount);

            // clean up
            deleteAccounts(null, this.accountCount);
            countAccounts(null, 0);
        } finally {
            if (vhost.isStarted()) {
                try {
                    vhost.tearDown();
                } catch (Exception e) {
                    this.host.log(Level.WARNING, "Failed to tear down host during cleanup: ",
                            e.getMessage());
                }
            }
            this.defaultHost = this.host;
        }
    }

    @Ignore
    @Test
    public void testClientFailureMidTransaction() throws Throwable {
        // create accounts in a new transaction, do not commit
        long transactionExpirationTimeMicros = Utils.getNowMicrosUtc()
                + TimeUnit.SECONDS.toMicros(1);
        String txid = newTransaction(transactionExpirationTimeMicros);
        createAccounts(txid, this.accountCount, 0.0);
        depositToAccounts(txid, this.accountCount, 100.0);

        // if this was a real client, that for some reason failed/disconnected,
        // no-one would have driven the transaction forward, effectively
        // keeping the accounts 'locked'; unless the transaction automatically
        // rolls-back...that's what we're verifying here.
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        for (int i = 0; i < this.accountCount; i++) {
            // the transaction has expired by now - verify we can access accounts
            withdrawFromAccount(null, buildAccountId(i), 30.0, true);
            verifyAccountBalance(null, buildAccountId(i), 70.0);
        }

        // now verify we can access in a new transaction
        String txid2 = newTransaction();
        for (int i = 0; i < this.accountCount; i++) {
            withdrawFromAccount(txid2, buildAccountId(i), 20.0, true);
            verifyAccountBalance(txid2, buildAccountId(i), 50.0);
        }
        commit(txid2);
        sumAccounts(null, this.accountCount * 50.0);

        deleteAccounts(null, this.accountCount);
        countAccounts(null, 0);
    }

    @Test
    public void testStrictUpdateChecking() throws Throwable {
        // start a factory with ServiceOption.STRICT_UPDATE_CHECKING
        this.defaultHost.startServiceAndWait(StrictUpdateCheckFactoryService.class,
                StrictUpdateCheckFactoryService.SELF_LINK);

        // create a document in the transaction
        String txid = newTransaction();
        StrictUpdateCheckService.StrictUpdateCheckServiceState initialState = new StrictUpdateCheckService.StrictUpdateCheckServiceState();
        String id = UUID.randomUUID().toString();
        initialState.documentSelfLink = id;
        this.defaultHost.testStart(1);
        Operation post = Operation.createPost(
                UriUtils.buildUri(this.defaultHost, StrictUpdateCheckFactoryService.SELF_LINK))
                .setBody(initialState).setTransactionId(txid)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.defaultHost.failIteration(e);
                        return;
                    }

                    this.defaultHost.completeIteration();
                });
        this.defaultHost.send(post);
        this.defaultHost.testWait();

        // get and patch with same version
        URI childUri = UriUtils.buildUri(this.defaultHost,
                StrictUpdateCheckFactoryService.SELF_LINK + "/" + id);
        StrictUpdateCheckService.StrictUpdateCheckServiceState[] state = new StrictUpdateCheckService.StrictUpdateCheckServiceState[1];
        this.defaultHost.testStart(1);
        Operation get = Operation.createGet(childUri).setTransactionId(txid)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.defaultHost.failIteration(e);
                        return;
                    }

                    state[0] = o
                            .getBody(StrictUpdateCheckService.StrictUpdateCheckServiceState.class);
                    this.defaultHost.completeIteration();
                });
        this.defaultHost.send(get);
        this.defaultHost.testWait();

        this.defaultHost.testStart(1);
        Operation patch = Operation.createPatch(childUri).setTransactionId(txid).setBody(state[0])
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.defaultHost.failIteration(e);
                        return;
                    }

                    this.defaultHost.completeIteration();
                });
        this.defaultHost.send(patch);
        this.defaultHost.testWait();

        // finally, commit transaction
        commit(txid);
    }

    @Test
    public void testNullSelfLink() throws Throwable {
        String txid = newTransaction();
        this.defaultHost.testStart(1);
        BankAccountServiceState initialState = new BankAccountServiceState();
        // set documentSelfLink to null, to test coordinator-generated self-link
        initialState.documentSelfLink = null;
        initialState.balance = 100.0;
        Operation post = Operation
                .createPost(getAccountFactoryUri())
                .setBody(initialState).setCompletion((o, e) -> {
                    if (operationFailed(o, e)) {
                        this.defaultHost.failIteration(e);
                        return;
                    }
                    this.defaultHost.completeIteration();
                });
        post.setTransactionId(txid);
        this.defaultHost.send(post);
        this.defaultHost.testWait();
        commit(txid);
    }

    @Test
    public void testAbsoluteSelfLink() throws Throwable {
        String txid = newTransaction();
        this.defaultHost.testStart(1);
        BankAccountServiceState initialState = new BankAccountServiceState();
        // set documentSelfLink - use full path
        initialState.documentSelfLink = buildAccountUri(buildAccountId(0)).getPath();
        initialState.balance = 100.0;
        Operation post = Operation
                .createPost(getAccountFactoryUri())
                .setBody(initialState).setCompletion((o, e) -> {
                    if (operationFailed(o, e)) {
                        this.defaultHost.failIteration(e);
                        return;
                    }
                    this.defaultHost.completeIteration();
                });
        post.setTransactionId(txid);
        this.defaultHost.send(post);
        this.defaultHost.testWait();
        commit(txid);
        countAccounts(null, 1);
    }

    private void sendWithdrawDepositOperationPairs(String[] txids, int numOfTransfers,
            boolean independentTest) throws Throwable {
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
            this.defaultHost
                    .log("Transaction %s: Transferring $%d from %d to %d", tid, amount, i, final_j);
            Operation withdraw = createWithdrawOperation(tid, buildAccountId(i), amount);
            withdraw.setCompletion((o, e) -> {
                if (e != null) {
                    this.defaultHost.log("Transaction %s: failed to withdraw, aborting...", tid);
                    Operation abort = SimpleTransactionService.TxUtils.buildAbortRequest(
                            this.defaultHost,
                            tid);
                    abort.setCompletion((op, ex) -> {
                        if (independentTest) {
                            this.defaultHost.completeIteration();
                        }
                    });
                    this.defaultHost.send(abort);
                    return;
                }
                Operation deposit = createDepositOperation(tid, buildAccountId(final_j), amount);
                deposit.setCompletion((op, ex) -> {
                    if (ex != null) {
                        this.defaultHost.log("Transaction %s: failed to deposit, aborting...", tid);
                        Operation abort = SimpleTransactionService.TxUtils.buildAbortRequest(
                                this.defaultHost, tid);
                        abort.setCompletion((op2, ex2) -> {
                            if (independentTest) {
                                this.defaultHost.completeIteration();
                            }
                        });
                        this.defaultHost.send(abort);
                        return;
                    }
                    this.defaultHost.log("Transaction %s: Committing", tid);
                    Operation commit = SimpleTransactionService.TxUtils.buildCommitRequest(
                            this.defaultHost, tid);
                    commit.setCompletion((op2, ex2) -> {
                        if (independentTest) {
                            this.defaultHost.completeIteration();
                        }
                    });
                    this.defaultHost.send(commit);
                });
                this.defaultHost.send(deposit);
            });
            requests.add(withdraw);
        }

        if (independentTest) {
            this.defaultHost.testStart(numOfTransfers);
        }
        for (Operation withdraw : requests) {
            this.defaultHost.send(withdraw);
        }
        if (independentTest) {
            this.defaultHost.testWait();
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
        return newTransaction(0);
    }

    private String newTransaction(long transactionExpirationTimeMicros) throws Throwable {
        String txid = UUID.randomUUID().toString();

        // this section is required until IDEMPOTENT_POST is used
        this.defaultHost.testStart(1);
        SimpleTransactionServiceState initialState = new SimpleTransactionServiceState();
        initialState.documentSelfLink = txid;
        initialState.documentExpirationTimeMicros = transactionExpirationTimeMicros;
        Operation post = Operation
                .createPost(getTransactionFactoryUri())
                .setBody(initialState).setCompletion((o, e) -> {
                    if (e != null) {
                        this.defaultHost.failIteration(e);
                        return;
                    }
                    this.defaultHost.completeIteration();
                });
        this.defaultHost.send(post);
        this.defaultHost.testWait();

        return txid;
    }

    private void commit(String transactionId) throws Throwable {
        this.defaultHost.testStart(1);
        Operation patch = SimpleTransactionService.TxUtils.buildCommitRequest(this.defaultHost,
                transactionId);
        patch.setCompletion((o, e) -> {
            if (operationFailed(o, e)) {
                this.defaultHost.failIteration(e);
                return;
            }
            this.defaultHost.completeIteration();
        });
        this.defaultHost.send(patch);
        this.defaultHost.testWait();
    }

    private void abort(String transactionId) throws Throwable {
        this.defaultHost.testStart(1);
        Operation patch = SimpleTransactionService.TxUtils.buildAbortRequest(this.defaultHost,
                transactionId);
        patch.setCompletion((o, e) -> {
            if (operationFailed(o, e)) {
                this.defaultHost.failIteration(e);
                return;
            }
            this.defaultHost.completeIteration();
        });
        this.defaultHost.send(patch);
        this.defaultHost.testWait();
    }

    private void createAccounts(String transactionId, int accounts) throws Throwable {
        createAccounts(transactionId, accounts, 0.0);
    }

    private void createAccounts(String transactionId, int accounts, double initialBalance)
            throws Throwable {
        this.defaultHost.testStart(accounts);
        for (int i = 0; i < accounts; i++) {
            createAccount(transactionId, buildAccountId(i), initialBalance, false);
        }
        this.defaultHost.testWait();
    }

    private void createAccount(String transactionId, String accountId, boolean independentTest)
            throws Throwable {
        createAccount(transactionId, accountId, 0.0, independentTest);
    }

    private void createAccount(String transactionId, String accountId, double initialBalance,
            boolean independentTest)
            throws Throwable {
        if (independentTest) {
            this.defaultHost.testStart(1);
        }
        BankAccountServiceState initialState = new BankAccountServiceState();
        initialState.documentSelfLink = accountId;
        initialState.balance = initialBalance;
        Operation post = Operation
                .createPost(getAccountFactoryUri())
                .setBody(initialState).setCompletion((o, e) -> {
                    if (operationFailed(o, e)) {
                        this.defaultHost.failIteration(e);
                        return;
                    }
                    this.defaultHost.completeIteration();
                });
        if (transactionId != null) {
            post.setTransactionId(transactionId);
        }
        this.defaultHost.send(post);
        if (independentTest) {
            this.defaultHost.testWait();
        }
    }

    private void deleteAccounts(String transactionId, int accounts) throws Throwable {
        this.defaultHost.testStart(accounts);
        for (int i = 0; i < accounts; i++) {
            Operation delete = Operation
                    .createDelete(buildAccountUri(buildAccountId(i)))
                    .setCompletion((o, e) -> {
                        if (operationFailed(o, e)) {
                            this.defaultHost.failIteration(e);
                            return;
                        }
                        this.defaultHost.completeIteration();
                    });
            if (transactionId != null) {
                delete.setTransactionId(transactionId);
            }
            this.defaultHost.send(delete);
        }
        this.defaultHost.testWait();
    }

    private void countAccounts(String transactionId, long expected) throws Throwable {
        this.host.waitFor("results did not converge, expected: " + expected, () -> {
            Query.Builder queryBuilder = Query.Builder
                    .create()
                    .addKindFieldClause(BankAccountServiceState.class)
                    .addFieldClause(
                            ServiceDocument.FIELD_NAME_SELF_LINK,
                            BankAccountFactoryService.SELF_LINK + UriUtils.URI_PATH_CHAR
                                    + this.baseAccountId + UriUtils.URI_WILDCARD_CHAR,
                            MatchType.WILDCARD);
            if (transactionId != null) {
                queryBuilder.addFieldClause(ServiceDocument.FIELD_NAME_TRANSACTION_ID,
                        transactionId);
            } else {
                queryBuilder.addFieldClause(ServiceDocument.FIELD_NAME_TRANSACTION_ID, "*",
                        MatchType.WILDCARD, Occurance.MUST_NOT_OCCUR);
            }
            QueryTask task = QueryTask.Builder.createDirectTask()
                    .setQuery(queryBuilder.build()).addOption(QueryOption.BROADCAST)
                    .build();
            this.defaultHost.createQueryTaskService(task, false, true, task, null);
            if (expected == task.results.documentCount.longValue()) {
                return true;
            }

            return false;
        });
    }

    private void sumAccounts(String transactionId, double expected) throws Throwable {
        Query.Builder queryBuilder = Query.Builder.create()
                .addKindFieldClause(BankAccountServiceState.class)
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK,
                        BankAccountFactoryService.SELF_LINK + UriUtils.URI_PATH_CHAR
                                + this.baseAccountId + UriUtils.URI_WILDCARD_CHAR,
                        MatchType.WILDCARD);
        // we need to sum up the account balances in a logical 'snapshot'. rigt now the only way to do it
        // is using a transaction, so if transactionId is null we're creating a new transaction
        boolean createNewTransaction = transactionId == null;
        if (createNewTransaction) {
            transactionId = newTransaction();
            this.defaultHost.log("Created new transaction %s for snapshot read", transactionId);
        } else {
            queryBuilder.addFieldClause(ServiceDocument.FIELD_NAME_TRANSACTION_ID, transactionId);
        }
        QueryTask task = QueryTask.Builder.createDirectTask().setQuery(queryBuilder.build())
                .addOption(QueryOption.BROADCAST).build();
        this.defaultHost.createQueryTaskService(task, false, true, task, null);
        double sum = 0;
        for (String serviceSelfLink : task.results.documentLinks) {
            String accountId = serviceSelfLink.substring(serviceSelfLink.lastIndexOf('/') + 1);
            for (int i = 0; i < RETRIES_IN_CASE_OF_CONFLICTS; i++) {
                try {
                    this.defaultHost.log("Trying to read account %s", accountId);
                    BankAccountServiceState account = getAccount(transactionId, accountId);
                    sum += account.balance;
                    this.defaultHost.log("Successfully read account %s, running sum=%f", accountId,
                            sum);
                    break;
                } catch (IllegalStateException ex) {
                    this.defaultHost.log(
                            "Could not read account %s probably due to a transactional conflict",
                            accountId);
                    Thread.sleep(new Random().nextInt(SLEEP_BETWEEN_RETRIES_MILLIS));
                    if (i == RETRIES_IN_CASE_OF_CONFLICTS - 1) {
                        this.defaultHost.log("Giving up reading account %s", accountId);
                    } else {
                        this.defaultHost.log("Retrying reading account %s", accountId);
                    }
                }
            }
        }
        if (createNewTransaction) {
            commit(transactionId);
        }
        assertEquals(expected, sum, 0);
    }

    private void depositToAccounts(String transactionId, int accounts, double amountToDeposit)
            throws Throwable {
        this.defaultHost.testStart(accounts);
        for (int i = 0; i < accounts; i++) {
            depositToAccount(transactionId, buildAccountId(i), amountToDeposit, false);
        }
        this.defaultHost.testWait();
    }

    private void depositToAccount(String transactionId, String accountId, double amountToDeposit,
            boolean independentTest)
            throws Throwable {
        Throwable[] ex = new Throwable[1];
        if (independentTest) {
            this.defaultHost.testStart(1);
        }
        Operation patch = createDepositOperation(transactionId, accountId, amountToDeposit);
        patch.setCompletion((o, e) -> {
            if (operationFailed(o, e)) {
                if (e instanceof IllegalStateException) {
                    ex[0] = e;
                    this.defaultHost.completeIteration();
                } else {
                    this.defaultHost.failIteration(e);
                }
                return;
            }
            this.defaultHost.completeIteration();
        });
        this.defaultHost.send(patch);
        if (independentTest) {
            this.defaultHost.testWait();
        }

        if (ex[0] != null) {
            throw ex[0];
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

    private void withdrawFromAccount(String transactionId, String accountId,
            double amountToWithdraw,
            boolean independentTest)
            throws Throwable {
        Throwable[] ex = new Throwable[1];
        if (independentTest) {
            this.defaultHost.testStart(1);
        }
        BankAccountServiceRequest body = new BankAccountServiceRequest();
        body.kind = BankAccountServiceRequest.Kind.WITHDRAW;
        body.amount = amountToWithdraw;
        Operation patch = createWithdrawOperation(transactionId, accountId, amountToWithdraw);
        patch.setCompletion((o, e) -> {
            if (operationFailed(o, e)) {
                if (e instanceof IllegalStateException || e instanceof IllegalArgumentException) {
                    ex[0] = e;
                    this.defaultHost.completeIteration();
                } else {
                    this.defaultHost.failIteration(e);
                }
                return;
            }
            this.defaultHost.completeIteration();
        });
        this.defaultHost.send(patch);
        if (independentTest) {
            this.defaultHost.testWait();
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
        Throwable[] ex = new Throwable[1];
        BankAccountServiceState[] responses = new BankAccountServiceState[1];
        this.defaultHost.testStart(1);
        Operation get = Operation
                .createGet(buildAccountUri(accountId))
                .setCompletion((o, e) -> {
                    if (operationFailed(o, e)) {
                        if (e instanceof IllegalStateException) {
                            ex[0] = e;
                            this.defaultHost.completeIteration();
                        } else {
                            this.defaultHost.failIteration(e);
                        }
                        return;
                    }
                    responses[0] = o.getBody(BankAccountServiceState.class);
                    this.defaultHost.completeIteration();
                });
        if (transactionId != null) {
            get.setTransactionId(transactionId);
        }
        this.defaultHost.send(get);
        this.defaultHost.testWait();

        if (ex[0] != null) {
            throw ex[0];
        }

        return responses[0];
    }

    private URI getTransactionFactoryUri() {
        return UriUtils.buildUri(this.defaultHost, SimpleTransactionFactoryService.class);
    }

    private URI getAccountFactoryUri() {
        return UriUtils.buildUri(this.defaultHost, BankAccountFactoryService.class);
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
            public enum Kind {
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

    public static class StrictUpdateCheckFactoryService extends FactoryService {

        public static final String SELF_LINK = ServiceUriPaths.SAMPLES + "/strict-updates";

        public StrictUpdateCheckFactoryService() {
            super(StrictUpdateCheckService.StrictUpdateCheckServiceState.class);
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            return new StrictUpdateCheckService();
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

    public static class StrictUpdateCheckService extends StatefulService {

        public static class StrictUpdateCheckServiceState extends ServiceDocument {

        }

        public StrictUpdateCheckService() {
            super(StrictUpdateCheckServiceState.class);
            super.toggleOption(ServiceOption.PERSISTENCE, true);
            super.toggleOption(ServiceOption.REPLICATION, true);
            super.toggleOption(ServiceOption.OWNER_SELECTION, true);
            super.toggleOption(ServiceOption.CONCURRENT_GET_HANDLING, false);
            super.toggleOption(ServiceOption.STRICT_UPDATE_CHECKING, true);
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

        @Override
        public void handlePatch(Operation patch) {
            StrictUpdateCheckServiceState newState = getBody(patch);
            setState(patch, newState);
            patch.complete();
        }
    }
}