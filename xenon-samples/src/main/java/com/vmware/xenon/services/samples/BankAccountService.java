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

package com.vmware.xenon.services.samples;

import com.vmware.xenon.common.DefaultPromise;
import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationProcessingChain;
import com.vmware.xenon.common.Promise;
import com.vmware.xenon.common.RequestRouter;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.SimpleStateAwareRequestsHandler;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class BankAccountService extends SimpleStateAwareRequestsHandler<BankAccountService.BankAccountServiceState> {

    public static final String FACTORY_LINK = ServiceUriPaths.SAMPLES + "/bank-accounts";

    public static Service createFactory() {
        return FactoryService.create(BankAccountService.class);
    }

    public static class BankAccountServiceState extends ServiceDocument {
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
    public Promise<BankAccountServiceState> handleStart(BankAccountServiceState initialState) {
        return DefaultPromise.completed(initialState).thenApply(this::validateState);
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

    private BankAccountServiceState validateState(BankAccountServiceState state) {
        if (state == null) {
            throw new IllegalArgumentException("attempt to initialize service with an empty state");
        }

        if (state.balance < 0) {
            throw new IllegalArgumentException("balance cannot be negative");
        }
        return state;
    }

}
