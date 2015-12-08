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

package com.vmware.xenon.samples.bankaccount.services;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationProcessingChain;
import com.vmware.xenon.common.RequestRouter;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;


public class BankAccountService extends StatefulService {

    public static class BankAccountState extends ServiceDocument {
        public double balance;
    }

    public static class BankAccountRequest {
        public enum Kind {
            DEPOSIT, WITHDRAW
        }

        public Kind kind;
        public double amount;
    }

    public BankAccountService() {
        super(BankAccountState.class);
        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handleStart(Operation post) {
        try {
            validateState(post);
            post.complete();
        } catch (Exception e) {
            post.fail(e);
        }
    }

    @Override
    public OperationProcessingChain getOperationProcessingChain() {
        if (super.getOperationProcessingChain() != null) {
            return super.getOperationProcessingChain();
        }

        RequestRouter myRouter = new RequestRouter();
        myRouter.register(
                Action.PATCH,
                new RequestRouter.RequestBodyMatcher<>(BankAccountRequest.class, "kind", BankAccountRequest.Kind.DEPOSIT),
                this::depositMoney,
                "Deposit money into account");

        myRouter.register(
                Action.PATCH,
                new RequestRouter.RequestBodyMatcher<>(BankAccountRequest.class, "kind", BankAccountRequest.Kind.WITHDRAW),
                this::withdrawMoney,
                "Withdraw money from account");

        OperationProcessingChain opProcessingChain = new OperationProcessingChain(this);
        opProcessingChain.add(myRouter);
        setOperationProcessingChain(opProcessingChain);

        return opProcessingChain;
    }

    void depositMoney(Operation patch) {
        BankAccountState current = getState(patch);
        BankAccountRequest req = patch.getBody(BankAccountRequest.class);

        if (req.amount < 0) {
            patch.fail(new IllegalArgumentException("can't deposit negative money"));
            return;
        }

        current.balance += req.amount;

        setState(patch, current);
        patch.setBody(current);
        patch.complete();
    }

    void withdrawMoney(Operation patch) {
        BankAccountState current = getState(patch);
        BankAccountRequest req = patch.getBody(BankAccountRequest.class);

        if (req.amount > current.balance) {
            patch.fail(new IllegalArgumentException("not enough funds to withdraw"));
            return;
        }

        current.balance -= req.amount;

        setState(patch, current);
        patch.setBody(current);
        patch.complete();
    }

    private void validateState(Operation start) {
        if (!start.hasBody()) {
            throw new IllegalArgumentException("attempt to initialize service with an empty state");
        }

        BankAccountState state = start.getBody(BankAccountState.class);
        if (state.balance < 0) {
            throw new IllegalArgumentException("balance cannot be negative");
        }
    }
}
