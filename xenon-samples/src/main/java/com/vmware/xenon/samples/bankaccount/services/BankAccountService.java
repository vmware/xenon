package com.vmware.xenon.samples.bankaccount.services;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.RequestRouter;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;

/**
 * Created by icarrero on 10/4/15.
 */
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
    public RequestRouter getRequestRouter() {
        if (super.getRequestRouter() != null) {
            return super.getRequestRouter();
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

        setRequestRouter(myRouter);

        return myRouter;
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
