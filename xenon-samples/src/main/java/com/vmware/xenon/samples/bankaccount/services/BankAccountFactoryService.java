package com.vmware.xenon.samples.bankaccount.services;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Service;

/**
 * Created by icarrero on 10/4/15.
 */
public class BankAccountFactoryService extends FactoryService {

    public static final String SELF_LINK = "/bank-accounts";

    public BankAccountFactoryService() {
        super(BankAccountService.BankAccountState.class);
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return new BankAccountService();
    }
}
