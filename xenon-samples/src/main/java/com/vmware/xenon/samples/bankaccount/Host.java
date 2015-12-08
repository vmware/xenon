package com.vmware.xenon.samples.bankaccount;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.samples.bankaccount.services.BankAccountFactoryService;

import java.util.logging.Level;

/**
 * Created by icarrero on 10/4/15.
 */
public class Host extends ServiceHost {

    public static void main(String[] args) throws Throwable {
        Host h = new Host();
        h.initialize(args);
        h.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            h.log(Level.WARNING, "Host stopping ...");
            h.stop();
            h.log(Level.WARNING, "Host is stopped");
        }));
    }

    @Override
    public ServiceHost start() throws Throwable {
        super.start();
        startDefaultCoreServicesSynchronously();
        startService(Operation.createPost(UriUtils.buildUri(this, BankAccountFactoryService.class)), new BankAccountFactoryService());
        return this;
    }

}
