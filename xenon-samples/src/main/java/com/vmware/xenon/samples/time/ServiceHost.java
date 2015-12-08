package com.vmware.xenon.samples.time;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.samples.time.services.TimeService;

import java.util.logging.Level;

/**
 * Created by icarrero on 10/1/15.
 */
public class ServiceHost extends com.vmware.xenon.common.ServiceHost {

    public static void main(String[] args) throws Throwable {
        ServiceHost h = new ServiceHost();
        h.initialize(args);
        h.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            h.log(Level.WARNING, "Host stopping ...");
            h.stop();
            h.log(Level.WARNING, "Host is stopped");
        }));
    }

    @Override
    public com.vmware.xenon.common.ServiceHost start() throws Throwable {
        super.start();
        startDefaultCoreServicesSynchronously();
        startService(Operation.createPost(UriUtils.buildUri(this, TimeService.class)), new TimeService());
        return this;
    }
}
