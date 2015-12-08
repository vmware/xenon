package com.vmware.xenon.samples.todolist;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.samples.todolist.services.TodoEntryServiceFactory;

import java.util.logging.Level;

/**
 * Created by icarrero on 10/3/15.
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
        startService(Operation.createPost(UriUtils.buildUri(this, TodoEntryServiceFactory.class)), new TodoEntryServiceFactory());
        return this;
    }
}
