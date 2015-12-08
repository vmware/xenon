package com.vmware.xenon.samples.pubsub;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.samples.pubsub.services.SubscriberService;
import com.vmware.xenon.samples.pubsub.services.TopicServiceFactory;

import java.util.logging.Level;

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

        startService(Operation.createPost(UriUtils.buildUri(this, TopicServiceFactory.class)), new TopicServiceFactory());
        startService(Operation.createPost(UriUtils.buildUri(this, SubscriberService.class)), new SubscriberService());

        return this;
    }
}