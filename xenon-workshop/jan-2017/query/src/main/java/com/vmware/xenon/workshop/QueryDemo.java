package com.vmware.xenon.workshop;

import java.util.HashMap;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.services.common.ExampleService;

/**
 * StatelessService demo
 */
public class QueryDemo {

    public static class QuerySampleServiceHost extends ServiceHost {
        @Override
        public ServiceHost start() throws Throwable {
            super.start();
            startDefaultCoreServicesSynchronously();
            this.startFactory(ExampleService.class, ExampleService::createFactory);
            return this;
        }
    }

    public static void main(String[] args) throws Throwable {
        ServiceHost host = new QuerySampleServiceHost();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            host.stop();
        }));
        host.initialize(args);
        host.start();

        host.registerForServiceAvailability((o, e) -> {
            ExampleService.ExampleServiceState state = new ExampleService.ExampleServiceState();
            state.counter = 1L;
            state.documentSelfLink = "a";
            state.keyValues = new HashMap<>();
            state.keyValues.put("key1", "value1");
            state.name = "Amanda";
            state.sortedCounter = 1L;

            Operation
                    .createPost(host, ExampleService.FACTORY_LINK)
                    .setBody(state)
                    .setReferer("localhost")
                    .sendWith(host);

            state = new ExampleService.ExampleServiceState();
            state.counter = 10L;
            state.documentSelfLink = "b";
            state.keyValues = new HashMap<>();
            state.keyValues.put("key2", "value1");
            state.name = "Bernard";
            state.sortedCounter = 10L;

            Operation
                    .createPost(host, ExampleService.FACTORY_LINK)
                    .setBody(state)
                    .setReferer("localhost")
                    .sendWith(host);

            state = new ExampleService.ExampleServiceState();
            state.counter = 100L;
            state.documentSelfLink = "c";
            state.keyValues = new HashMap<>();
            state.keyValues.put("key1", "value3");
            state.name = "Commander Adama";
            state.sortedCounter = 100L;

            Operation
                    .createPost(host, ExampleService.FACTORY_LINK)
                    .setBody(state)
                    .setReferer("localhost")
                    .sendWith(host);

        }, ExampleService.FACTORY_LINK);
    }
}
