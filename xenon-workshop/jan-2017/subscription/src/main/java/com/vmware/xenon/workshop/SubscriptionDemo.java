package com.vmware.xenon.workshop;

import com.vmware.xenon.common.*;
import com.vmware.xenon.services.common.RootNamespaceService;

import java.util.function.Consumer;

/**
 * Demonstrate subscription.
 *
 * To list registered subscriptions:
 *   > curl localhost:8000/me/subscriptions
 *
 * Create a new document:
 *   > curl localhost:8000/me -X POST -H "content-type: application/json" -d "{}"
 *
 */
public class SubscriptionDemo {

    public static class MySubscriptionService extends StatefulService {

        public static final String FACTORY_LINK = "/me";

        public static FactoryService createFactory() {
            return FactoryService.create(MySubscriptionService.class);
        }

        public MySubscriptionService() {
            super(ServiceDocument.class);
        }

    }

    public static class MySubscriptionHost extends ServiceHost {
        @Override
        public ServiceHost start() throws Throwable {
            super.start();

            startDefaultCoreServicesSynchronously();

            // Start the root namespace factory: this will respond to the root URI (/) and list all
            // the factory services.
            this.startService(new RootNamespaceService());

            this.startFactory(MySubscriptionService.class, MySubscriptionService::createFactory);

            // subscription callback
            Consumer<Operation> target = (notifyOp) -> {
                System.out.println("notification: " + notifyOp);
                notifyOp.complete();
            };

            Operation createSub = Operation.createPost(
                    UriUtils.buildUri(this, MySubscriptionService.FACTORY_LINK))
                    .setReferer(this.getUri());


            // start subscription when factory became available
            this.registerForServiceAvailability((o, e) -> {
                this.startSubscriptionService(createSub, target);
//                this.startReliableSubscriptionService(createSub, target);

            }, MySubscriptionService.FACTORY_LINK);
            return this;
        }
    }

    public static void main(String[] args) throws Throwable {
        MySubscriptionHost host = new MySubscriptionHost();
        Runtime.getRuntime().addShutdownHook(new Thread(host::stop));
        host.initialize(args);
        host.start();
    }
}
