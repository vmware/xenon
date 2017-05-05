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

package com.vmware.workshop.labs.answer;

import java.util.function.Consumer;
import java.util.logging.Level;

import com.vmware.workshop.labs.BookService;
import com.vmware.workshop.labs.Top10BookRankingService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.RootNamespaceService;

/**
 * Hands on lab answer implementation.
 */
public class BookstoreDemoAnswer {

    public static void main(String[] args) throws Throwable {

        // create adhoc ServiceHost.
        // In real application, you should create own ServiceHost implementation.
        ServiceHost host = ServiceHost.create();
        host.start();
        host.startDefaultCoreServicesSynchronously();
        host.startService(new RootNamespaceService());
        host.startFactory(new BookService());
        host.startService(new Top10BookRankingService());


        // Create a consumer of subscription callbacks

        // subscription callback
        Consumer<Operation> target = (notifyOp) -> {
            host.log(Level.INFO, "notification: %s", notifyOp);
            if (notifyOp.getAction() == Service.Action.POST) {
                BookService.BookState state = notifyOp.getBody(BookService.BookState.class);
                host.log(Level.INFO, "title=%s, sold=%d", state.title, state.sold);
            }
            notifyOp.complete();
        };


        // start subscription when factory became available
        // For reliable subscription, use "startReliableSubscriptionService()" instead of "startSubscriptionService()"
        host.registerForServiceAvailability((o, e) -> {

            // Register subscription to the factory of BookService
            Operation createSub = Operation.createPost(
                    UriUtils.buildUri(host, BookService.FACTORY_LINK)).setReferer(host.getUri());
            host.startSubscriptionService(createSub, target);

        }, BookService.FACTORY_LINK);

    }

    private BookstoreDemoAnswer() {
    }

}
