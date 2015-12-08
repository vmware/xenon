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

package com.vmware.xenon.samples.pubsub.services;

import java.util.function.Consumer;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;

public class SubscriberService extends StatelessService {

    public static final String SELF_LINK = "/subscriber";

    @Override
    public void handleStart(Operation startPost) {
        // creating a topic to monitor for this run
        createTopic(startPost, this::subscribeToTopicUpdates);
    }

    private void createTopic(Operation op, TopicSubscriptionListener subscription) {
        TopicService.TopicState topic = new TopicService.TopicState();
        topic.name = "A sample topic";

        Operation createOp = Operation.createPost(UriUtils.buildUri(getHost(), TopicServiceFactory.class));
        createOp.setBody(topic);
        createOp.setCompletion((o, e) -> {
            if (e != null) {
                logSevere(e);
                // ... and we're done ...
                op.complete();
                return;
            }

            TopicService.TopicState createdTopic = o.getBody(TopicService.TopicState.class);
            subscription.apply(op, createdTopic);
        });

        sendRequest(createOp);
    }

    private void subscribeToTopicUpdates(Operation op, TopicService.TopicState topic) {
        logInfo("created topic %s@%s", topic.name, topic.documentSelfLink);

        Consumer<Operation> cons = operation -> {
            // The end goal of this example.
            // When we get here we're receiving notifications from
            // state modifications in the topic we're interested in.
            logInfo("%s %s", operation.getAction().name(), operation.getUri());
            logInfo("%s", operation.getBodyRaw());
        };

        Operation subscribeOp = Operation.createPost(UriUtils.buildSubscriptionUri(getHost(), topic.documentSelfLink));
        subscribeOp.setReferer(getUri());
        subscribeOp.setCompletion((o, e) -> {
            if (e != null) {
                logSevere(e);
                // ... and we're done ...
                op.complete();
                return;
            }

            // ... and we're done ...
            op.complete();
        });

        getHost().startSubscriptionService(subscribeOp, cons);

    }

    @FunctionalInterface
    interface TopicSubscriptionListener {
        void apply(Operation op, TopicService.TopicState topic);
    }


}
