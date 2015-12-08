package com.vmware.xenon.samples.pubsub.services;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;

import java.util.function.Consumer;

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
