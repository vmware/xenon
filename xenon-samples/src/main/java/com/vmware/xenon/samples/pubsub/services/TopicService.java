package com.vmware.xenon.samples.pubsub.services;

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;

public class TopicService extends StatefulService {
    public static class TopicState extends ServiceDocument {
        public String name;
    }

    public TopicService() {
        super(TopicState.class);
    }
}
