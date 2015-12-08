package com.vmware.xenon.samples.pubsub.services;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Service;

public class TopicServiceFactory extends FactoryService {

    public static final String SELF_LINK = "/topics";

    public TopicServiceFactory() {
        super(TopicService.TopicState.class);
    }


    @Override
    public Service createServiceInstance() throws Throwable {
        return new TopicService();
    }
}
