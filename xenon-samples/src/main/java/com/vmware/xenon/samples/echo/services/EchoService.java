package com.vmware.xenon.samples.echo.services;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;

/**
 * Created by icarrero on 10/1/15.
 */
public class EchoService extends StatefulService {
    public static final String SELF_LINK = "/echo";

    public static class EchoServiceState extends ServiceDocument {
        public String message;
    }

    public EchoService() {
        super(EchoServiceState.class);
    }


    @Override
    public void handlePost(Operation post) {
        handlePut(post);
    }

    @Override
    public void handlePut(Operation put) {
        EchoServiceState msg = put.getBody(EchoServiceState.class);
        msg.message = "echo: " + msg.message;
        put.setBodyNoCloning(msg).complete();
    }
}
