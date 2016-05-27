/*
 * Copyright (c) 2015-2016 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.swagger;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.RequestRouter.Route;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.StatelessService;

public class TokenService extends StatelessService {
    public static final String SELF_LINK = "/tokens";

    static class Token {
        public String token;
    }

    static class UserToken {
        public String user;
        public String token;
    }


    @Override
    public void handleGet(Operation op) {
        Token response = new Token();
        response.token = UUID.randomUUID().toString();
        op.setBody(response).complete();
    }

    @Override
    public void handlePost(Operation post) {
        UserToken body = post.getBody(UserToken.class);
        log(Level.INFO, "user:%s token:%s", body.user, body.token);
    }

    @Override
    public ServiceDocument getDocumentTemplate() {
        ServiceDocument d = super.getDocumentTemplate();
        d.documentDescription = new ServiceDocumentDescription();

        d.documentDescription.serviceRequestRoutes = new HashMap<>();

        Route route = new Route();
        route.action = Action.GET;
        route.description = "Retrieves a random token";
        route.responseType = Token.class;

        d.documentDescription.serviceRequestRoutes
                .put(route.action, Collections.singletonList(route));

        route = new Route();
        route.action = Action.POST;
        route.description = "Creates user-token mapping";
        route.requestType = UserToken.class;

        d.documentDescription.serviceRequestRoutes
                .put(route.action, Collections.singletonList(route));
        return d;
    }
}