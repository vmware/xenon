/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.RequestRouter.Route;
import com.vmware.xenon.common.RequestRouter.Route.RouteDocumentation;
import com.vmware.xenon.common.RequestRouter.Route.RouteDocumentation.PathParam;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.ServiceConfigUpdateRequest;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.NodeGroupService.JoinPeerRequest;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class TestServiceDocumentDescription {

    public static class NsOwner extends StatelessService {
        public static final String SELF_LINK = "/ns-owner";

        public NsOwner() {
            toggleOption(ServiceOption.URI_NAMESPACE_OWNER, true);
        }

        @RouteDocumentation(
                path = "/users/{user}",
                pathParams = {
                        @PathParam(name = "user", description = "The user id")
                })
        @RouteDocumentation(
                path = "/tenants/{tenant}",
                pathParams = {
                        @PathParam(name = "tenant", description = "The user id")
                })
        @Override
        public void handleGet(Operation get) {
            get.fail(new UnsupportedOperationException("not implemented"));
        }

        @RouteDocumentation(requestBodyType = ServiceConfigUpdateRequest.class, description = "just test that "
                + "requestBodyType is propagated")
        @Override
        public void handlePatch(Operation patch) {
            patch.fail(new UnsupportedOperationException("not implemented"));
        }
    }

    private VerificationHost host;

    @Before
    public void setup() throws Throwable {
        this.host = VerificationHost.create(0);
        this.host.start();

        this.host.startServiceAndWait(new NsOwner(), NsOwner.SELF_LINK, new ServiceDocument());
    }

    private static Route failNotFound() {
        fail("No such route found");
        return null;
    }

    @Test
    public void testRoutes() throws InterruptedException {
        this.host.waitForServiceAvailable(ServiceUriPaths.NODE_GROUP_FACTORY);

        Operation get = Operation.createGet(UriUtils.buildUri(this.host,
                NsOwner.SELF_LINK + ServiceHost.SERVICE_URI_SUFFIX_TEMPLATE));

        Operation resp = this.host.waitForResponse(get);
        ServiceDocument body = resp.getBody(ServiceDocument.class);
        ServiceDocumentDescription desc = body.documentDescription;

        // there is a handler for  DELETE + 1 annotated for GET + 1 PATCH
        assertEquals(3, desc.serviceRequestRoutes.size());

        List<Route> getRoutes = desc.serviceRequestRoutes.get(Action.GET);
        assertEquals(2, getRoutes.size());

        Route users = getRoutes.stream().filter(r -> r.path.equals("/users/{user}")).findFirst()
                .orElseGet(TestServiceDocumentDescription::failNotFound);

        assertEquals(1, users.parameters.size());
        assertEquals("user", users.parameters.get(0).name);

        Route tenants = getRoutes.stream().filter(r -> r.path.equals("/tenants/{tenant}")).findFirst()
                .orElseGet(TestServiceDocumentDescription::failNotFound);

        assertEquals(1, tenants.parameters.size());
        assertEquals("tenant", tenants.parameters.get(0).name);

        List<Route> patchRoutes = desc.serviceRequestRoutes.get(Action.PATCH);
        assertEquals(1, patchRoutes.size());
        Route patch = patchRoutes.get(0);
        assertEquals(ServiceConfigUpdateRequest.class, patch.requestType);
    }

    @Test
    public void testNodeGroupService() {
        Operation get = Operation.createGet(UriUtils.buildUri(this.host,
                ServiceUriPaths.NODE_GROUP_FACTORY + ServiceHost.SERVICE_URI_SUFFIX_TEMPLATE));

        Operation resp = this.host.waitForResponse(get);
        ServiceDocumentQueryResult body = resp.getBody(ServiceDocumentQueryResult.class);
        ServiceDocumentDescription desc = Utils.fromJson(body.documents.values().iterator().next().toString(),
                ServiceDocument.class).documentDescription;

        List<Route> postRoutes = desc.serviceRequestRoutes.get(Action.POST);
        assertEquals(1, postRoutes.size());
        Route post = postRoutes.get(0);
        assertEquals(JoinPeerRequest.class, post.requestType);
    }
}
