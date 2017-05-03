/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.workshop;

import java.util.Map;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;

/**
 * StatelessService demo
 */
public class StatelessDemo {

    /**
     * Simple StatelessService
     */
    public static class MyStatelessService extends StatelessService {
        public static final String SELF_LINK = "/hello";

        // curl localhost:8000/hello
        @Override
        public void handleGet(Operation get) {
            get.setBody("HELLO");
            get.complete();
        }

        // curl localhost:8000/hello -X POST -H "Content-Type: application/json" -d '{"name":"foo"}'
        @Override
        public void handlePost(Operation post) {
            // bind to NON service document object.
            MyDomain domain = post.getBody(MyDomain.class);
            System.out.println("name=" + domain.name);
            post.complete();
        }
    }

    public static class MyDomain {
        public String name;
    }

    /**
     * StatelessService with URI_NAMESPACE_OWNER
     */
    public static class MyNamespaceService extends StatelessService {
        public static final String SELF_LINK = "/offices";

        public MyNamespaceService() {
            super();
            this.toggleOption(ServiceOption.URI_NAMESPACE_OWNER, true);
        }

        // curl localhost:8000/offices/us/paloalto
        @Override
        public void handleGet(Operation get) {
            String template = "/offices/{location}/{site}";
            Map<String, String> params = UriUtils.parseUriPathSegments(get.getUri(), template);
            get.setBody(params);
            get.complete();
        }
    }

    public static void main(String[] args) throws Throwable {
        ServiceHost host = ServiceHost.create();
        host.start();
        host.startDefaultCoreServicesSynchronously();
        host.startService(new MyStatelessService());
        host.startService(new MyNamespaceService());
    }

    private StatelessDemo() {
    }
}
