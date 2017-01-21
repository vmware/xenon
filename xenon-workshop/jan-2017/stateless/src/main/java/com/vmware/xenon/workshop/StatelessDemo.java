package com.vmware.xenon.workshop;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;

import java.util.Map;

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

    public static class MyServiceHost extends ServiceHost {
        @Override
        public ServiceHost start() throws Throwable {
            super.start();
            this.startService(new MyStatelessService());
            this.startService(new MyNamespaceService());
            return this;
        }
    }

    public static void main(String[] args) throws Throwable {
        ServiceHost host = new MyServiceHost();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            host.stop();
        }));
        host.initialize(args);
        host.start();
    }

}
