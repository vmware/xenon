package com.vmware.xenon.workshop;

import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.services.common.RootNamespaceService;

import java.nio.file.Paths;

public class DemoHost extends ServiceHost {

    public static void main(String[] stringArgs) throws Throwable {
        ServiceHost.Arguments defaultArgs = new ServiceHost.Arguments();
        defaultArgs.sandbox = Paths.get("/tmp/xenondb");

        startHost(stringArgs, defaultArgs);
    }

    static void startHost(String[] stringArgs, ServiceHost.Arguments defaultArgs) throws Throwable {
        DemoHost host = new DemoHost();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            host.stop();
        }));
        host.initialize(stringArgs, defaultArgs);
        host.start();
    }

    @Override
    public ServiceHost start() throws Throwable {
        super.start();
        startDefaultCoreServicesSynchronously();

        // A stateless service that enumerates all the
        // factory services started on the Service host.
        this.startService(new RootNamespaceService());

        // Starting the employee factory service.
        EmployeeFactoryService factoryService = new EmployeeFactoryService();
        this.startService(factoryService);

//        // Starting the employee factory service.
//        this.startFactory(new EmployeeService());

        return this;
    }
}
