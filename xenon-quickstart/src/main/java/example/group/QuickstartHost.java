/*
 * Copyright (c) 2016 VMware, Inc. All Rights Reserved.
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

package example.group;

import java.nio.file.Paths;

import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.services.common.RootNamespaceService;

/**
 * Stand alone process entry point
 */
public class QuickstartHost extends ServiceHost {
    /**
     * Launch a complete xenon-based application. For production instances,
     *
     * When starting a true Xenon cluster on multiple different nodes, start this application on each node.
     * Ideally at least three nodes. Pass in "--peerNodes" command line parameter that specifies a comma-separated list
     * of URIs of all the nodes that will be eventually part of the cluster.
     *
     * @param args - command line arguments that can be specified on the command line used to start the JVM.
     *             @see com.vmware.xenon.common.ServiceHost.Arguments for descrtion of command line parameters.
     * @throws Throwable - a failure while starting up Xenon
     */
    public static void main(String[] args) throws Throwable {
        Arguments defaultArgs = new Arguments();
        defaultArgs.port = 8000;
        defaultArgs.id = "host" + defaultArgs.port;   // human readable name instead of GUID
        defaultArgs.sandbox = Paths.get("/tmp/xenondb");
        startHost(args, defaultArgs);
    }

    /**
     * Construct and launch a single host, with the specified properties. Also registers a shutdown hook with the
     * JVM to call the host stop() method on shutdown.
     * @param stringArgs - string that will be parsed to get additional arguments. The stringArgs will override
     *                   @see com.vmware.xenon.common.ServiceHost.Arguments for descrtion of command line parameters.
     * @param args - structure holding list of arguments the specified args.
     * @return newly launched host.
     * @throws Throwable - some sort of failure during startup
     */
    static QuickstartHost startHost(String[] stringArgs, Arguments args) throws Throwable {
        QuickstartHost h = new QuickstartHost();
        h.initialize(stringArgs, args);
        h.start();
        Runtime.getRuntime().addShutdownHook(new Thread(h::stop));
        return h;
    }

    /**
     * Launch all the desired Xenon services here. This method is called when Xenon is starting.
     * If you add new services, be sure to start them here - using either startService (for stand-alone services) or
     * startFactory (for factories)
     *
     * @return host
     * @throws Throwable - a failure while starting up Xenon services
     */
    @Override
    public ServiceHost start() throws Throwable {
        super.start();

        startDefaultCoreServicesSynchronously();

        // Stateless services can be launched with startService().
        // URL is defined by the static variable SELF_LINK of class passed in.
        super.startService(new RootNamespaceService());

        // You want a distinct factory to manage all stateful services (documents) of the same kind.
        // Then through the factory you can create new services dynamically, get lists of running services.
        // Thus you will need a line like this for every stateful service that you create.
        // The URL of the factory is defined by the static variable FACTORY_LINK in the StatefulService class.
        // All services of this type are created under the FACTORY_LINK URL
        super.startFactory(new EmployeeService());

        return this;
    }

}
