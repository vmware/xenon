/*
 * Copyright (c) 2014-2015 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.gateway.hosts;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.gateway.GatewayConfigService;
import com.vmware.xenon.gateway.GatewayPathFactoryService;

/**
 * This class represents the gateway application and
 * is responsible for starting and stopping both Gateway
 * hosts (Config and Dispatch).
 */
public class GatewayHost {

    public static final int DEFAULT_DISPATCH_PORT = 8080;

    /**
     * Arguments used by the Gateway Host.
     *
     * Notice that we inherit from
     * {@link com.vmware.xenon.common.ServiceHost.Arguments}.
     * This gives us all the default service host arguments
     * that are reserved for the Config host.
     *
     * For the dispatch host, we define separate arguments.
     */
    public static class Arguments extends ServiceHost.Arguments {
        /**
         * Port used for the Dispatch Host.
         */
        public int dispatchPort = DEFAULT_DISPATCH_PORT;

        /**
         * Network interface address to bind to for the Dispatch Host.
         */
        public String dispatchBindAddress = ServiceHost.DEFAULT_BIND_ADDRESS;

        /**
         * A stable identity associated with the Dispatch host
         */
        public String dispatchId;

        /**
         * The maintenance interval used for the Dispatch and Configuration hosts.
         * Used to override default maintenance interval in unit-tests.
         */
        public Long maintenanceIntervalMicros;
    }

    protected GatewayConfigHost configHost;
    protected GatewayDispatchHost dispatchHost;

    public static void main(String[] args) throws Throwable {
        Arguments appArgs = new Arguments();
        CommandLineArgumentParser.parse(appArgs, args);

        GatewayHost host = new GatewayHost();
        host.start(appArgs, null);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            host.stop();
        }));
    }

    public void start(Arguments args, Consumer<Throwable> completionCallback) throws Throwable {
        // Starting Gateway Configuration host
        if (this.configHost == null) {
            this.configHost = new GatewayConfigHost();
            this.configHost.initialize(args);
            if (args.maintenanceIntervalMicros != null) {
                this.configHost.setMaintenanceIntervalMicros(args.maintenanceIntervalMicros);
            }
            this.configHost.start();
            this.configHost.log(Level.INFO, "GatewayConfigHost started ...");
        }

        // We will start the Dispatch host, once the Configuration
        // factories are available. This ensures that, when the Dispatch
        // host is available, it has all configuration information available.
        String[] factories = new String[] {
                GatewayConfigService.FACTORY_LINK,
                GatewayPathFactoryService.SELF_LINK
        };
        AtomicInteger count = new AtomicInteger(factories.length);
        this.configHost.registerForServiceAvailability((o, e) -> {
            if (count.decrementAndGet() == 0) {
                startDispatchHost(args, completionCallback);
            }
        }, true, factories);
    }

    protected void startDispatchHost(Arguments args, Consumer<Throwable> completionCallback) {
        // Starting Gateway Dispatch host
        if (this.dispatchHost == null) {
            ServiceHost.Arguments defaultArgs = new ServiceHost.Arguments();
            defaultArgs.port = args.dispatchPort;
            defaultArgs.bindAddress = args.dispatchBindAddress;
            defaultArgs.id = args.dispatchId;

            try {
                this.dispatchHost = GatewayDispatchHost.create(this.configHost.getUri());
                this.dispatchHost.initialize(defaultArgs);
                if (args.maintenanceIntervalMicros != null) {
                    this.dispatchHost.setMaintenanceIntervalMicros(args.maintenanceIntervalMicros);
                }
                this.dispatchHost.start();
                this.dispatchHost.log(Level.INFO, "Dispatch host is started");
            } catch (Throwable t) {
                this.configHost.log(Level.SEVERE,
                        "Failed to start the Dispatch Host. Error: %s", t.toString());
                if (completionCallback != null) {
                    completionCallback.accept(t);
                }
            }
        }
        if (completionCallback != null) {
            completionCallback.accept(null);
        }
    }

    public void stop() {
        if (this.dispatchHost != null) {
            this.dispatchHost.stop();
            this.dispatchHost = null;
        }
        if (this.configHost != null) {
            this.configHost.stop();
            this.configHost = null;
        }
    }
}
