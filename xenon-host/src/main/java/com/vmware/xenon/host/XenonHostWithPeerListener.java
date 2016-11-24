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

package com.vmware.xenon.host;

import java.util.logging.Level;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.http.netty.NettyHttpListener;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.RootNamespaceService;
import com.vmware.xenon.ui.UiService;

/**
 * Stand alone process entry point with a dedicated listener for the p2p traffic
 */
public class XenonHostWithPeerListener extends ServiceHost {

    private PeerArgs hostArgs;

    /**
     * Hold configuration for the peer listener
     */
    public static class PeerArgs extends Arguments {
        public int peerPort;
        public String peerBindAddress;
        // you may also configure SSL context
    }

    /**
     * Must be started with sth like "--peerPort=9000 --peerBindAddress=10.0.1.3 --publicUri=http://10.0.1.3:9000"
     * The --publicUri MUST be set otherwise the nodes will try to talk to this host on the --port and --bindAddress.
     * @param args
     * @throws Throwable
     */
    public static void main(String[] args) throws Throwable {
        XenonHostWithPeerListener h = new XenonHostWithPeerListener();
        PeerArgs extArgs = new PeerArgs();

        h.initializeWithPeerArgs(args, extArgs);

        h.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            h.log(Level.WARNING, "Host stopping ...");
            h.stop();
            h.log(Level.WARNING, "Host is stopped");
        }));
    }

    @Override
    public ServiceHost start() throws Throwable {
        super.start();

        startDefaultCoreServicesSynchronously();

        startPeerListener();

        setAuthorizationContext(this.getSystemAuthorizationContext());

        super.startService(new RootNamespaceService());

        // start an example factory for folks that want to experiment with service instances
        super.startFactory(ExampleService.class, ExampleService::createFactory);

        // Start UI service
        super.startService(new UiService());

        setAuthorizationContext(null);

        return this;
    }

    private void startPeerListener() throws Throwable {
        NettyHttpListener peerListener = new NettyHttpListener(this);
        peerListener.start(this.hostArgs.peerPort, this.hostArgs.peerBindAddress);
    }

    protected ServiceHost initializeWithPeerArgs(String[] args, PeerArgs hostArgs) throws Throwable {
        this.hostArgs = hostArgs;

        CommandLineArgumentParser.parse(hostArgs, args);
        CommandLineArgumentParser.parse(COLOR_LOG_FORMATTER, args);

        validatePeerArgs();

        initialize(hostArgs);
        setProcessOwner(true);
        return this;
    }

    private void validatePeerArgs() {
        if (this.hostArgs.peerPort == this.hostArgs.port) {
            throw new IllegalArgumentException("--peerPort must be different from --port");
        }

        if (this.hostArgs.peerPort < 0 || this.hostArgs.peerPort >= Short.MAX_VALUE * 2) {
            throw new IllegalArgumentException("--peerPort is not in range");
        }

        if (this.hostArgs.peerPort > 0 && this.hostArgs.peerBindAddress == null) {
            throw new IllegalArgumentException("--peerBindAddress is required when --peerPort is set");
        }

        if (this.hostArgs.peerPort > 0 && this.hostArgs.publicUri == null) {
            throw new IllegalArgumentException("--publicUri is required when --peerPort is set");
        }
    }
}
