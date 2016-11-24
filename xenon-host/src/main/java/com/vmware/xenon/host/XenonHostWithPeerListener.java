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

import java.net.URI;
import java.nio.file.Path;
import java.util.logging.Level;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.http.netty.NettyHttpListener;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.RootNamespaceService;
import com.vmware.xenon.ui.UiService;

/**
 * Stand alone process entry point with a dedicated listener for the p2p traffic.
 * If --nodeGroupPublicUri is node configured it behaves like a regular XenonHost.
 * If --nodeGroupPublicUri value starts with https: then a secure listener is started. The certificate of the
 * secure listener defaults to the one used for client-server communication unless
 * --peerKeyFile and --peerCertificateFile are also set.
 *
 * A URL with port=0 is valid for --nodeGroupPublicUri and in this case a random free port will be used.
 */
public class XenonHostWithPeerListener extends ServiceHost {

    private CustomArguments hostArgs;

    /**
     * Hold configuration for the peer listener
     */
    public static class CustomArguments extends Arguments {
        public String nodeGroupPublicUri;
        /**
         * File path to key file(PKCS#8 private key file in PEM format)
         */
        public Path peerKeyFile;

        /**
         * Key passphrase
         */
        public String peerKeyPassphrase;

        /**
         * File path to certificate file
         */
        public Path peerCertificateFile;
    }

    /**
     * Must be started with arguments "--nodeGroupPublicUri=https://10.0.1.3:9001 --publicUri=http://10.0.1.3:9001"
     * If --publicUri is not set but --nodeGroupPublicUri is, then the nodeGroupPublicUri will be used for the publicUri.
     * @param args
     * @throws Throwable
     */
    public static void main(String[] args) throws Throwable {
        XenonHostWithPeerListener h = new XenonHostWithPeerListener();
        CustomArguments extArgs = new CustomArguments();

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

        setAuthorizationContext(this.getSystemAuthorizationContext());

        startService(new RootNamespaceService());

        // start an example factory
        startFactory(ExampleService.class, ExampleService::createFactory);

        // Start UI service
        startService(new UiService());

        setAuthorizationContext(null);

        return this;
    }

    private int startPeerListener() throws Throwable {
        if (this.hostArgs.nodeGroupPublicUri == null) {
            return ServiceHost.PORT_VALUE_LISTENER_DISABLED;
        }

        URI uri = URI.create(this.hostArgs.nodeGroupPublicUri);
        NettyHttpListener peerListener = new NettyHttpListener(this);

        boolean isHttps = uri.getScheme().equals("https");
        if (isHttps) {
            SslContext context;
            if (this.hostArgs.peerCertificateFile != null && this.hostArgs.peerKeyFile != null) {
                context = SslContextBuilder.forServer(
                        this.hostArgs.peerCertificateFile.toFile(),
                        this.hostArgs.peerKeyFile.toFile(),
                        this.hostArgs.peerKeyPassphrase)
                        .build();
            } else {
                context = SslContextBuilder.forServer(
                        this.hostArgs.certificateFile.toFile(),
                        this.hostArgs.keyFile.toFile(),
                        this.hostArgs.keyPassphrase)
                        .build();
            }

            peerListener.setSSLContext(context);
        }

        peerListener.start(uri.getPort(), uri.getHost());
        int assignedPort = peerListener.getPort();
        log(Level.INFO, "Started peer listener on %s",
                UriUtils.buildUri(uri.getScheme(), uri.getHost(), assignedPort, null, null));
        return assignedPort;
    }

    @Override
    public ServiceHost initialize(String[] args) throws Throwable {
        initializeWithPeerArgs(args, new CustomArguments());
        return this;
    }

    protected ServiceHost initializeWithPeerArgs(String[] args, CustomArguments hostArgs) throws Throwable {
        this.hostArgs = hostArgs;

        CommandLineArgumentParser.parse(hostArgs, args);
        CommandLineArgumentParser.parse(COLOR_LOG_FORMATTER, args);

        validatePeerArgs();

        initialize(hostArgs);
        setProcessOwner(true);
        return this;
    }

    private void validatePeerArgs() throws Throwable {
        if (this.hostArgs.nodeGroupPublicUri == null) {
            return;
        }

        URI uri = URI.create(this.hostArgs.nodeGroupPublicUri);

        if (uri.getPort() < 0) {
            throw new IllegalArgumentException(
                    "the port of --nodeGroupPublicUri must be set to a valid port number or 0 to let Xenon pick a free port");
        }

        if (uri.getPort() == this.hostArgs.port && uri.getPort() != 0) {
            throw new IllegalArgumentException("the port of --nodeGroupPublicUri must be different from --port");
        }

        int port = startPeerListener();
        this.hostArgs.nodeGroupPublicUri = UriUtils.buildUri(uri.getScheme(), uri.getHost(), port, null, null)
                .toString();

        if (this.hostArgs.publicUri == null) {
            // save to assume this, a client can always explicitly set the publicUri (when using NAT or a hostname etc.)
            this.hostArgs.publicUri = this.hostArgs.nodeGroupPublicUri;
        }
    }
}
