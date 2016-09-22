/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common.test;

import java.net.URI;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;

public class TestRemoteHost extends ServiceHost {

    private String remoteUri;
    // use VerificationHost for now to send a request
    private VerificationHost verificationHost;

    public TestRemoteHost(String remoteUri) {
        this.remoteUri = remoteUri;
    }

    @Override
    public ServiceHost initialize(Arguments args) throws Throwable {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public ServiceHost start() throws Throwable {
        this.verificationHost = VerificationHost.create(0);
        this.verificationHost.start();
        return this;
    }

    @Override
    public URI getUri() {
        return URI.create(this.remoteUri);
    }

    @Override
    public void sendRequest(Operation op) {
        op.forceRemote();
        this.verificationHost.sendRequest(op);
    }

    @Override
    public void stop() {
        this.verificationHost.stop();
    }

    public static void main(String[] args) throws Throwable {
        TestRemoteHost host = new TestRemoteHost("http://127.0.0.1:8000");
        host.start();
        TestRequestSender sender = new TestRequestSender(host);


        Operation op = Operation.createGet(host, "/");
        Operation res = sender.sendAndWait(op);

        System.out.println(res.getBodyRaw());

        host.stop();
    }
}
