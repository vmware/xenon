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

package com.vmware.xenon.services.samples;

import java.util.logging.Level;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.RootNamespaceService;

public class SampleServiceCustomUiHost extends ServiceHost {

    public static void main(String[] args) throws Throwable {
        SampleServiceCustomUiHost h = new SampleServiceCustomUiHost();
        h.initialize(args);
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

        super.startService(
                Operation.createPost(UriUtils.buildUri(this, RootNamespaceService.class)),
                new RootNamespaceService()
        );

        // start the example custom ui factory
        super.startService(
                Operation.createPost(UriUtils.buildUri(this, SampleServiceWithSharedCustomUi
                        .class)), new SampleServiceWithSharedCustomUi());

        // start the example custom ui factory
        super.startService(
                Operation.createPost(
                        UriUtils.buildUri(this, SampleFactoryServiceWithCustomUi.class)),
                new SampleFactoryServiceWithCustomUi());

        return this;
    }
}