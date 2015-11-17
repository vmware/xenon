/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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

package com.vmware.dcp.performance;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import com.sun.jdi.connect.spi.TransportService;
import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.Service;
import com.vmware.dcp.common.ServiceHost;
import com.vmware.dcp.common.UriUtils;
import com.vmware.dcp.services.common.ExampleFactoryService;
import com.vmware.dcp.services.common.RootNamespaceService;
import com.vmware.dcp.services.samples.SampleSimpleEchoFactoryService;

/**
 * Host a number of services targeting performance benchrmaks
 */
public class PerformanceHost extends ServiceHost {

    public static void main(String[] args) throws Throwable {
        PerformanceHost h = new PerformanceHost();
        h.initialize(args);
        h.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            h.log(Level.WARNING, "Host stopping ...");
            h.stop();
            h.log(Level.WARNING, "Host is stopped");
        }));
    }

    /**
     * Currently: Start a continuum of example services
     * TODO -- test UI rendering
     */
    @Override
    public ServiceHost start() throws Throwable {
        super.start();

        startDefaultCoreServicesSynchronously();

        super.startService(
                Operation.createPost(UriUtils.buildUri(this, RootNamespaceService.class)),
                new RootNamespaceService()
        );

        super.startService(
                Operation.createPost(UriUtils.buildUri(this,
                        SimpleStatefulService.SimpleStatefulFactoryService.class)),
                new SimpleStatefulService.SimpleStatefulFactoryService(PerfUtils.SimpleState.class));
        
        super.startService(
                Operation.createPost(UriUtils.buildUri(this,
                        SimpleStatefulService.SimpleStatefulFactoryService.class)),
                new SimpleStatefulService.SimpleStatefulFactoryService(PerfUtils.SimpleState.class));

        super.startService(
                Operation.createPost(UriUtils.buildUri(this,
                        PersistenceService.PersistenceFactoryService.class)),
                new PersistenceService.PersistenceFactoryService(PerfUtils.SimpleState.class));

        super.startService(
                Operation.createPost(UriUtils.buildUri(this,
                        ReplicationService.ReplicationFactoryService.class)),
                new ReplicationService.ReplicationFactoryService(PerfUtils.SimpleState.class));

        super.startService(
                Operation.createPost(UriUtils.buildUri(this,
                        OwnerSelectionService.OwnerSelectionFactoryService.class)),
                new OwnerSelectionService.OwnerSelectionFactoryService(PerfUtils.SimpleState.class));

        super.startService(
                Operation.createPost(UriUtils.buildUri(this,
                        ConsistencyService.ConsistencyFactoryService.class)),
                new ConsistencyService.ConsistencyFactoryService(PerfUtils.SimpleState.class));

        super.startService(
                Operation.createPost(UriUtils.buildUri(this,
                        FullCapService.FullCapFactoryService.class)),
                FullCapService.FullCapFactoryService.create(PerfUtils.SimpleState.class));

        super.startService(
                Operation.createPost(UriUtils.buildUri(this, ExampleFactoryService.class)),
                new ExampleFactoryService());

        return this;
    }
}
