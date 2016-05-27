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

package com.vmware.xenon.swagger;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.RequestRouter.Route;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;

/**
 */
public class CarService extends StatefulService {
    public static final String FACTORY_LINK = "/cars";

    public CarService() {
        super(Car.class);
    }

    public static Service createFactory() {
        return FactoryService.create(CarService.class, Car.class);
    }

    public void handlePut(Operation put) {
        this.setState(put, put.getBody(Car.class));
        put.complete();
    }

    public enum FuelType {
        GASOLINE, DIESEL, ELECTRICITY
    }

    public static class EngineInfo {
        public FuelType fuel;
        public double power;
    }

    public static class Car extends ServiceDocument {
        public URI manufacturerHomePage;
        public double length;
        public double weight;
        @Documentation(description = "Make of the car", exampleString = "BMW")
        public String make;
        @Documentation(description = "License plate of the car", exampleString = "XXXAAAA")
        public String licensePlate;
        public EngineInfo engineInfo;
    }

    @Override
    public ServiceDocument getDocumentTemplate() {
        ServiceDocument d = super.getDocumentTemplate();
        d.documentDescription.serviceRequestRoutes = new HashMap<>();

        Route route = new Route();
        route.action = Action.PUT;
        route.description = "Updates car properties";
        route.requestType = Car.class;

        d.documentDescription.serviceRequestRoutes
                .put(route.action, Collections.singletonList(route));
        return d;
    }
}
