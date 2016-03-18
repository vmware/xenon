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

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import io.swagger.annotations.ApiOperation;

import com.vmware.xenon.swagger.CarService.Car;

/**
 */
@Path("/{id}")
public interface CarServiceDescriptor {

    @GET
    @ApiOperation("retrieves a car's representation in json")
    Car get(@PathParam("id") String id);


    @POST
    void post(@PathParam("id") String id, Car car);

    @PUT
    @ApiOperation("replace a car")
    void put(@PathParam("id") String id, Car car);
}
