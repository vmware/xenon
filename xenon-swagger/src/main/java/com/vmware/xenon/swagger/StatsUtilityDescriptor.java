package com.vmware.xenon.swagger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import io.swagger.annotations.ApiOperation;

import com.vmware.xenon.common.ServiceStats;

/**
  */
@Path("/{id}/stats")
public interface StatsUtilityDescriptor {

    @GET
    @ApiOperation("Retrieves stats for a service instance")
    ServiceStats get(@PathParam("id") String id);
}
