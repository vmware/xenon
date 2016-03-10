package com.vmware.xenon.swagger;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;

/**
 */
public class CarService extends StatefulService {
    public static final String FACTORY_LINK = "/cars";

    public static Service createFactory() {
        return FactoryService.create(CarService.class, Car.class);
    }

    public enum FuelType {
        GASOLINE, DIESEL, ELECTRIC
    }

    public static class EngineInfo {
        public FuelType fuel;
        public double power;
    }
    public static class Car extends ServiceDocument {
        public String make;
        public String licensePlate;
        public EngineInfo engineInfo;
    }

    public CarService() {
        super(Car.class);
    }



    @Override
    @ApiOperation("overwrites a car")
    @ApiResponse(code = 200, message= "Success", response = Car.class)
    public void handlePut(Operation put) {
        this.setState(put, put.getBody(Car.class));
        put.complete();
    }
}
