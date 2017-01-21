package com.vmware.xenon.workshop;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Service;

public class EmployeeFactoryService extends FactoryService {

    public static final String SELF_LINK = "/sample/employees";

    public EmployeeFactoryService() {
        super(EmployeeService.Employee.class);
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return new EmployeeService();
    }
}
