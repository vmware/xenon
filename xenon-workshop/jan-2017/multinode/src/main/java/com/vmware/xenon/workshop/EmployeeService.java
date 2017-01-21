package com.vmware.xenon.workshop;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;

public class EmployeeService extends StatefulService {
    public static final String FACTORY_LINK = "/sample/employees";

    public static class Employee extends ServiceDocument {
        public String name;
        public String managerName;
    }

    public EmployeeService() {
        super(Employee.class);

        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
        toggleOption(ServiceOption.REPLICATION, true);
    }

    /**
     * handleCreate() is called when a service is first created. This happens on POST to the factory.
     * When xenon is shut down and restarted, this method will NOT be called. If you want a method that will be called
     * whenever a service is started (both on creation and when the service is restarted), then override
     * handleStart(). Note that we only need to overide handleCreate() because we want to do some verification on
     * the proposed document body before we persist it. If you don't want to validate any inputs, you can
     * exclude this method.
     * @param startPost - use this to fetch the state and body as necessary and call complete() when done
     */
    @Override
    public void handleCreate(Operation startPost) {
        Employee s = getBody(startPost);

        if (s == null) {
            startPost.fail(new IllegalArgumentException("missing body"));
            return;
        }

        if (s.name == null || s.name.isEmpty()) {
            startPost.fail(new IllegalArgumentException("name cannot be null"));
            return;
        }

        // When you call complete(), not only will the object be created, but the REST POST operation will
        // return to the sender.
        startPost.complete();

        // You can still do processing after the complete - very common in an async world.
        // You can also not call complete() this function but rather call complete() at some later time. The REST
        // caller will be blocked until complete() is called.
    }

    /**
     * With PUT, the old version is discarded and replaced with the new complete object.
     * PUT *cannot* be used to create services - it can only be used to update an already running service.
     * @param put - use this to fetch the state and body as necessary and call complete() when done
     */
    @Override
    public void handlePut(Operation put) {
        Employee newState = getBody(put);
        Employee currentState = getState(put);

        if (newState == null) {
            put.fail(new IllegalArgumentException("missing body"));
            return;
        }

        if (newState.name == null) {
            put.fail(new IllegalArgumentException("employee name cannot be set to null"));
            return;
        }

        if (newState.name != null && !newState.name.equals(currentState.name)) {
            put.fail(new IllegalArgumentException("employee name cannot be changed"));
            return;
        }

        setState(put, newState);
        put.complete();
    }

    /**
     * The HTTP PATCH is used to update a subset of fields on the service/document. The PATCH request specifies
     * just the fields that need to be updated. The rest of the fields are left unchanged.
     * @param patch - use this to fetch the state and body as necessary and call complete() when done
     */
    @Override
    public void handlePatch(Operation patch) {
        Employee newState = getBody(patch);
        Employee currentState = getState(patch);

        if (newState == null) {
            patch.fail(new IllegalArgumentException("missing body"));
            return;
        }

        if (newState.name != null) {
            patch.fail(new IllegalArgumentException("employee name cannot be changed in a PATCH"));
            return;
        }

        if (newState.managerName != null) {
            currentState.managerName = newState.managerName;
        }

        patch.setBody(currentState);
        patch.complete();
    }
}
