package com.vmware.xenon.samples.todolist.services;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;

/**
 * Created by icarrero on 10/3/15.
 */
public class TodoEntryServiceFactory extends FactoryService {
    public static final String SELF_LINK = "/todo";

    public TodoEntryServiceFactory() {
        super(TodoEntryService.TodoEntry.class);
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return new TodoEntryService();
    }
}
