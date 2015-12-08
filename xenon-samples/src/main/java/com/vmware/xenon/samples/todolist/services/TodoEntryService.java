package com.vmware.xenon.samples.todolist.services;

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;

/**
 * Created by icarrero on 10/3/15.
 */
public class TodoEntryService extends StatefulService {
    public static class TodoEntry extends ServiceDocument {
        public String body;
        public boolean done;
    }

    public TodoEntryService() {
        super(TodoEntry.class);
        toggleOption(ServiceOption.PERSISTENCE, true);
    }

}
