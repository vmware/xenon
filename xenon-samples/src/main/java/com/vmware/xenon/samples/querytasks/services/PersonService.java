package com.vmware.xenon.samples.querytasks.services;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;

import java.time.ZonedDateTime;
import java.util.Date;
import java.util.UUID;

public class PersonService extends StatefulService {

    public static class PersonState extends ServiceDocument {

        public static final String FIELD_NAME_BIRTH_DATE = "birthDate";
        public static final String FIELD_NAME_EMAIL = "email";
        public static final String FIELD_NAME_NAME = "name";

        public UUID id;
        public String name;
        public String email;
        public Date birthDate;
    }

    public PersonService() {
        super(PersonState.class);
        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
    }



}
