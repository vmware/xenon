package com.vmware.xenon.samples.querytasks.services;

import com.google.gson.reflect.TypeToken;
import com.vmware.xenon.common.*;
import com.vmware.xenon.common.serialization.JsonMapper;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

public class PersonFactoryService extends FactoryService{

    public static final Type COLLECTION_TYPE = new TypeToken<List<PersonService.PersonState>>(){}.getType();
    public static final String SEED_FILE = "/people.json";

    public static final String SELF_LINK = "/people";

    public PersonFactoryService() {
        super(PersonService.PersonState.class);
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return new PersonService();
    }



}
