/*
 * Copyright (c) 2014-2015 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.samples.querytasks;

import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.serialization.JsonMapper;
import com.vmware.xenon.samples.querytasks.services.OracleEmployeesService;
import com.vmware.xenon.samples.querytasks.services.PersonFactoryService;
import com.vmware.xenon.samples.querytasks.services.PersonService;

public class Host extends ServiceHost {

    public static void main(String[] args) throws Throwable {
        Host h = new Host();
        h.initialize(args);
        h.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            h.log(Level.WARNING, "Host stopping ...");
            h.stop();
            h.log(Level.WARNING, "Host is stopped");
        }));
    }

    @Override
    public ServiceHost start() throws Throwable {
        super.start();
        startDefaultCoreServicesSynchronously();

        startService(Operation.createPost(UriUtils.buildUri(this, OracleEmployeesService.class)), new OracleEmployeesService());

        Operation startServiceOp = Operation
                .createPost(UriUtils.buildUri(this, PersonFactoryService.class))
                .setCompletion(this::onGetAllPeopleComplete);

        startService(startServiceOp, new PersonFactoryService());
        return this;
    }

    void onGetAllPeopleComplete(Operation po, Throwable pe) {
        CountDownLatch latch = new CountDownLatch(1);

        Operation op = Operation.createGet(UriUtils.buildUri(this, PersonFactoryService.class)).setReferer(UriUtils.buildUri(this, "/")).setCompletion((o, e) -> {
            if (e != null) {
                log(Level.SEVERE, "failed to get the person instances from people");
                latch.countDown(); // make sure we stop waiting
                return;
            }

            latch.countDown(); // make sure we stop waiting
            ServiceDocumentQueryResult result = o.getBody(ServiceDocumentQueryResult.class);
            log(Level.INFO, "retrieved %d people", result.documentLinks.size());
            if (result.documentLinks == null || result.documentLinks.isEmpty()) {
                // this is empty so we have to seed the data for querying
                log(Level.INFO, "seeding with 1000 people");
                seedData(); // pretends to be synchronous
            }

        });
        sendRequest(op);
        try {
            latch.await();
        } catch (InterruptedException ex) {
            log(Level.SEVERE, "%s", Utils.toString(ex));
        }
    }

    public void seedData() {
        // get the resource file path
        URL fileUrl = PersonService.class.getResource(PersonFactoryService.SEED_FILE);
        try {
            Path filePath = Paths.get(fileUrl.toURI());
            String json = new String(Files.readAllBytes(filePath), "UTF-8");

            // read all the people from that file
            JsonMapper mapper = new JsonMapper();
            List<PersonService.PersonState> people = mapper.fromJson(json, PersonFactoryService.COLLECTION_TYPE);
            URI peopleUri = UriUtils.buildUri(this, PersonFactoryService.class);

            // set up an expectation of how many people we're going to insert
            CountDownLatch latch = new CountDownLatch(people.size());
            for (PersonService.PersonState person : people) {
                // create the insert of a person
                Operation op = Operation.createPost(peopleUri).setReferer(UriUtils.buildUri(this, "/")).setBodyNoCloning(person).setCompletion((o, e) -> {
                    // make sure we don't wait indefinitely for this guy to complete
                    // success or failure doesn't matter much in this case
                    latch.countDown();

                    if (e != null) {
                        log(Level.SEVERE, "%s", Utils.toString(e));
                        return;
                    }

                    // Yay! success, let people know we did something
                    log(Level.INFO, "added %s", person.name);
                });
                sendRequest(op);
            }

            // wait for this to complete
            latch.await();
        } catch (Exception e) {
            // sad panda 8:(
            log(Level.SEVERE, "%s", Utils.toString(e));
        }
    }


}

