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

package com.vmware.xenon.common.cloning;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.VerificationHost;

public class TestCloningJava8Times {

    private VerificationHost host;
    private URI factoryUri;
    private URI tmpUri;

    @Before
    public void setup() throws Throwable {
        this.host = VerificationHost.create(0, null);
        this.host.start();

        this.factoryUri = UriUtils.buildUri(this.host, FactoryServiceWithJava8Times.class);

        this.host.startService(
                Operation.createPost(this.factoryUri),
                new FactoryServiceWithJava8Times());
        this.host.waitForServiceAvailable(FactoryServiceWithJava8Times.SELF_LINK);
    }

    @After
    public void tearDown() throws Throwable {
        this.host.stop();
    }

    @Test
    public void testNonNullValues() throws Throwable {
        Instant inst = Instant.now().minus(2, ChronoUnit.DAYS);
        ZonedDateTime zdtWithOffset = Instant.now().plus(3, ChronoUnit.HOURS)
                .atZone(ZoneOffset.of("+08:00"));
        ZonedDateTime zdtWithRegion = Instant.now().plus(2, ChronoUnit.DAYS)
                .atZone(ZoneId.of("America/New_York"));

        ServiceDocumentWithJava8Times doc = new ServiceDocumentWithJava8Times();
        doc.inst = inst;
        doc.zdtWithOffset = zdtWithOffset;
        doc.zdtWithRegion = zdtWithRegion;

        doc.uuid = UUID.randomUUID();
        doc.uri = URI.create("/test/unit");

        // create new doc
        this.host.testStart(1);
        this.host.send(Operation.createPost(this.factoryUri)
                .setBody(doc)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    this.tmpUri = o.getUri();
                    this.host.completeIteration();
                }));
        this.host.testWait();

        // now check correctly defined
        this.host.testStart(1);
        this.host.send(Operation.createGet(this.tmpUri)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    ServiceDocumentWithJava8Times actual = o
                            .getBody(ServiceDocumentWithJava8Times.class);
                    if (checkExpected(doc, actual)) {
                        this.host.completeIteration();
                    }
                }));
        this.host.testWait();
    }

    @Test
    public void testNullValues() throws Throwable {
        ServiceDocumentWithJava8Times doc = new ServiceDocumentWithJava8Times();

        // create new doc
        this.host.testStart(1);
        this.host.send(Operation.createPost(this.factoryUri)
                .setBody(doc)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    this.tmpUri = o.getUri();
                    this.host.completeIteration();
                }));
        this.host.testWait();

        // now check correctly defined
        this.host.testStart(1);
        this.host.send(Operation.createGet(this.tmpUri)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    ServiceDocumentWithJava8Times actual = o
                            .getBody(ServiceDocumentWithJava8Times.class);
                    if (checkExpected(doc, actual)) {
                        this.host.completeIteration();
                    }
                }));
        this.host.testWait();
    }

    private boolean checkExpected(ServiceDocumentWithJava8Times expected,
            ServiceDocumentWithJava8Times actual) {
        try {
            assertEquals(expected.inst, actual.inst);
            assertEquals(expected.zdtWithOffset, actual.zdtWithOffset);
            assertEquals(expected.zdtWithRegion, actual.zdtWithRegion);
            return true;
        } catch (Throwable t) {
            this.host.failIteration(t);
            return false;
        }
    }

    public static class FactoryServiceWithJava8Times extends FactoryService {

        public static final String SELF_LINK = "/test/java8times";

        FactoryServiceWithJava8Times() {
            super(ServiceDocumentWithJava8Times.class);
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            return new ServiceWithJava8Times();
        }

    }

    public static class ServiceWithJava8Times extends StatefulService {

        ServiceWithJava8Times() {
            super(ServiceDocumentWithJava8Times.class);
        }

    }

    public static class ServiceDocumentWithJava8Times extends ServiceDocument {
        public Instant inst;
        public ZonedDateTime zdtWithOffset;
        public ZonedDateTime zdtWithRegion;
        public UUID uuid;
        public URI uri;
    }

}
