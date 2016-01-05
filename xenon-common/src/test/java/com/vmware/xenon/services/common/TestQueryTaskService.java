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

package com.vmware.xenon.services.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import org.junit.After;
import org.junit.Test;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceDocumentDescription.TypeName;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceHost.ServiceNotFoundException;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask.NumericRange;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Occurance;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.xenon.services.common.QueryValidationTestService.QueryValidationServiceState;
import com.vmware.xenon.services.common.TenantService.TenantState;

public class TestQueryTaskService {
    private static final String TEXT_VALUE = "the decentralized control plane is a nice framework for queries";
    private static final String SERVICE_LINK_VALUE = "provisioning/dhcp-subnets/192.4.0.0/16";
    private static final double DOUBLE_MIN_OFFSET = 123.0;

    public int serviceCount = 50;
    public int queryCount = 10;

    private VerificationHost host;

    private void setUpHost() throws Throwable {
        if (this.host != null) {
            return;
        }
        this.host = VerificationHost.create(0);
        CommandLineArgumentParser.parseFromProperties(this.host);
        CommandLineArgumentParser.parseFromProperties(this);
        try {
            this.host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS
                    .toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
            this.host.start();
            this.host.toggleServiceOptions(this.host.getDocumentIndexServiceUri(),
                    EnumSet.of(ServiceOption.INSTRUMENTATION),
                    null);
            // disable synchronization so it does not interfere with the factory POSTs.
            // This test does not test nodes coming and going, it just relies on replication.
            // In theory, POSTs while the node group is changing should succeed (any failures
            // should be transparently retried) but its a best effort process.
            // Disabling it on the core verification host also disables it on all peer in process
            // hosts since VerificationHost.setupPeerHosts uses the setting of the parent host
            this.host.setPeerSynchronizationEnabled(false);

        } catch (Throwable e) {
            throw new Exception(e);
        }
    }

    @After
    public void tearDown() throws Exception {
        if (this.host == null) {
            return;
        }
        try {
            this.host.logServiceStats(this.host.getDocumentIndexServiceUri());
        } catch (Throwable e) {
            this.host.log("Error logging stats: %s", e.toString());
        }
        this.host.tearDownInProcessPeers();
        this.host.tearDown();
    }

    @Test
    public void complexDocumentReflection() {
        PropertyDescription pd;
        ServiceDocumentDescription.Builder b = ServiceDocumentDescription.Builder.create();

        QueryValidationServiceState s = new QueryValidationServiceState();
        ServiceDocumentDescription sdd = b.buildDescription(s.getClass(),
                EnumSet.of(ServiceOption.PERSISTENCE));

        final int expectedCustomFields = 28;
        final int expectedBuiltInFields = 10;
        // Verify the reflection of the root document
        assertTrue(sdd.propertyDescriptions != null && !sdd.propertyDescriptions.isEmpty());
        assertTrue(sdd.propertyDescriptions.size() == expectedCustomFields + expectedBuiltInFields);

        pd = sdd.propertyDescriptions.get(ServiceDocument.FIELD_NAME_SOURCE_LINK);
        assertTrue(pd.exampleValue == null);

        pd = sdd.propertyDescriptions.get(ServiceDocument.FIELD_NAME_OWNER);
        assertTrue(pd.exampleValue == null);

        pd = sdd.propertyDescriptions.get(ServiceDocument.FIELD_NAME_AUTH_PRINCIPAL_LINK);
        assertTrue(pd.exampleValue == null);

        pd = sdd.propertyDescriptions.get(ServiceDocument.FIELD_NAME_TRANSACTION_ID);
        assertTrue(pd.exampleValue == null);

        pd = sdd.propertyDescriptions.get(ServiceDocument.FIELD_NAME_EPOCH);
        assertTrue(pd.exampleValue == null);

        assertTrue(sdd.serviceCapabilities.contains(ServiceOption.PERSISTENCE));
        Map<ServiceDocumentDescription.TypeName, Long> descriptionsPerType = countReflectedFieldTypes(
                sdd);
        assertTrue(descriptionsPerType.get(TypeName.BOOLEAN) == 1L);
        assertTrue(descriptionsPerType.get(TypeName.MAP) == 8L);
        assertTrue(descriptionsPerType.get(TypeName.LONG) == 1L + 4L);
        assertTrue(descriptionsPerType.get(TypeName.PODO) == 3L);
        assertTrue(descriptionsPerType.get(TypeName.COLLECTION) == 4L);
        assertTrue(descriptionsPerType.get(TypeName.ARRAY) == 3L);
        assertTrue(descriptionsPerType.get(TypeName.STRING) == 5L + 5L);
        assertTrue(descriptionsPerType.get(TypeName.DATE) == 1L);
        assertTrue(descriptionsPerType.get(TypeName.DOUBLE) == 1L);
        assertTrue(descriptionsPerType.get(TypeName.BYTES) == 1L);

        pd = sdd.propertyDescriptions.get("exampleValue");
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.PODO));
        assertTrue(pd.fieldDescriptions != null);
        assertTrue(pd.fieldDescriptions.size() == 3 + expectedBuiltInFields);
        assertTrue(pd.fieldDescriptions.get("keyValues") != null);

        pd = sdd.propertyDescriptions.get("nestedComplexValue");
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.PODO));
        assertTrue(pd.fieldDescriptions != null);
        assertTrue(pd.fieldDescriptions.size() == 3);
        assertTrue(pd.fieldDescriptions.get("link") != null);

        // Verify the reflection of generic types
        pd = sdd.propertyDescriptions.get("listOfStrings");
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.COLLECTION));
        assertTrue(pd.elementDescription != null);
        assertTrue(pd.elementDescription.typeName.equals(TypeName.STRING));

        pd = sdd.propertyDescriptions.get("listOfNestedValues");
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.COLLECTION));
        assertTrue(pd.elementDescription != null);
        assertTrue(pd.elementDescription.typeName.equals(TypeName.PODO));

        pd = sdd.propertyDescriptions.get("listOfExampleValues");
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.COLLECTION));
        assertEquals(EnumSet.of(PropertyUsageOption.OPTIONAL), pd.usageOptions);
        assertTrue(pd.elementDescription != null);
        assertTrue(pd.elementDescription.typeName.equals(TypeName.PODO));
        // make sure annotation doesn't recurse to elements
        assertEquals(EnumSet.noneOf(PropertyUsageOption.class), pd.elementDescription.usageOptions);

        // Verify the reflection of array types
        pd = sdd.propertyDescriptions.get("arrayOfStrings");
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.ARRAY));
        assertTrue(pd.elementDescription != null);
        assertTrue(pd.elementDescription.typeName.equals(TypeName.STRING));

        pd = sdd.propertyDescriptions.get("arrayOfExampleValues");
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.ARRAY));
        assertTrue(pd.elementDescription != null);
        assertTrue(pd.elementDescription.typeName.equals(TypeName.PODO));

        // Verify the reflection of a composite type
        pd = sdd.propertyDescriptions.get("compositeTypeValue");
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.COLLECTION));
        assertEquals(EnumSet.noneOf(PropertyIndexingOption.class), pd.indexingOptions);
        pd = pd.elementDescription;
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.MAP));
        assertEquals(EnumSet.of(PropertyIndexingOption.EXPAND), pd.indexingOptions);
        pd = pd.elementDescription;
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.COLLECTION));
        assertEquals(EnumSet.of(PropertyIndexingOption.EXPAND), pd.indexingOptions);
        pd = pd.elementDescription;
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.PODO));
        assertEquals(EnumSet.of(PropertyIndexingOption.EXPAND), pd.indexingOptions);
        pd = pd.fieldDescriptions.get("link");
        assertEquals(EnumSet.of(PropertyUsageOption.LINK), pd.usageOptions);
        assertEquals("some/service", pd.exampleValue);

        // Verify Documentation annotation
        pd = sdd.propertyDescriptions.get("longValue");
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.LONG));
        assertEquals(EnumSet.of(PropertyUsageOption.OPTIONAL), pd.usageOptions);
        assertEquals("a Long value", pd.propertyDocumentation);

        // Verify multiple Usage annotations are processed correctly
        pd = sdd.propertyDescriptions.get("serviceLink");
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.STRING));
        assertEquals(EnumSet.of(PropertyUsageOption.OPTIONAL, PropertyUsageOption.LINK),
                pd.usageOptions);
        assertEquals("some/service", pd.exampleValue);
    }

    private Map<ServiceDocumentDescription.TypeName, Long> countReflectedFieldTypes(
            ServiceDocumentDescription sdd) {
        Map<ServiceDocumentDescription.TypeName, Long> descriptionsPerType = new HashMap<>();
        for (Entry<String, PropertyDescription> e : sdd.propertyDescriptions.entrySet()) {
            PropertyDescription pd = e.getValue();
            Long count = descriptionsPerType.get(pd.typeName);
            if (count == null) {
                count = 0L;
            }
            count++;
            descriptionsPerType.put(pd.typeName, count);
        }
        return descriptionsPerType;
    }

    @Test
    public void continuousQueryTask() throws Throwable {
        setUpHost();

        Throwable[] failure = new Throwable[1];

        int servicesWithExpirationCount = Math.min(3, this.serviceCount);
        // we expect an update for initial state indexed as part of POST to the factory,
        // then another for the PUT we do on each service, and another for the DELETE.
        // For services with expiration there will be one more PATCH to set expiration.
        int totalCount = this.serviceCount * 3 + servicesWithExpirationCount;
        CountDownLatch stateUpdates = new CountDownLatch(totalCount);

        // create the continuous task monitoring updates across the index
        QueryValidationServiceState newState = createContinuousQueryTasks(failure, stateUpdates);

        this.host.log("Query task is active in index service");
        long start = Utils.getNowMicrosUtc();

        // start services
        List<URI> services = startQueryTargetServices(this.serviceCount, newState);

        // reserve a couple of services to test notification of expiration induced deletes
        Set<URI> servicesWithExpiration = new HashSet<>();
        for (URI u : services) {
            servicesWithExpiration.add(u);
            if (servicesWithExpiration.size() >= servicesWithExpirationCount) {
                break;
            }
        }

        // update services
        newState = putSimpleStateOnQueryTargetServices(services, newState);

        // send DELETEs, wait for DELETE notifications
        this.host.testStart(services.size() - servicesWithExpirationCount);
        for (URI service : services) {
            if (servicesWithExpiration.contains(service)) {
                continue;
            }
            Operation delete = Operation.createDelete(service).setCompletion(
                    this.host.getCompletion());
            this.host.send(delete);
        }
        this.host.testWait();

        // issue a PATCH to a sub set of the services and expect notifications for both the PATCH
        // and the expiration induced DELETE
        this.host.testStart(servicesWithExpirationCount);
        for (URI service : servicesWithExpiration) {
            QueryValidationServiceState patchBody = new QueryValidationServiceState();
            patchBody.documentExpirationTimeMicros = 1;
            Operation patchExpiration = Operation.createPatch(service)
                    .setBody(patchBody)
                    .setCompletion(this.host.getCompletion());
            this.host.send(patchExpiration);
        }
        this.host.testWait();

        if (!stateUpdates.await(this.host.getOperationTimeoutMicros(), TimeUnit.MICROSECONDS)) {
            throw new TimeoutException("Notifications never received");
        }

        if (failure[0] != null) {
            throw failure[0];
        }

        long end = Utils.getNowMicrosUtc();

        double thpt = totalCount / ((end - start) / 1000000.0);
        this.host.log("Update notification throughput (updates/sec): %f, update count: %d", thpt,
                totalCount);
    }

    private QueryValidationServiceState createContinuousQueryTasks(Throwable[] failure,
            CountDownLatch stateUpdates)
            throws Throwable, InterruptedException {
        QueryValidationServiceState newState = new QueryValidationServiceState();
        final String stringValue = UUID.randomUUID().toString();
        Query query = Query.Builder.create()
                .addFieldClause("stringValue", stringValue)
                .build();
        newState.stringValue = stringValue;

        QueryTask task = QueryTask.Builder.create()
                .addOptions(EnumSet.of(QueryOption.CONTINUOUS, QueryOption.EXPAND_CONTENT))
                .setQuery(query)
                .build();
        URI updateQueryTask = this.host.createQueryTaskService(
                UriUtils.buildUri(this.host.getUri(), ServiceUriPaths.CORE_QUERY_TASKS),
                task, false, false, task, null);

        newState.stringValue = stringValue;

        this.host.testStart(1);
        Operation post = Operation.createPost(updateQueryTask)
                .setReferer(this.host.getReferer())
                .setCompletion(this.host.getCompletion());

        // subscribe to state update query task
        this.host.startSubscriptionService(
                post,
                (notifyOp) -> {
                    try {
                        QueryTask body = notifyOp.getBody(QueryTask.class);
                        if (body.results == null || body.results.documentLinks.isEmpty()) {
                            return;
                        }

                        for (Object doc : body.results.documents.values()) {
                            QueryValidationServiceState state = Utils.fromJson(doc,
                                    QueryValidationServiceState.class);

                            if (!stringValue.equals(state.stringValue)) {
                                failure[0] = new IllegalStateException(
                                        "Unexpected document:" + Utils.toJsonHtml(state));
                                return;
                            }
                        }
                    } catch (Throwable e) {
                        failure[0] = e;
                    } finally {
                        stateUpdates.countDown();
                    }
                });

        // wait for subscription to go through before we start issuing updates
        this.host.testWait();

        // wait for filter to be active in the index service, which happens asynchronously
        // in relation to query task creation, before issuing updates.

        Date exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {
            ServiceStats indexStats = this.host.getServiceState(null, ServiceStats.class,
                    UriUtils.buildStatsUri(this.host.getDocumentIndexServiceUri()));
            ServiceStat activeQueryStat = indexStats.entries.get(
                    LuceneDocumentIndexService.STAT_NAME_ACTIVE_QUERY_FILTERS);
            if (activeQueryStat == null || activeQueryStat.latestValue < 1.0) {
                Thread.sleep(250);
                continue;
            }
            break;
        }
        return newState;
    }

    @Test
    public void throughputSimpleQuery() throws Throwable {
        setUpHost();
        List<URI> services = startQueryTargetServices(this.serviceCount);
        QueryValidationServiceState newState = new QueryValidationServiceState();
        newState.stringValue = "now";
        newState = putSimpleStateOnQueryTargetServices(services, newState);
        Query q = Query.Builder.create()
                .addFieldClause("id", newState.id, MatchType.PHRASE, Occurance.MUST_OCCUR)
                .addKindFieldClause(QueryValidationServiceState.class).build();

        // first do the test with no concurrent updates to the index, while we query
        boolean interleaveWrites = false;
        for (int i = 0; i < 3; i++) {
            doThroughputQuery(q, 1, newState, interleaveWrites);
        }

        // now update the index, once for every N queries. This will have a significant
        // impact on performance
        interleaveWrites = true;
        doThroughputQuery(q, 1, newState, interleaveWrites);
    }

    public void doThroughputQuery(Query q, int expectedResults,
            QueryValidationServiceState template, boolean interleaveWrites)
            throws Throwable {

        this.host.log(
                "Starting QPS test, service count: %d, query count: %d, interleave writes: %s",
                this.serviceCount, this.queryCount, interleaveWrites);
        QueryTask qt = QueryTask.Builder.createDirectTask().setQuery(q).build();
        this.host.testStart(this.queryCount);
        for (int i = 0; i < this.queryCount; i++) {
            final int index = i;
            Operation post = Operation.createPost(
                    UriUtils.buildUri(this.host, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS))
                    .setBodyNoCloning(qt)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        QueryTask rsp = o
                                .getBody(QueryTask.class);
                        if (rsp.results.documentLinks.size() != expectedResults) {
                            this.host.failIteration(
                                    new IllegalStateException("Unexpected result count"));
                            return;
                        }

                        if (!interleaveWrites || (index % 100) != 0) {
                            this.host.completeIteration();
                            return;
                        }

                        template.stringValue = "bla";
                        Operation put = Operation.createPut(
                                UriUtils.buildUri(this.host, rsp.results.documentLinks.get(0)))
                                .setBody(template);
                        this.host.send(put);
                        this.host.completeIteration();

                    });
            this.host.send(post);

        }
        this.host.testWait();
        this.host.logThroughput();
    }

    @Test
    public void throughputSimpleQueryDocumentSearch() throws Throwable {
        setUpHost();

        List<URI> services = startQueryTargetServices(this.serviceCount);

        // start two different types of services, creating two sets of documents
        // first start the query validation service instances, setting the id
        // field
        // to the same value
        QueryValidationServiceState newState = new QueryValidationServiceState();

        // test search performance using a single version per service, single match
        newState = putSimpleStateOnQueryTargetServices(services, newState);
        for (int i = 0; i < 5; i++) {
            this.host.createAndWaitSimpleDirectQuery("id", newState.id, services.size(), 1);
        }

        // all expected as results
        newState.stringValue = "hello";
        newState = putSimpleStateOnQueryTargetServices(services, newState);
        for (int i = 0; i < 5; i++) {
            this.host.createAndWaitSimpleDirectQuery("stringValue", newState.stringValue,
                    services.size(),
                    services.size());
        }

        // make sure throughput is not degraded when multiple versions are added per service
        for (int i = 0; i < 5; i++) {
            newState = putSimpleStateOnQueryTargetServices(services, newState);
        }

        for (int i = 0; i < 5; i++) {
            this.host.createAndWaitSimpleDirectQuery("id", newState.id, services.size(), 1);
        }

    }

    @Test
    public void throughputComplexQueryDocumentSearch() throws Throwable {
        setUpHost();

        List<URI> services = startQueryTargetServices(this.serviceCount);

        // start two different types of services, creating two sets of documents
        // first start the query validation service instances, setting the id
        // field
        // to the same value
        QueryValidationServiceState newState = VerificationHost.buildQueryValidationState();
        newState.nestedComplexValue.link = TestQueryTaskService.SERVICE_LINK_VALUE;

        // first pass, we test search performance using a single version per service
        newState = putStateOnQueryTargetServices(services, 1,
                newState);
        doComplexStateQueries(services, 1, newState);

        // second pass, we measure perf with multiple versions per service
        int versionCount = 1;
        newState = putStateOnQueryTargetServices(services, versionCount,
                newState);
        doComplexStateQueries(services, versionCount + 1, newState);
    }

    private void doComplexStateQueries(List<URI> services, int versionCount,
            QueryValidationServiceState newState)
            throws Throwable {

        for (int i = 0; i < 5; i++) {
            this.host.log("%d", i);
            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCompositeFieldName("exampleValue", "name"),
                    newState.exampleValue.name, services.size(), 1);

            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCompositeFieldName("nestedComplexValue", "id"),
                    newState.nestedComplexValue.id, services.size(), services.size());

            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCompositeFieldName("listOfExampleValues", "item",
                            "name"),
                    newState.listOfExampleValues.get(0).name, services.size(), services.size());

            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCompositeFieldName("arrayOfExampleValues", "item",
                            "name"),
                    newState.arrayOfExampleValues[0].name, services.size(), services.size());

            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCollectionItemName("listOfStrings"),
                    newState.listOfStrings.get(0), services.size(), services.size());

            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCollectionItemName("arrayOfStrings"),
                    newState.arrayOfStrings[1], services.size(), services.size());

            this.host.createAndWaitSimpleDirectQuery(
                    "id",
                    newState.id, services.size(), 1);

            doInQuery("id",
                    newState.id, services.size(), 1);
            doInCollectionQuery("listOfStrings", newState.listOfStrings,
                    services.size(), services.size());

            doMapQuery("mapOfBooleans", newState.mapOfBooleans, services.size(), services.size());
            doMapQuery("mapOfBytesArray", newState.mapOfBytesArrays, services.size(), 0);
            doMapQuery("mapOfStrings", newState.mapOfStrings, services.size(), services.size());
            doMapQuery("mapOfUris", newState.mapOfUris, services.size(), services.size());

            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCompositeFieldName("mapOfNestedTypes", "nested", "id"),
                    newState.mapOfNestedTypes.get("nested").id, services.size(), services.size());

            // query for a field that SHOULD be ignored. We should get zero links back
            this.host.createAndWaitSimpleDirectQuery(
                    "ignoredStringValue",
                    newState.ignoredStringValue, services.size(), 0);

            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCollectionItemName("ignoredArrayOfStrings"),
                    newState.ignoredArrayOfStrings[1], services.size(), 0);
        }
    }

    @SuppressWarnings({ "rawtypes" })
    private void doMapQuery(String mapName, Map map, long documentCount, long expectedResultCount)
            throws Throwable {
        for (Object o : map.entrySet()) {
            Entry e = (Entry) o;
            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCompositeFieldName(mapName, (String) e.getKey()),
                    e.getValue().toString(), documentCount, expectedResultCount);
        }
    }

    private void doInQuery(String fieldName, String fieldValue, long documentCount,
            long expectedResultCount) throws Throwable {
        QuerySpecification spec = new QuerySpecification();
        spec.query = Query.Builder.create().addInClause(
                fieldName,
                Arrays.asList(
                        UUID.randomUUID().toString(),
                        fieldValue,
                        UUID.randomUUID().toString()))
                .build();
        this.host.createAndWaitSimpleDirectQuery(spec,
                documentCount, expectedResultCount);
    }

    @SuppressWarnings({ "rawtypes" })
    private void doInCollectionQuery(String collName, Collection coll, long documentCount,
            long expectedResultCount)
            throws Throwable {
        for (Object val : coll) {
            QuerySpecification spec = new QuerySpecification();
            spec.query = Query.Builder.create().addInCollectionItemClause(
                    collName,
                    Arrays.asList(
                            UUID.randomUUID().toString(),
                            (String) val,
                            UUID.randomUUID().toString()))
                    .build();
            this.host.createAndWaitSimpleDirectQuery(spec,
                    documentCount, expectedResultCount);
        }
    }

    @Test
    public void kindMatch() throws Throwable {
        setUpHost();
        long sc = this.serviceCount;
        long vc = 10;
        doKindMatchTest(sc, vc, false);
    }

    @Test
    public void kindMatchRemote() throws Throwable {
        setUpHost();
        doKindMatchTest(this.serviceCount, 2, true);
    }

    public void doKindMatchTest(long serviceCount, long versionCount, boolean forceRemote)
            throws Throwable {

        List<URI> services = startQueryTargetServices((int) (serviceCount / 2));

        // start two different types of services, creating two sets of documents
        // first start the query validation service instances, setting the id
        // field
        // to the same value
        QueryValidationServiceState newState = new QueryValidationServiceState();
        newState.id = UUID.randomUUID().toString();
        newState = putStateOnQueryTargetServices(services, (int) versionCount,
                newState);

        // start some unrelated service that also happens to have an id field,
        // also set to the same value
        MinimalTestServiceState mtss = new MinimalTestServiceState();
        mtss.id = newState.id;
        List<Service> mt = this.host.doThroughputServiceStart(serviceCount / 2,
                MinimalTestService.class, mtss, EnumSet.of(ServiceOption.PERSISTENCE), null);
        List<URI> minimalTestServices = new ArrayList<>();
        for (Service s : mt) {
            minimalTestServices.add(s.getUri());
        }

        // issue a query that matches kind for the query validation service
        Query query = Query.Builder.create()
                .addKindFieldClause(QueryValidationServiceState.class)
                .build();
        QueryTask queryTask = QueryTask.Builder.create().setQuery(query).build();

        createWaitAndValidateQueryTask((int) versionCount, services, queryTask.querySpec,
                forceRemote);

        // same as above, but ask for COUNT only, no links
        queryTask.querySpec.options = EnumSet.of(QueryOption.COUNT,
                QueryOption.INCLUDE_ALL_VERSIONS);
        createWaitAndValidateQueryTask((int) versionCount, services, queryTask.querySpec,
                forceRemote);

        // now make sure expand works. Issue same query, but enable expand
        queryTask.querySpec.options = EnumSet.of(QueryOption.EXPAND_CONTENT);
        createWaitAndValidateQueryTask(versionCount, services, queryTask.querySpec, forceRemote);

        // now for the minimal test service
        query = Query.Builder.create()
                .addKindFieldClause(MinimalTestServiceState.class)
                .build();
        queryTask = QueryTask.Builder.create().setQuery(query).build();

        createWaitAndValidateQueryTask(versionCount, minimalTestServices, queryTask.querySpec,
                forceRemote);
    }

    @Test
    public void singleFieldQuery() throws Throwable {
        setUpHost();

        long c = this.host.computeIterationsFromMemory(10);
        doSelfLinkTest((int) c, 10, false);
    }

    @Test
    public void broadcastQueryTasksOnExampleStates () throws Throwable {
        setUpHost();

        int nodeCount = 3;

        this.host.setUpPeerHosts(nodeCount);
        this.host.joinNodesAndVerifyConvergence(nodeCount);

        verifyOnlySupportSortOnSelfLinkInBroadcast();
        verifyNotAllowDirectQueryInBroadcast();

        VerificationHost targetHost = this.host.getPeerHost();
        URI exampleFactoryURI = UriUtils.buildUri(targetHost, ExampleFactoryService.SELF_LINK);

        this.host.testStart(this.serviceCount);
        List<URI> exampleServices = new ArrayList<>();
        for (int i = 0; i < this.serviceCount; i++) {
            ExampleServiceState s = new ExampleServiceState();
            s.name = "document" + i;
            s.documentSelfLink = s.name;
            exampleServices.add(UriUtils.buildUri(this.host.getUri(),
                    ExampleFactoryService.SELF_LINK, s.documentSelfLink));
            this.host.send(Operation.createPost(exampleFactoryURI)
                    .setBody(s)
                    .setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();

        nonpaginatedBroadcastQueryTasksOnExampleStates(targetHost);
        paginatedBroadcastQueryTasksOnExampleStates();
        paginatedBroadcastQueryTasksWithoutMatching();
        paginatedBroadcastQueryTasksRepeatSamePage();
    }

    private void verifyOnlySupportSortOnSelfLinkInBroadcast() throws Throwable {
        VerificationHost targetHost = this.host.getPeerHost();

        QuerySpecification q = new QuerySpecification();
        Query kindClause = new Query();
        kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(Utils.buildKind(ExampleServiceState.class));
        q.query = kindClause;
        q.options = EnumSet.of(QueryOption.EXPAND_CONTENT, QueryOption.SORT, QueryOption.BROADCAST);
        q.sortTerm = new QueryTask.QueryTerm();
        q.sortTerm.propertyType = TypeName.STRING;
        q.sortTerm.propertyName = ExampleServiceState.FIELD_NAME_NAME;

        QueryTask task = QueryTask.create(q);

        targetHost.testStart(1);

        URI factoryUri = UriUtils.buildUri(targetHost, ServiceUriPaths.CORE_QUERY_TASKS);
        Operation startPost = Operation
                .createPost(factoryUri)
                .setBody(task)
                .setCompletion((o, e) -> {
                    validateBroadcastQueryPostFailure(targetHost, o, e);
                });
        targetHost.send(startPost);

        targetHost.testWait();
    }

    private void verifyNotAllowDirectQueryInBroadcast() throws Throwable {
        VerificationHost targetHost = this.host.getPeerHost();

        QuerySpecification q = new QuerySpecification();
        Query kindClause = new Query();
        kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(Utils.buildKind(ExampleServiceState.class));
        q.query = kindClause;
        q.options = EnumSet.of(QueryOption.BROADCAST);

        QueryTask task = QueryTask.create(q);
        task.setDirect(true);

        targetHost.testStart(1);

        URI factoryUri = UriUtils.buildUri(targetHost, ServiceUriPaths.CORE_QUERY_TASKS);
        Operation startPost = Operation
                .createPost(factoryUri)
                .setBody(task)
                .setCompletion((o, e) -> {
                    validateBroadcastQueryPostFailure(targetHost, o, e);
                });
        targetHost.send(startPost);

        targetHost.testWait();
    }

    private void validateBroadcastQueryPostFailure(VerificationHost targetHost, Operation o,
            Throwable e) {
        if (e != null) {
            ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
            if (rsp.message == null
                    || !rsp.message.contains(QueryOption.BROADCAST.toString())) {
                targetHost.failIteration(new IllegalStateException("Expected failure"));
                return;
            }
            targetHost.completeIteration();
        } else {
            targetHost.failIteration(new IllegalStateException("expected failure"));
        }
    }

    private void nonpaginatedBroadcastQueryTasksOnExampleStates(VerificationHost targetHost)
            throws Throwable {
        QuerySpecification q = new QuerySpecification();
        Query kindClause = new Query();
        kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(Utils.buildKind(ExampleServiceState.class));
        q.query = kindClause;
        q.options = EnumSet.of(QueryOption.EXPAND_CONTENT, QueryOption.BROADCAST);

        QueryTask task = QueryTask.create(q);

        URI taskUri = this.host.createQueryTaskService(task, false, task.taskInfo.isDirect, task, null);
        task = this.host.waitForQueryTaskCompletion(task.querySpec, 0, 0, taskUri, false, false);

        targetHost.testStart(1);
        Operation startGet = Operation
                .createGet(taskUri)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        targetHost.failIteration(e);
                        return;
                    }

                    QueryTask rsp = o.getBody(QueryTask.class);
                    if (this.serviceCount != rsp.results.documentCount) {
                        targetHost.failIteration(new IllegalStateException("Incorrect number of documents returned: "
                                + this.serviceCount + " expected, but " + rsp.results.documentCount + " returned"));
                        return;
                    }
                    targetHost.completeIteration();
                });
        targetHost.send(startGet);
        targetHost.testWait();
    }

    private void paginatedBroadcastQueryTasksOnExampleStates() throws Throwable {

        VerificationHost targetHost = this.host.getPeerHost();

        int resultLimit = 30;

        QuerySpecification q = new QuerySpecification();
        Query kindClause = new Query();
        kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(Utils.buildKind(ExampleServiceState.class));
        q.query = kindClause;
        q.options = EnumSet.of(QueryOption.EXPAND_CONTENT, QueryOption.BROADCAST);
        q.resultLimit = resultLimit;

        QueryTask task = QueryTask.create(q);

        URI taskUri = this.host.createQueryTaskService(task, false, task.taskInfo.isDirect, task, null);
        task = this.host.waitForQueryTaskCompletion(task.querySpec, 0, 0, taskUri, false, false);

        targetHost.testStart(1);
        Operation startGet = Operation
                .createGet(taskUri)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        targetHost.failIteration(e);
                        return;
                    }

                    QueryTask rsp = o.getBody(QueryTask.class);
                    if (rsp.results.documentCount != 0) {
                        targetHost.failIteration(new IllegalStateException("Incorrect number of documents returned: " +
                                "0 expected, but " + rsp.results.documentCount + " returned"));
                        return;
                    }

                    String expectedPageLinkSegment = UriUtils.buildUriPath(ServiceUriPaths.CORE,
                            BroadcastQueryPageService.SELF_LINK_PREFIX);
                    if (!rsp.results.nextPageLink.contains(expectedPageLinkSegment)) {
                        targetHost.failIteration(new IllegalStateException("Incorrect next page link returned: " +
                                rsp.results.nextPageLink));
                        return;
                    }

                    targetHost.completeIteration();
                });
        targetHost.send(startGet);
        targetHost.testWait();

        String nextPageLink = task.results.nextPageLink;
        Set<String> documentLinks = new HashSet<>();
        while (nextPageLink != null) {
            List<String> pageLinks = new ArrayList<>();
            URI u = UriUtils.buildUri(targetHost, nextPageLink);

            targetHost.testStart(1);
            Operation get = Operation
                    .createGet(u)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            targetHost.failIteration(e);
                            return;
                        }

                        QueryTask rsp = o.getBody(QueryTask.class);
                        pageLinks.add(rsp.results.nextPageLink);
                        documentLinks.addAll(rsp.results.documentLinks);

                        targetHost.completeIteration();
                    });

            targetHost.send(get);
            targetHost.testWait();

            nextPageLink = pageLinks.isEmpty() ? null : pageLinks.get(0);
        }

        assertEquals(this.serviceCount, documentLinks.size());

        for (int i = 0; i < this.serviceCount; i++) {
            assertTrue(documentLinks.contains(ExampleFactoryService.SELF_LINK + "/document" + i));
        }
    }

    private void paginatedBroadcastQueryTasksWithoutMatching() throws Throwable {

        VerificationHost targetHost = this.host.getPeerHost();

        int serviceCount = 100;
        int resultLimit = 30;

        QuerySpecification q = new QuerySpecification();

        Query kindClause = new Query();
        kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(Utils.buildKind(ExampleServiceState.class));
        q.query.addBooleanClause(kindClause);

        Query nameClause = new Query();
        nameClause.setTermPropertyName(ExampleServiceState.FIELD_NAME_NAME)
                .setTermMatchValue("document" + serviceCount);
        q.query.addBooleanClause(nameClause);

        q.options = EnumSet.of(QueryOption.EXPAND_CONTENT, QueryOption.BROADCAST);
        q.resultLimit = resultLimit;

        QueryTask task = QueryTask.create(q);

        URI taskUri = this.host.createQueryTaskService(task, false, task.taskInfo.isDirect, task, null);
        task = this.host.waitForQueryTaskCompletion(task.querySpec, 0, 0, taskUri, false, false);

        targetHost.testStart(1);
        Operation startGet = Operation
                .createGet(taskUri)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        targetHost.failIteration(e);
                        return;
                    }

                    QueryTask rsp = o.getBody(QueryTask.class);
                    if (rsp.results.documentCount != 0) {
                        targetHost.failIteration(new IllegalStateException("Incorrect number of documents returned: " +
                                "0 expected, but " + rsp.results.documentCount + " returned"));
                        return;
                    }

                    if (rsp.results.nextPageLink != null) {
                        targetHost.failIteration(new IllegalStateException("Next page link should be null"));
                        return;
                    }

                    targetHost.completeIteration();
                });
        targetHost.send(startGet);
        targetHost.testWait();
    }

    private void paginatedBroadcastQueryTasksRepeatSamePage() throws Throwable {

        VerificationHost targetHost = this.host.getPeerHost();

        int resultLimit = 30;

        QuerySpecification q = new QuerySpecification();

        Query kindClause = new Query();
        kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(Utils.buildKind(ExampleServiceState.class));
        q.query.addBooleanClause(kindClause);

        q.options = EnumSet.of(QueryOption.EXPAND_CONTENT, QueryOption.BROADCAST);
        q.resultLimit = resultLimit;

        QueryTask task = QueryTask.create(q);

        URI taskUri = this.host.createQueryTaskService(task, false, task.taskInfo.isDirect, task, null);
        task = this.host.waitForQueryTaskCompletion(task.querySpec, 0, 0, taskUri, false, false);

        targetHost.testStart(1);
        Operation startGet = Operation
                .createGet(taskUri)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        targetHost.failIteration(e);
                        return;
                    }

                    targetHost.completeIteration();
                });
        targetHost.send(startGet);
        targetHost.testWait();

        // Query the same page link twice, and make sure the same results are returned.
        List<List<String>> documentLinksList = new ArrayList<>();
        URI u = UriUtils.buildUri(targetHost, task.results.nextPageLink);

        for (int i = 0; i < 2; i++) {
            targetHost.testStart(1);
            startGet = Operation
                    .createGet(u)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            targetHost.failIteration(e);
                            return;
                        }

                        QueryTask rsp = o.getBody(QueryTask.class);
                        documentLinksList.add(rsp.results.documentLinks);

                        if (rsp.results.documentCount != resultLimit) {
                            targetHost.failIteration(new IllegalStateException("Incorrect number of documents " +
                                    "returned: " + resultLimit + " was expected, but " + rsp.results.documentCount +
                                    " was returned."));
                            return;
                        }

                        targetHost.completeIteration();
                    });

            targetHost.send(startGet);
            targetHost.testWait();
        }

        assertTrue(documentLinksList.get(0).equals(documentLinksList.get(1)));
    }

    @Test
    public void broadcastQueryStressTest() throws Throwable {
        setUpHost();

        final int nodeCount = 3;
        this.serviceCount = 1000;
        this.host.setUpPeerHosts(nodeCount);
        this.host.joinNodesAndVerifyConvergence(nodeCount);

        VerificationHost targetHost = this.host.getPeerHost();
        URI exampleFactoryURI = UriUtils.buildUri(targetHost, ExampleFactoryService.SELF_LINK);

        CommandLineArgumentParser.parseFromProperties(this);
        if (this.serviceCount > 1000) {
            targetHost.setStressTest(true);
        }

        this.host.testStart(this.serviceCount);
        List<URI> exampleServices = new ArrayList<>();
        for (int i = 0; i < this.serviceCount; i++) {
            ExampleServiceState s = new ExampleServiceState();
            s.name = "document" + i;
            s.documentSelfLink = s.name;
            exampleServices.add(UriUtils.buildUri(this.host.getUri(),
                    ExampleFactoryService.SELF_LINK, s.documentSelfLink));
            this.host.send(Operation.createPost(exampleFactoryURI)
                    .setBody(s)
                    .setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();

        // Simulate the scenario that multiple users query documents page by page
        // in broadcast way.
        targetHost.testStart(this.queryCount);
        for (int i = 0; i < this.queryCount; i++) {
            startPagedBroadCastQuery(targetHost);
        }

        // If the query cannot be finished in time, timeout exception would be thrown.
        targetHost.testWait();
    }

    private void startPagedBroadCastQuery(VerificationHost targetHost) {

        // This is a multi stage task, that could be easily modelled as a service,
        // but since we are in test code, we use synchronous waits between stages,
        // but run N threads in parallel
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    final int resultLimit = 100;

                    QuerySpecification q = new QuerySpecification();
                    Query kindClause = new Query();
                    kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                            .setTermMatchValue(Utils.buildKind(ExampleServiceState.class));
                    q.query = kindClause;
                    q.options = EnumSet.of(QueryOption.EXPAND_CONTENT, QueryOption.BROADCAST);
                    q.resultLimit = resultLimit;

                    QueryTask task = QueryTask.create(q);
                    if (task.documentExpirationTimeMicros == 0) {
                        task.documentExpirationTimeMicros = Utils.getNowMicrosUtc() + targetHost
                                .getOperationTimeoutMicros();
                    }
                    task.documentSelfLink = UUID.randomUUID().toString();

                    URI factoryUri = UriUtils.buildUri(targetHost, ServiceUriPaths.CORE_QUERY_TASKS);
                    Operation post = Operation
                            .createPost(factoryUri)
                            .setBody(task);
                    targetHost.send(post);

                    URI taskUri = UriUtils.extendUri(factoryUri, task.documentSelfLink);
                    List<String> pageLinks = new ArrayList<>();
                    do {
                        CountDownLatch waitForCompletion = new CountDownLatch(1);
                        Operation get = Operation
                                .createGet(taskUri)
                                .setCompletion((o, e) -> {
                                    if (e != null) {
                                        targetHost.log(Level.SEVERE, "test failed: %s", e.toString());
                                        targetHost.failIteration(e);

                                        return;
                                    }

                                    QueryTask rsp = o.getBody(QueryTask.class);
                                    if (rsp.taskInfo.stage == TaskStage.FINISHED
                                            || rsp.taskInfo.stage == TaskStage.FAILED
                                            || rsp.taskInfo.stage == TaskStage.CANCELLED) {

                                        pageLinks.add(rsp.results.nextPageLink);
                                    }

                                    waitForCompletion.countDown();
                                });
                        targetHost.send(get);

                        waitForCompletion.await();

                        if (!pageLinks.isEmpty()) {
                            break;
                        }

                        Thread.sleep(100);
                    } while (true);

                    String nextPageLink = pageLinks.get(0);
                    while (nextPageLink != null) {
                        pageLinks.clear();
                        URI u = UriUtils.buildUri(targetHost, nextPageLink);

                        CountDownLatch waitForCompletion = new CountDownLatch(1);
                        Operation get = Operation
                                .createGet(u)
                                .setCompletion((o, e) -> {
                                    if (e != null) {
                                        targetHost.log(Level.SEVERE, "test failed: %s", e.toString());
                                        targetHost.failIteration(e);

                                        return;
                                    }

                                    QueryTask rsp = o.getBody(QueryTask.class);
                                    pageLinks.add(rsp.results.nextPageLink);

                                    waitForCompletion.countDown();
                                });

                        targetHost.send(get);
                        waitForCompletion.await();

                        nextPageLink = pageLinks.isEmpty() ? null : pageLinks.get(0);
                    }
                } catch (Throwable e) {
                    targetHost.log(Level.SEVERE, "test failed: %s", e.toString());
                    targetHost.failIteration(e);

                    return;
                }

                targetHost.completeIteration();
            }
        };
        t.start();
    }

    @Test
    public void sortTestOnExampleStates() throws Throwable {
        doSortTestOnExampleStates(false, Integer.MAX_VALUE);
        doSortTestOnExampleStates(true, Integer.MAX_VALUE);
    }

    @Test
    public void topResultsWithSort() throws Throwable {
        doSortTestOnExampleStates(true, 10);
    }

    public void doSortTestOnExampleStates(boolean isDirect, int resultLimit) throws Throwable {
        setUpHost();
        int serviceCount = 100;
        URI exampleFactoryURI = UriUtils.buildUri(this.host, ExampleFactoryService.SELF_LINK);
        List<URI> exampleServices = new ArrayList<>();
        this.host.testStart(serviceCount);
        for (int i = 0; i < serviceCount; i++) {
            ExampleServiceState s = new ExampleServiceState();
            s.name = UUID.randomUUID().toString();
            s.documentSelfLink = s.name;
            exampleServices.add(UriUtils.buildUri(this.host.getUri(),
                    ExampleFactoryService.SELF_LINK, s.documentSelfLink));
            this.host.send(Operation.createPost(exampleFactoryURI)
                    .setBody(s)
                    .setCompletion(this.host.getCompletion()));

        }
        this.host.testWait();
        Query kindClause = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();

        QueryTask.Builder queryTaskBuilder = isDirect ? QueryTask.Builder.createDirectTask()
                : QueryTask.Builder.create();
        QueryTask task = queryTaskBuilder
                .setQuery(kindClause)
                .orderAscending(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING)
                .build();

        task.querySpec.resultLimit = resultLimit;
        if (resultLimit < Integer.MAX_VALUE) {
            task.querySpec.options.add(QueryOption.TOP_RESULTS);
        }

        if (task.documentExpirationTimeMicros != 0) {
            // the value was set as an interval by the calling test. Make absolute here so
            // account for service creation above
            task.documentExpirationTimeMicros = Utils.getNowMicrosUtc()
                    + task.documentExpirationTimeMicros;
        }

        this.host.logThroughput();
        URI taskURI = this.host.createQueryTaskService(task, false,
                task.taskInfo.isDirect, task, null);

        if (!task.taskInfo.isDirect) {
            task = this.host.waitForQueryTaskCompletion(task.querySpec, 0, 0,
                    taskURI, false, false);
        }

        assertTrue(task.results.nextPageLink == null);

        if (task.querySpec.options.contains(QueryOption.TOP_RESULTS)) {
            assertTrue(task.results.documentLinks.size() == resultLimit);
        }

        List<String> documentLinks = task.results.documentLinks;
        validateSortedList(documentLinks);

        this.host.testStart(exampleServices.size());
        for (URI u : exampleServices) {
            this.host.send(Operation.createDelete(u)
                    .setBody(new ServiceDocument())
                    .setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();
    }

    private void validateSortedList(List<String> documentLinks) {
        int i;
        /*
        Since name and documentLink are the same,
        validate that the documentLinks are in order
         */
        String prevLink = documentLinks.get(0);
        for (i = 1; i < documentLinks.size(); i++) {
            String currentLink = documentLinks.get(i);
            assertTrue("Sort Test Failed", currentLink.compareTo(prevLink) > 0);
            prevLink = currentLink;
        }
    }

    private void getNextPageResults(String nextPageLink, int resultLimit,
            final int[] numberOfDocumentLinks, final List<URI> toDelete,
            List<ExampleServiceState> documents, List<URI> pageLinks) {

        URI u = UriUtils.buildUri(this.host, nextPageLink);
        toDelete.add(u);
        pageLinks.add(u);

        Operation get = Operation
                .createGet(u)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    QueryTask page = o.getBody(QueryTask.class);
                    int nlinks = page.results.documentLinks.size();
                    for (int i = 0; i < nlinks; i++) {
                        ExampleServiceState document = Utils.fromJson(
                                page.results.documents.get(page.results.documentLinks.get(i)),
                                ExampleServiceState.class);
                        documents.add(document);
                    }
                    assertTrue(nlinks <= resultLimit);
                    verifyLinks(nextPageLink, pageLinks, page);

                    numberOfDocumentLinks[0] += nlinks;
                    if (page.results.nextPageLink == null || nlinks == 0) {
                        this.host.completeIteration();
                        return;
                    }

                    getNextPageResults(page.results.nextPageLink,
                            resultLimit, numberOfDocumentLinks, toDelete, documents, pageLinks);
                });

        this.host.send(get);
    }

    @Test
    public void paginatedSortOnLongAndSelfLink() throws Throwable {
        doPaginatedSortTestOnExampleStates(false);
        doPaginatedSortTestOnExampleStates(true);
    }

    public void doPaginatedSortTestOnExampleStates(boolean isDirect) throws Throwable {
        setUpHost();
        int serviceCount = 25;
        int resultLimit = 10;
        URI exampleFactoryURI = UriUtils.buildUri(this.host, ExampleFactoryService.SELF_LINK);
        List<URI> exampleServices = new ArrayList<>();
        this.host.testStart(serviceCount);
        Random r = new Random();
        for (int i = 0; i < serviceCount; i++) {
            ExampleServiceState s = new ExampleServiceState();
            s.name = UUID.randomUUID().toString();
            s.counter = new Long(Math.abs(r.nextLong()));
            s.documentSelfLink = s.name;

            exampleServices.add(UriUtils.buildUri(this.host.getUri(),
                    ExampleFactoryService.SELF_LINK, s.documentSelfLink));

            this.host.send(Operation.createPost(exampleFactoryURI)
                    .setBody(s)
                    .setCompletion(this.host.getCompletion()));

        }
        this.host.testWait();

        Query kindClause = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();

        QueryTask.Builder queryTaskBuilder = isDirect ? QueryTask.Builder.createDirectTask()
                : QueryTask.Builder.create();
        queryTaskBuilder
                .setQuery(kindClause)
                .orderDescending(ExampleServiceState.FIELD_NAME_COUNTER, TypeName.LONG)
                .addOption(QueryOption.EXPAND_CONTENT)
                .setResultLimit(resultLimit);

        QueryTask task = queryTaskBuilder.build();

        if (task.documentExpirationTimeMicros != 0) {
            // the value was set as an interval by the calling test. Make absolute here so
            // account for service creation above
            task.documentExpirationTimeMicros = Utils.getNowMicrosUtc()
                    + task.documentExpirationTimeMicros;
        }

        this.host.logThroughput();
        URI taskURI = this.host.createQueryTaskService(task, false,
                task.taskInfo.isDirect, task, null);

        if (!task.taskInfo.isDirect) {
            task = this.host.waitForQueryTaskCompletion(task.querySpec, 0, 0,
                    taskURI, false, false);
        }

        String nextPageLink = task.results.nextPageLink;
        assertNotNull(nextPageLink);

        List<ExampleServiceState> documents = Collections.synchronizedList(new ArrayList<>());
        final int[] numberOfDocumentLinks = { task.results.documentLinks.size() };
        assertTrue(numberOfDocumentLinks[0] == 0);

        List<URI> toDelete = new ArrayList<>(exampleServices);
        List<URI> pageLinks = new ArrayList<>();
        this.host.testStart(1);
        getNextPageResults(nextPageLink, resultLimit, numberOfDocumentLinks, toDelete, documents,
                pageLinks);
        this.host.testWait();

        assertEquals(serviceCount, numberOfDocumentLinks[0]);
        validateSortedResults(documents, ExampleServiceState.FIELD_NAME_COUNTER);

        // do another query but sort on self links
        numberOfDocumentLinks[0] = 0;

        queryTaskBuilder = isDirect ? QueryTask.Builder.createDirectTask() : QueryTask.Builder
                .create();
        queryTaskBuilder
                .setQuery(kindClause)
                .orderAscending(ServiceDocument.FIELD_NAME_SELF_LINK, TypeName.STRING)
                .addOption(QueryOption.EXPAND_CONTENT)
                .setResultLimit(resultLimit);
        task = queryTaskBuilder.build();
        taskURI = this.host.createQueryTaskService(task, false,
                task.taskInfo.isDirect, task, null);
        if (!task.taskInfo.isDirect) {
            task = this.host.waitForQueryTaskCompletion(task.querySpec, 0, 0,
                    taskURI, false, false);
        }

        nextPageLink = task.results.nextPageLink;
        assertNotNull(nextPageLink);

        documents = Collections.synchronizedList(new ArrayList<>());

        toDelete = new ArrayList<>(exampleServices);
        pageLinks.clear();
        this.host.testStart(1);
        getNextPageResults(nextPageLink, resultLimit, numberOfDocumentLinks, toDelete, documents,
                pageLinks);
        this.host.testWait();

        assertEquals(serviceCount, numberOfDocumentLinks[0]);
        validateSortedResults(documents, ServiceDocument.FIELD_NAME_SELF_LINK);

        this.host.testStart(toDelete.size());
        for (URI u : toDelete) {
            this.host.send(Operation.createDelete(u)
                    .setBody(new ServiceDocument())
                    .setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();
    }

    private void validateSortedResults(List<ExampleServiceState> documents, String fieldName) {
        ExampleServiceState prevDoc = documents.get(0);

        for (int i = 1; i < documents.size(); i++) {
            ExampleServiceState currentDoc = documents.get(i);
            this.host.log("%s", currentDoc.documentSelfLink);
            if (fieldName.equals(ServiceDocument.FIELD_NAME_SELF_LINK)) {
                int r = currentDoc.documentSelfLink.compareTo(prevDoc.documentSelfLink);
                assertTrue("Sort by self link failed", r > 0);
            } else {
                assertTrue("Sort Test Failed", currentDoc.counter < prevDoc.counter);
            }
            prevDoc = currentDoc;
        }
    }

    public void doSelfLinkTest(int serviceCount, int versionCount, boolean forceRemote)
            throws Throwable {

        String prefix = "testPrefix";
        List<URI> services = startQueryTargetServices(serviceCount);

        // start two different types of services, creating two sets of documents
        // first start the query validation service instances, setting the id
        // field
        // to the same value
        QueryValidationServiceState newState = new QueryValidationServiceState();
        newState.id = prefix + UUID.randomUUID().toString();
        newState = putStateOnQueryTargetServices(services, versionCount,
                newState);

        // issue a query that matches a specific link. This is essentially a primary key query
        Query query = Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, services.get(0).getPath())
                .build();

        QueryTask queryTask = QueryTask.Builder.create().setQuery(query).build();
        URI u = this.host.createQueryTaskService(queryTask, forceRemote);
        QueryTask finishedTaskState = this.host.waitForQueryTaskCompletion(queryTask.querySpec,
                services.size(), versionCount, u, forceRemote, true);
        assertTrue(finishedTaskState.results.documentLinks.size() == 1);

        // now make sure expand works. Issue same query, but enable expand
        queryTask.querySpec.options = EnumSet.of(QueryOption.EXPAND_CONTENT);
        u = this.host.createQueryTaskService(queryTask, forceRemote);
        finishedTaskState = this.host.waitForQueryTaskCompletion(queryTask.querySpec,
                services.size(), versionCount, u, forceRemote, true);
        assertTrue(finishedTaskState.results.documentLinks.size() == 1);
        assertTrue(finishedTaskState.results.documents.size() == 1);

        // test EXPAND with include deleted: Delete one service
        this.host.testStart(1);
        Operation delete = Operation.createDelete(services.remove(0))
                .setBody(new ServiceDocument())
                .setCompletion(this.host.getCompletion());
        this.host.send(delete);
        this.host.testWait();

        // search for the deleted services, we should get nothing back
        query = Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, delete.getUri().getPath())
                .build();
        queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.EXPAND_CONTENT)
                .setQuery(query)
                .build();
        u = this.host.createQueryTaskService(queryTask, forceRemote);
        finishedTaskState = this.host.waitForQueryTaskCompletion(queryTask.querySpec,
                services.size(), 1, u, forceRemote, true);
        assertTrue(finishedTaskState.results.documents.size() == 0);

        // add INCLUDE_DELETED and try again, we should get back one entry
        queryTask.querySpec.options = EnumSet.of(QueryOption.EXPAND_CONTENT,
                QueryOption.INCLUDE_DELETED);
        u = this.host.createQueryTaskService(queryTask, forceRemote);
        finishedTaskState = this.host.waitForQueryTaskCompletion(queryTask.querySpec,
                services.size(), 1, u, forceRemote, true);
        // the results will contain a "deleted" document
        assertEquals(1, finishedTaskState.results.documents.size());
        QueryValidationServiceState deletedState = Utils.fromJson(
                finishedTaskState.results.documents.values().iterator().next(),
                QueryValidationServiceState.class);

        assertEquals(Action.DELETE.toString(), deletedState.documentUpdateAction);
        // now request all versions
        queryTask.querySpec.options = EnumSet.of(QueryOption.EXPAND_CONTENT,
                QueryOption.INCLUDE_ALL_VERSIONS,
                QueryOption.INCLUDE_DELETED);
        u = this.host.createQueryTaskService(queryTask, forceRemote);
        finishedTaskState = this.host.waitForQueryTaskCompletion(queryTask.querySpec,
                services.size(), 1, u, forceRemote, true);
        // delete increments version, and initial service create counts as another.
        assertEquals(versionCount + 2, finishedTaskState.results.documents.size());

        // Verify that we maintain the lucene ordering
        EnumSet<QueryOption> currentOptions = queryTask.querySpec.options;
        query = Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, services.get(0).getPath())
                .build();

        queryTask = QueryTask.Builder.create().addOptions(currentOptions).setQuery(query).build();
        u = this.host.createQueryTaskService(queryTask, forceRemote);
        finishedTaskState = this.host.waitForQueryTaskCompletion(queryTask.querySpec,
                services.size(), 1, u, forceRemote, true);

        int ordinal = finishedTaskState.results.documentLinks.size();
        // we are taking advantage of the fact that they come in descending order
        // and _end with_ the document version in the refLink!
        Iterator<String> iter = finishedTaskState.results.documentLinks.iterator();
        while (iter.hasNext()) {
            String s = iter.next();
            assertTrue(s.endsWith(Integer.toString(--ordinal)));
        }
    }

    @Test
    public void numericRangeQuery() throws Throwable {
        doNumericRangeQueries("longValue", "doubleValue");
    }

    @Test
    public void numericRangeQueryOnCollection() throws Throwable {
        doNumericRangeQueries(QuerySpecification.buildCompositeFieldName("mapOfLongs", "long"),
                QuerySpecification.buildCompositeFieldName("mapOfDoubles", "double"));
    }

    private void doNumericRangeQueries(String longFieldName, String doubleFieldName)
            throws Throwable {
        setUpHost();
        int sc = this.serviceCount;
        int versionCount = 2;
        List<URI> services = startQueryTargetServices(sc);
        // the PUT will increment the long field, so we will do queries over its
        // range
        putStateOnQueryTargetServices(services, versionCount);

        // now issue a query that does an exact match on the String value using
        // the complete phrase
        long offset = 10;
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(longFieldName)
                .setNumericRange(
                        NumericRange.createLongRange(offset, sc - offset,
                                false, false));

        URI u = this.host.createQueryTaskService(QueryTask.create(q), false);
        QueryTask finishedTaskState = this.host.waitForQueryTaskCompletion(q,
                services.size(), versionCount, u, false, true);
        assertTrue(finishedTaskState.results != null);
        assertTrue(finishedTaskState.results.documentLinks != null);
        this.host
                .log("results %d, expected %d",
                        finishedTaskState.results.documentLinks.size(), sc - offset
                                * 2 - 1);
        assertTrue(finishedTaskState.results.documentLinks.size() == sc - offset
                * 2 - 1);

        // do inclusive range search
        q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(longFieldName).setNumericRange(
                NumericRange.createLongRange(0L, (long) (sc - 1), true, true));
        u = this.host.createQueryTaskService(QueryTask.create(q));
        finishedTaskState = this.host.waitForQueryTaskCompletion(q, services.size(),
                versionCount, u, false, true);
        assertTrue(finishedTaskState.results != null);
        assertTrue(finishedTaskState.results.documentLinks != null);
        assertTrue(finishedTaskState.results.documentLinks.size() == sc);

        // do min side open range search
        q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(longFieldName).setNumericRange(
                NumericRange.createLongRange(null, (long) (sc - 1), true, true));
        u = this.host.createQueryTaskService(QueryTask.create(q));
        finishedTaskState = this.host.waitForQueryTaskCompletion(q, services.size(),
                versionCount, u, false, true);
        assertTrue(finishedTaskState.results != null);
        assertTrue(finishedTaskState.results.documentLinks != null);
        assertTrue(finishedTaskState.results.documentLinks.size() == sc);

        // do max side open range search
        q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(longFieldName).setNumericRange(
                NumericRange.createLongRange(0L, null, true, true));
        u = this.host.createQueryTaskService(QueryTask.create(q));
        finishedTaskState = this.host.waitForQueryTaskCompletion(q, services.size(),
                versionCount, u, false, true);
        assertTrue(finishedTaskState.results != null);
        assertTrue(finishedTaskState.results.documentLinks != null);
        assertTrue(finishedTaskState.results.documentLinks.size() == sc);

        // double fields are 1 / 10 of the long fields
        // do double inclusive range search
        q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(doubleFieldName).setNumericRange(
                NumericRange.createDoubleRange(DOUBLE_MIN_OFFSET, DOUBLE_MIN_OFFSET + sc * 0.1,
                        true, true));
        u = this.host.createQueryTaskService(QueryTask.create(q));
        finishedTaskState = this.host.waitForQueryTaskCompletion(q, services.size(),
                versionCount, u, false, true);
        assertTrue(finishedTaskState.results != null);
        assertTrue(finishedTaskState.results.documentLinks != null);
        assertTrue(finishedTaskState.results.documentLinks.size() == sc);

        // do double range search with min inclusive
        q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(doubleFieldName).setNumericRange(
                NumericRange.createDoubleRange(DOUBLE_MIN_OFFSET, DOUBLE_MIN_OFFSET + sc * 0.05,
                        true, false));
        u = this.host.createQueryTaskService(QueryTask.create(q));
        finishedTaskState = this.host.waitForQueryTaskCompletion(q, services.size(),
                versionCount, u, false, true);
        assertTrue(finishedTaskState.results != null);
        assertTrue(finishedTaskState.results.documentLinks != null);
        assertTrue(finishedTaskState.results.documentLinks.size() == sc / 2);
    }

    @Test
    public void testTextMatch() throws Throwable {
        doTextMatchTest(false, false);
    }

    @Test
    public void testTextMatchRemote() throws Throwable {
        doTextMatchTest(true, false);
    }

    @Test
    public void testTextMatchRemoteDirect() throws Throwable {
        doTextMatchTest(true, true);
    }

    public void doTextMatchTest(boolean forceRemote, boolean isDirect) throws Throwable {
        setUpHost();
        int sc = this.serviceCount;
        int versionCount = 2;
        List<URI> services = startQueryTargetServices(sc);

        // PUT a new state on all services, with one field set to the same
        // value;
        QueryValidationServiceState newState = putStateOnQueryTargetServices(
                services, versionCount);

        // now issue a query that does an exact match on the String value using
        // the complete phrase
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        q.options = EnumSet.of(QueryOption.EXPAND_CONTENT);

        q.query.setTermPropertyName("stringValue")
                .setTermMatchValue(newState.stringValue)
                .setTermMatchType(MatchType.PHRASE);

        createWaitAndValidateQueryTask(versionCount, services, q, forceRemote, isDirect);

        // now do a "contains" search on terms using wild cards, although this
        // will be much slower
        String term = newState.stringValue.split(" ")[1];
        term = term.substring(1, term.length() - 2);
        term = UriUtils.URI_WILDCARD_CHAR + term + UriUtils.URI_WILDCARD_CHAR;

        q.query = new QueryTask.Query();
        q.query.setTermPropertyName("stringValue").setTermMatchValue(term)
                .setTermMatchType(MatchType.WILDCARD);
        createWaitAndValidateQueryTask(versionCount, services, q, forceRemote);
        // now do a "contains" search without using wild cards, just a plain
        // term. This only
        // works if the string is a phrase that can be tokenized with the
        // default tokenizers
        String word = TEXT_VALUE.split(" ")[1];

        q.query = new QueryTask.Query();
        q.query.setTermPropertyName("stringValue").setTermMatchValue(word)
                .setTermMatchType(MatchType.TERM);

        createWaitAndValidateQueryTask(versionCount, services, q, forceRemote);
    }

    @Test
    public void booleanQueries() throws Throwable {
        int versionCount = 2;
        int serviceCount = this.serviceCount;
        setUpHost();

        // Create pre-canned query terms
        Query kindClause = Query.Builder.create()
                .addKindFieldClause(QueryValidationServiceState.class)
                .build();

        Query termClause = Query.Builder.create()
                .addFieldClause("stringValue", TEXT_VALUE.split(" ")[1])
                .build();

        // Create and populate services using another test
        doKindMatchTest(serviceCount, versionCount, false);

        // Start with a simple, two clause boolean query
        Query query = Query.Builder.create()
                .addClause(kindClause)
                .addClause(termClause)
                .build();

        QueryTask queryTask = QueryTask.Builder.create().setQuery(query).build();
        URI u = this.host.createQueryTaskService(queryTask);
        QueryTask finishedTaskState = this.host.waitForQueryTaskCompletion(queryTask.querySpec,
                serviceCount, versionCount, u, false, false);

        // We filtered by kind, which prunes half the services we created
        validateSelfLinkResultCount(serviceCount / 2, finishedTaskState);

        // Test that cloning works; expect same results.
        // Use an empty query spec to make sure its the source query that is used for the clone
        queryTask = QueryTask.Builder.create().build();
        u = this.host.createQueryTaskService(queryTask, false,
                finishedTaskState.documentSelfLink);
        finishedTaskState = this.host.waitForQueryTaskCompletion(queryTask.querySpec,
                serviceCount, versionCount, u, false, true);
        validateSelfLinkResultCount(serviceCount / 2, finishedTaskState);

        // Run boolean query with just a single clause
        query = Query.Builder.create()
                .addClause(kindClause)
                .build();
        queryTask = QueryTask.Builder.create().setQuery(query).build();
        u = this.host.createQueryTaskService(queryTask);
        finishedTaskState = this.host.waitForQueryTaskCompletion(queryTask.querySpec,
                serviceCount, versionCount, u, false, true);
        validateSelfLinkResultCount(serviceCount / 2, finishedTaskState);

        // Run query with both a boolean clause and a term; expect failure
        query = Query.Builder.create()
                .addClause(kindClause)
                .build();
        query.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(Utils.buildKind(QueryValidationServiceState.class));
        queryTask = QueryTask.Builder.create().setQuery(query).build();
        u = this.host.createQueryTaskService(queryTask);
        finishedTaskState = this.host.waitForQueryTaskCompletion(queryTask.querySpec,
                serviceCount, versionCount, u, false, true, false);
        assertEquals(TaskState.TaskStage.FAILED, finishedTaskState.taskInfo.stage);

        // Another two clause boolean query with kind and string match
        query = Query.Builder.create()
                .addClause(kindClause)
                .addFieldClause("serviceLink", SERVICE_LINK_VALUE)
                .build();

        queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.EXPAND_CONTENT)
                .setQuery(query)
                .build();
        u = this.host.createQueryTaskService(queryTask);
        finishedTaskState = this.host.waitForQueryTaskCompletion(queryTask.querySpec,
                serviceCount, versionCount, u, false, true);

        // We filtered by kind, which prunes half the services we created
        validateSelfLinkResultCount(serviceCount / 2, finishedTaskState);

        // Run a double range query
        Query doubleMatchClause = Query.Builder
                .create()
                .addRangeClause("doubleValue",
                        NumericRange.createDoubleRange(DOUBLE_MIN_OFFSET + 0.2,
                                DOUBLE_MIN_OFFSET + 0.21, true, false))
                .build();
        query = Query.Builder.create()
                .addClause(kindClause)
                .addClause(doubleMatchClause)
                .build();
        queryTask = QueryTask.Builder.create().setQuery(query).build();
        u = this.host.createQueryTaskService(queryTask);
        finishedTaskState = this.host.waitForQueryTaskCompletion(queryTask.querySpec, serviceCount,
                versionCount, u,
                false, true);

        // Only one service should have doubleValue == 0.2
        validateSelfLinkResultCount(1, finishedTaskState);

        // Now created a nested boolean query with two boolean queries under a left and right branch
        Query leftParentClause = Query.Builder.create()
                .addClause(kindClause)
                .addClause(termClause)
                .build();

        // Make the right clause a NOT, so the results should be leftClauseResultCount - 1)
        Query rightParentClause = Query.Builder.create(Occurance.MUST_NOT_OCCUR)
                .addClause(doubleMatchClause)
                .addClause(kindClause)
                .build();

        query = Query.Builder.create()
                .addClause(leftParentClause)
                .addClause(rightParentClause)
                .build();

        queryTask = QueryTask.Builder.create().setQuery(query).build();
        u = this.host.createQueryTaskService(queryTask);
        finishedTaskState = this.host.waitForQueryTaskCompletion(queryTask.querySpec,
                serviceCount, versionCount, u, false, true);

        validateSelfLinkResultCount(serviceCount / 2 - 1, finishedTaskState);
    }

    @Test
    public void taskStateFieldQueries() throws Throwable {
        setUpHost();
        int sc = this.serviceCount;
        int versionCount = 1;
        boolean includeAllVersions = false;
        List<URI> services = startQueryTargetServices(sc);

        TaskStage stage = TaskState.TaskStage.CREATED;
        QueryValidationServiceState newState = doTaskStageQuery(sc, 1, services, stage,
                includeAllVersions);
        versionCount++;

        stage = TaskState.TaskStage.FINISHED;
        newState = doTaskStageQuery(sc, 1, services, stage, includeAllVersions);

        // now verify that if we ask for a stage that was set in an older version, we get zero
        // results back
        stage = TaskState.TaskStage.CREATED;
        int expectedResultCount = 0;
        newState = doTaskStageQuery(expectedResultCount, 1, services, stage,
                includeAllVersions);

        // now AGAIN, but include history (older versions)
        stage = TaskState.TaskStage.CREATED;
        expectedResultCount = sc;
        includeAllVersions = true;
        newState = doTaskStageQuery(expectedResultCount, versionCount, services, stage,
                includeAllVersions);

        // test that we can query on failure message
        newState.taskInfo.failure = new ServiceErrorResponse();
        newState.taskInfo.failure.message = "ERROR: Test failure";
        putStateOnQueryTargetServices(services, versionCount, newState);

        versionCount++;
        // now issue a query that does a match on the taskInfo.failure member
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName("taskInfo.failure.message")
                .setTermMatchValue("ERROR: Test failure");

        URI u = this.host.createQueryTaskService(QueryTask.create(q));
        QueryTask finishedTaskState = this.host.waitForQueryTaskCompletion(q,
                sc, versionCount, u, false, true);

        validateSelfLinkResultCount(sc, finishedTaskState);
    }

    @Test
    public void expireQueryTask() throws Throwable {
        setUpHost();

        // Create a query task with isDirect=false, testing that LuceneQueryTaskService
        // expires the task and sends a DELETE request.
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName("stringValue")
                .setTermMatchValue(TEXT_VALUE)
                .setTermMatchType(MatchType.PHRASE);

        QueryTask task = QueryTask.create(q);
        task.documentExpirationTimeMicros = Utils.getNowMicrosUtc() + TimeUnit.SECONDS.toMicros(1);

        URI taskURI = this.host.createQueryTaskService(task, false, false, task, null);
        this.host.waitForQueryTaskCompletion(q, 0, 0, taskURI, false, false);

        verifyTaskAutoExpiration(taskURI);
    }

    @Test
    public void expectedResultCountQuery() throws Throwable {
        setUpHost();
        int expectedCount = this.serviceCount;
        int batches = 2;
        int versions = 2;
        int sc = expectedCount / batches;

        Runnable createServices = () -> {
            try {
                List<URI> services = startQueryTargetServices(sc);

                putStateOnQueryTargetServices(services, versions);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        };

        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();

        q.expectedResultCount = Long.valueOf(expectedCount);

        q.query.setTermPropertyName("stringValue")
                .setTermMatchValue(TEXT_VALUE)
                .setTermMatchType(MatchType.PHRASE);

        long exp = Utils.getNowMicrosUtc() + TimeUnit.SECONDS.toMicros(30);
        QueryTask task = QueryTask.create(q);
        task.documentExpirationTimeMicros = exp;

        // Query results at this point will == 0, so query will be retried
        URI taskURI = this.host.createQueryTaskService(task, false,
                false, task, null);

        // Create services in batches so query retries will have results,
        // but at least 1 query with < expectedResultCount so the query will be retried
        for (int i = 1; i <= batches; i++) {
            // Would prefer: host.schedule(createServices, 100*i, TimeUnit.MILLISECONDS);
            // but can't have concurrent testWait()'s
            Thread.sleep(100 * i);
            createServices.run();
        }

        QueryTask taskState = this.host.waitForQueryTaskCompletion(q, expectedCount, 0,
                taskURI, false, false);

        assertEquals(expectedCount, taskState.results.documentLinks.size());

        // Test that expectedResultCount works with QueryOption.COUNT
        q.options = EnumSet.of(QueryOption.COUNT, QueryOption.INCLUDE_ALL_VERSIONS);
        task = QueryTask.create(q);
        task.documentExpirationTimeMicros = exp;
        taskURI = this.host.createQueryTaskService(task, false,
                false, task, null);
        taskState = this.host.waitForQueryTaskCompletion(q, expectedCount, 0,
                taskURI, false, false);
        assertEquals(Long.valueOf(expectedCount * versions), taskState.results.documentCount);
        assertEquals(0, taskState.results.documentLinks.size());

        // Increase expectedResultCount, should timeout and PATCH task state to FAILED
        q.expectedResultCount *= versions * 2;
        task = QueryTask.create(q);
        task.documentExpirationTimeMicros = Utils.getNowMicrosUtc() + TimeUnit.SECONDS.toMicros(1);

        taskURI = this.host.createQueryTaskService(task, false,
                false, task, null);

        verifyTaskAutoExpiration(taskURI);
        this.host.log("Query task has expired: %s", taskURI.getPath());
    }

    private URI doPaginatedQueryTest(QueryTask task, int sc, int resultLimit,
            List<URI> queryPageURIs, List<URI> targetServiceURIs) throws Throwable {
        List<URI> services = startQueryTargetServices(sc);
        if (targetServiceURIs == null) {
            targetServiceURIs = new ArrayList<>();
        }

        targetServiceURIs.addAll(services);
        QueryValidationServiceState newState = putStateOnQueryTargetServices(
                services, 1);

        task.querySpec.resultLimit = resultLimit;

        task.querySpec.query.setTermPropertyName("stringValue")
                .setTermMatchValue(newState.stringValue)
                .setTermMatchType(MatchType.PHRASE);

        if (task.documentExpirationTimeMicros != 0) {
            // the value was set as an interval by the calling test. Make absolute here so
            // account for service creation above
            task.documentExpirationTimeMicros = Utils.getNowMicrosUtc()
                    + task.documentExpirationTimeMicros;

        }

        URI taskURI = this.host.createQueryTaskService(task, false,
                task.taskInfo.isDirect, task, null);

        if (!task.taskInfo.isDirect) {
            task = this.host.waitForQueryTaskCompletion(task.querySpec, 0, 0,
                    taskURI, false, false);
        }

        String nextPageLink = task.results.nextPageLink;
        assertNotNull(nextPageLink);
        assertEquals(null, task.results.prevPageLink);

        assertNotNull(task.results);
        assertNotNull(task.results.documentLinks);

        final int[] numberOfDocumentLinks = { task.results.documentLinks.size() };

        assertEquals(0, numberOfDocumentLinks[0]);

        // update the index after the paginated query has been created to verify that its
        // stable while index searchers are updated
        services = startQueryTargetServices(10);
        targetServiceURIs.addAll(services);
        newState = putStateOnQueryTargetServices(services, 1);

        int numberOfPages = sc / resultLimit;
        this.host.testStart(Math.max(1, numberOfPages));

        getNextPageLinks(nextPageLink, resultLimit, numberOfDocumentLinks, queryPageURIs);

        this.host.testWait();

        assertEquals(sc, numberOfDocumentLinks[0]);

        return taskURI;
    }

    @Test
    public void paginatedQueries() throws Throwable {
        setUpHost();
        int sc = this.serviceCount;
        int numberOfPages = 2;
        int resultLimit = sc / numberOfPages;

        // indirect query, many results expected
        QueryTask task = QueryTask.create(new QuerySpecification()).setDirect(false);

        List<URI> pageServiceURIs = new ArrayList<>();
        List<URI> targetServiceURIs = new ArrayList<>();
        doPaginatedQueryTest(task, sc, resultLimit, pageServiceURIs, targetServiceURIs);

        // delete target services before doing next query to verify deleted documents are excluded
        this.host.testStart(targetServiceURIs.size());
        for (URI u : targetServiceURIs) {
            this.host.send(Operation.createDelete(u)
                    .setBody(new ServiceDocument())
                    .setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();

        sc = 1;
        // direct query, single result expected, plus verify all previously deleted and created
        // documents are ignored
        task = QueryTask.create(new QuerySpecification()).setDirect(true);
        pageServiceURIs = new ArrayList<>();
        targetServiceURIs = new ArrayList<>();
        doPaginatedQueryTest(task, sc, resultLimit, pageServiceURIs, targetServiceURIs);
        String nextPageLink = task.results.nextPageLink;
        assertNotNull(nextPageLink);

        // delete target services before doing next query to verify deleted documents are excluded
        this.host.testStart(targetServiceURIs.size());
        for (URI u : targetServiceURIs) {
            this.host.send(Operation.createDelete(u)
                    .setBody(new ServiceDocument())
                    .setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();

        sc = 0;

        // direct query, no results expected,
        task = QueryTask.create(new QuerySpecification()).setDirect(true);
        pageServiceURIs = new ArrayList<>();
        targetServiceURIs = new ArrayList<>();
        doPaginatedQueryTest(task, sc, resultLimit, pageServiceURIs, targetServiceURIs);
    }

    @Test
    public void paginatedQueriesWithExpirationValidation() throws Throwable {
        setUpHost();
        int sc = this.serviceCount;
        int numberOfPages = 5;
        int resultLimit = sc / numberOfPages;

        QueryTask task = QueryTask.create(new QuerySpecification()).setDirect(true);
        List<URI> serviceURIs = new ArrayList<>();
        long timeoutMillis = 3000;
        task.documentExpirationTimeMicros = TimeUnit.MILLISECONDS.toMicros(timeoutMillis);
        URI taskURI = doPaginatedQueryTest(task, sc, resultLimit, serviceURIs, null);

        // Test that task has expired
        verifyTaskAutoExpiration(taskURI);
        this.host.log("Starting page link expiration test");

        // Test that page services have expired and been deleted
        Date exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {
            this.host.testStart(serviceURIs.size());
            AtomicInteger remaining = new AtomicInteger(serviceURIs.size());

            for (URI u : serviceURIs) {
                Operation get = Operation.createGet(u).setCompletion((o, e) -> {
                    if (e != null && (e instanceof ServiceNotFoundException)) {
                        remaining.decrementAndGet();
                    }
                    this.host.completeIteration();
                });

                this.host.send(get);
            }

            this.host.testWait();
            if (remaining.get() == 0) {
                return;
            }
            Thread.sleep(timeoutMillis / 8);
        }

        throw new TimeoutException("Next page services should have expired");
    }

    private void getNextPageLinks(String nextPageLink, int resultLimit,
            final int[] numberOfDocumentLinks, final List<URI> serviceURIs) {

        URI u = UriUtils.buildUri(this.host, nextPageLink);
        serviceURIs.add(u);

        Operation get = Operation
                .createGet(u)
                .setCompletion((o, e) -> {
                    try {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }

                        QueryTask page = o.getBody(QueryTask.class);
                        int nlinks = page.results.documentLinks.size();
                        assertTrue(nlinks <= resultLimit);
                        verifyLinks(nextPageLink, serviceURIs, page);

                        numberOfDocumentLinks[0] += nlinks;

                        if (page.results.nextPageLink == null || nlinks == 0) {
                            if (numberOfDocumentLinks[0] == 0) {
                                this.host.completeIteration();
                            }
                            return;
                        }

                        this.host.completeIteration();
                        getNextPageLinks(page.results.nextPageLink,
                                resultLimit, numberOfDocumentLinks, serviceURIs);
                    } catch (Throwable e1) {
                        this.host.failIteration(e1);
                    }
                });

        this.host.send(get);
    }

    private void verifyLinks(String nextPageLink, List<URI> serviceURIs, QueryTask page) {
        assertEquals(LuceneQueryPageService.KIND, page.documentKind);
        assertNotEquals(nextPageLink, page.results.nextPageLink);
        assertNotEquals(nextPageLink, page.results.prevPageLink);

        if (serviceURIs.size() >= 1) {
            URI currentPageForwardUri = UriUtils.buildForwardToPeerUri(
                    UriUtils.buildUri(page.documentSelfLink), this.host.getId(),
                    ServiceUriPaths.DEFAULT_NODE_SELECTOR,
                    EnumSet.noneOf(ServiceOption.class));

            String currentPageLink = currentPageForwardUri.getPath()
                    + UriUtils.URI_QUERY_CHAR + currentPageForwardUri.getQuery();

            assertEquals(serviceURIs.get(serviceURIs.size() - 1), UriUtils.buildUri(this.host,
                    currentPageLink));
        }

        if (serviceURIs.size() >= 2) {
            assertEquals(serviceURIs.get(serviceURIs.size() - 2), UriUtils.buildUri(this.host,
                    page.results.prevPageLink));
        } else {
            assertEquals(null, page.results.prevPageLink);
        }
    }

    public QueryValidationServiceState doTaskStageQuery(int sc, int versionCount,
            List<URI> services, TaskStage stage, boolean includeAllVersions) throws Throwable {
        // test that we can query on task stage
        QueryValidationServiceState newState = new QueryValidationServiceState();
        newState.taskInfo = new TaskState();
        newState.taskInfo.stage = stage;
        if (sc > 0) {
            putStateOnQueryTargetServices(services, 1, newState);
        }

        // now issue a query that does a match on the taskInfo.stage member
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();

        if (includeAllVersions) {
            q.options = EnumSet.of(QueryOption.INCLUDE_ALL_VERSIONS);
        }

        q.query.setTermPropertyName("taskInfo.stage")
                .setTermMatchValue(newState.taskInfo.stage.toString());

        URI u = this.host.createQueryTaskService(QueryTask.create(q));
        QueryTask finishedTaskState = this.host.waitForQueryTaskCompletion(q,
                sc, versionCount, u, false, true);

        this.host.log("Result count : %d", finishedTaskState.results.documentLinks.size());
        validateSelfLinkResultCount(includeAllVersions ? sc * versionCount : sc, finishedTaskState);
        return newState;
    }

    private void createWaitAndValidateQueryTask(long versionCount,
            List<URI> services, QueryTask.QuerySpecification q, boolean forceRemote)
            throws Throwable {
        createWaitAndValidateQueryTask(versionCount, services, q, forceRemote, false);
    }

    @Test
    public void doNotRefreshSearcherTest() throws Throwable {
        setUpHost();
        int sc = 10;
        int iter = 10;
        List<URI> services = startQueryTargetServices(sc);
        QueryValidationServiceState newState = new QueryValidationServiceState();
        double currentStat;
        double newStat;
        int counter = 0;

        for (int i = 0; i < iter; i++) {
            newState.stringValue = "current";
            newState = putSimpleStateOnQueryTargetServices(services, newState);
            QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
            Query kindClause = new Query();
            kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                    .setTermMatchValue(Utils.buildKind(QueryValidationServiceState.class));
            q.query = kindClause;
            QueryTask task = QueryTask.create(q);
            task.setDirect(true);
            this.host.createQueryTaskService(task, false, task.taskInfo.isDirect, task, null);
            newState.stringValue = "new";
            newState = putSimpleStateOnQueryTargetServices(services, newState);

            URI luceneStatsUri = UriUtils.buildStatsUri(this.host.getDocumentIndexServiceUri());
            ServiceStats stats = this.host
                    .getServiceState(null, ServiceStats.class, luceneStatsUri);
            ServiceStat searcherUpdateBeforeQuery = stats.entries
                    .get(LuceneDocumentIndexService.STAT_NAME_SEARCHER_UPDATE_COUNT);
            currentStat = searcherUpdateBeforeQuery.latestValue;

            q = new QueryTask.QuerySpecification();
            kindClause = new Query();
            kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                    .setTermMatchValue(Utils.buildKind(QueryValidationServiceState.class));
            q.query = kindClause;
            q.options = EnumSet.of(QueryOption.DO_NOT_REFRESH);
            task = QueryTask.create(q);
            task.setDirect(true);
            this.host.createQueryTaskService(task, false, task.taskInfo.isDirect, task, null);

            stats = this.host.getServiceState(null, ServiceStats.class, luceneStatsUri);
            ServiceStat searcherUpdateAfterQuery = stats.entries
                    .get(LuceneDocumentIndexService.STAT_NAME_SEARCHER_UPDATE_COUNT);
            newStat = searcherUpdateAfterQuery.latestValue;
            if (currentStat == newStat) {
                counter++;
            }
        }

        assertTrue(String.format("Could not re-use index searcher in %d attempts", iter),
                counter > 0);
    }

    private void createWaitAndValidateQueryTask(long versionCount,
            List<URI> services, QueryTask.QuerySpecification q, boolean forceRemote,
            boolean isDirect)
            throws Throwable {
        QueryTask task = QueryTask.create(q).setDirect(isDirect);
        if (isDirect) {
            task.documentExpirationTimeMicros = Utils.getNowMicrosUtc()
                    + TimeUnit.MILLISECONDS.toMicros(100);
        }
        URI u = this.host.createQueryTaskService(task, forceRemote,
                isDirect, task, null);
        if (!isDirect) {
            task = this.host.waitForQueryTaskCompletion(q, services.size(), (int) versionCount, u,
                    forceRemote, true);
        }

        if (q.options != null && q.options.contains(QueryOption.COUNT)) {
            assertTrue(task.results.documentCount != null);
            assertTrue(task.results.documentCount == services.size() * (versionCount + 1));
            return;
        }

        validateFinishedQueryTask(services, task);

        if (isDirect) {
            verifyTaskAutoExpiration(u);
        }

        if (q.options == null
                || !q.options.contains(QueryOption.EXPAND_CONTENT)) {
            return;
        }
        assertTrue(task.results.documentLinks.size() == task.results.documents
                .size());
    }

    @Test
    public void toMatchValue() throws Throwable {
        final String str = "aaa";
        final Enum<?> enumV = TaskStage.CANCELLED;
        final String uriStr = "http://about.drekware.com";
        final URI uriV = URI.create(uriStr);

        // Object-based calls
        assertNull(QuerySpecification.toMatchValue((Object) null));
        assertEquals(str, QuerySpecification.toMatchValue(str));
        assertEquals("CANCELLED", QuerySpecification.toMatchValue((Object) enumV));
        assertEquals(uriStr, QuerySpecification.toMatchValue((Object) uriV));
        assertEquals("2345", QuerySpecification.toMatchValue(2345L));
        assertEquals("true", QuerySpecification.toMatchValue((Object) true));
        assertEquals("false", QuerySpecification.toMatchValue((Object) false));

        // Boolean-specific calls
        assertEquals("true", QuerySpecification.toMatchValue(true));
        assertEquals("false", QuerySpecification.toMatchValue(false));

        // Enum-specific calls
        assertNull(QuerySpecification.toMatchValue((Enum<?>) null));
        assertEquals("CANCELLED", QuerySpecification.toMatchValue(enumV));

        // URI-specific calls
        assertNull(QuerySpecification.toMatchValue((URI) null));
        assertEquals(uriStr, QuerySpecification.toMatchValue(uriV));
    }

    private void verifyTaskAutoExpiration(URI u) throws Throwable {
        // test task expiration
        Date exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {
            Thread.sleep(100);
            ServiceDocumentQueryResult r = this.host.getServiceState(null,
                    ServiceDocumentQueryResult.class,
                    UriUtils.buildUri(this.host, LuceneQueryTaskFactoryService.class));

            if (r.documentLinks != null) {
                boolean taskExists = false;

                for (String link : r.documentLinks) {
                    if (u.getPath().equals(link)) {
                        taskExists = true;
                        break;
                    }
                }

                if (!taskExists) {
                    return;
                }
            }
        }

        throw new TimeoutException("Task should have expired");
    }

    private void validateFinishedQueryTask(List<URI> services,
            QueryTask finishedTaskState) {
        validateSelfLinkResultCount(services.size(), finishedTaskState);
    }

    private void validateSelfLinkResultCount(int expectedLinks, QueryTask finishedTaskState) {
        assertNotNull(finishedTaskState.results);
        assertNotNull(finishedTaskState.taskInfo.durationMicros);
        assertNotNull(finishedTaskState.results.documentLinks);
        assertEquals(expectedLinks, finishedTaskState.results.documentLinks.size());
    }

    private QueryValidationServiceState putStateOnQueryTargetServices(
            List<URI> services, int versionsPerService) throws Throwable {
        QueryValidationServiceState newState = new QueryValidationServiceState();
        newState.stringValue = TEXT_VALUE;
        return putStateOnQueryTargetServices(services, versionsPerService,
                newState);
    }

    private QueryValidationServiceState putStateOnQueryTargetServices(
            List<URI> services, int versionsPerService,
            QueryValidationServiceState templateState) throws Throwable {

        this.host.testStart(services.size() * versionsPerService);
        Random r = new Random();
        long k = 0;
        templateState.mapOfLongs = new HashMap<>();
        templateState.mapOfDoubles = new HashMap<>();
        for (URI u : services) {
            templateState.longValue = k;
            templateState.doubleValue = (double) k++;
            templateState.doubleValue *= 0.1;
            templateState.doubleValue += DOUBLE_MIN_OFFSET;
            templateState.mapOfLongs.put("long", templateState.longValue);
            templateState.mapOfDoubles.put("double", templateState.doubleValue);
            templateState.stringValue = TEXT_VALUE;
            templateState.serviceLink = SERVICE_LINK_VALUE;
            for (int i = 0; i < versionsPerService; i++) {
                // change all other fields, per service
                templateState.booleanValue = r.nextBoolean();
                templateState.id = Utils.getNowMicrosUtc() + "";
                templateState.dateValue = new Date(System.nanoTime() / 1000);

                if (templateState.exampleValue != null) {
                    templateState.exampleValue.name = Utils.getNowMicrosUtc() + "";
                }

                Operation put = Operation.createPut(u).setBody(templateState)
                        .setCompletion(this.host.getCompletion());
                this.host.send(put);
            }
        }

        this.host.testWait();
        this.host.logThroughput();
        return templateState;
    }

    private QueryValidationServiceState putSimpleStateOnQueryTargetServices(
            List<URI> services,
            QueryValidationServiceState templateState) throws Throwable {

        this.host.testStart(services.size());
        for (URI u : services) {
            templateState.id = Utils.getNowMicrosUtc() + "";
            Operation put = Operation.createPut(u).setBody(templateState)
                    .setCompletion(this.host.getCompletion());
            this.host.send(put);
        }

        this.host.testWait();
        this.host.logThroughput();
        return templateState;
    }

    private List<URI> startQueryTargetServices(int serviceCount)
            throws Throwable {
        return startQueryTargetServices(serviceCount, new QueryValidationServiceState());
    }

    private List<URI> startQueryTargetServices(int serviceCount,
            QueryValidationServiceState initState)
            throws Throwable {
        List<URI> queryValidationServices = new ArrayList<>();
        List<Service> services = this.host.doThroughputServiceStart(
                serviceCount, QueryValidationTestService.class,
                initState,
                null, null);

        for (Service s : services) {
            queryValidationServices.add(s.getUri());
        }
        return queryValidationServices;

    }

    @Test
    public void testQueryBuilderShouldOccur() throws Throwable {
        setUpHost();
        URI exampleFactoryUri = UriUtils.buildUri(this.host, ExampleFactoryService.SELF_LINK);
        URI tenantFactoryUri = UriUtils.buildUri(this.host, TenantFactoryService.SELF_LINK);
        this.host.testStart(2);

        ExampleServiceState exampleServiceState = new ExampleServiceState();
        exampleServiceState.name = "Foo";
        exampleServiceState.counter = 5L;
        exampleServiceState.keyValues.put("exampleKey", "exampleValue");
        Operation postExample = Operation.createPost(exampleFactoryUri)
                .setBody(exampleServiceState)
                .setCompletion(this.host.getCompletion());
        this.host.send(postExample);

        TenantState tenantState = new TenantState();
        tenantState.name = "Pepsi";
        Operation postTenant = Operation.createPost(tenantFactoryUri)
                .setBody(tenantState)
                .setCompletion(this.host.getCompletion());
        this.host.send(postTenant);

        this.host.testWait();

        QuerySpecification spec = new QuerySpecification();
        spec.query = Query.Builder.create()
                .addKindFieldClause(TenantState.class, Occurance.SHOULD_OCCUR)
                .addKindFieldClause(ExampleServiceState.class, Occurance.SHOULD_OCCUR)
                .build();
        this.host.createAndWaitSimpleDirectQuery(spec, 2, 2);

        spec.query = Query.Builder.create()
                .addKindFieldClause(TenantState.class, Occurance.SHOULD_OCCUR)
                .build();
        this.host.createAndWaitSimpleDirectQuery(spec, 2, 1);

        spec.query = Query.Builder.create()
                .addFieldClause("name", "Pepsi", Occurance.SHOULD_OCCUR)
                .addKindFieldClause(ExampleServiceState.class, Occurance.SHOULD_OCCUR)
                .build();
        this.host.createAndWaitSimpleDirectQuery(spec, 2, 2);

        spec.query = Query.Builder
                .create()
                .addCompositeFieldClause(ExampleServiceState.FIELD_NAME_KEY_VALUES, "exampleKey",
                        "exampleValue",
                        Occurance.SHOULD_OCCUR)
                .addKindFieldClause(TenantState.class, Occurance.SHOULD_OCCUR)
                .build();
        this.host.createAndWaitSimpleDirectQuery(spec, 2, 2);

        spec.query = Query.Builder
                .create()
                .addCompositeFieldClause(ExampleServiceState.FIELD_NAME_KEY_VALUES, "exampleKey",
                        "exampleValue",
                        Occurance.SHOULD_OCCUR)
                .addKindFieldClause(TenantState.class, Occurance.MUST_OCCUR)
                .build();
        this.host.createAndWaitSimpleDirectQuery(spec, 2, 1);

        spec.query = Query.Builder
                .create()
                .addRangeClause(ExampleServiceState.FIELD_NAME_COUNTER,
                        NumericRange.createEqualRange(exampleServiceState.counter),
                        Occurance.SHOULD_OCCUR)
                .addKindFieldClause(TenantState.class, Occurance.SHOULD_OCCUR)
                .build();
        this.host.createAndWaitSimpleDirectQuery(spec, 2, 2);
    }
}
