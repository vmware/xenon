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

import java.net.ProtocolException;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Operation.OperationOption;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceConfiguration;
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
import com.vmware.xenon.common.ServiceSubscriptionState.ServiceSubscriber;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.TestResults;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;

import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask.Builder;
import com.vmware.xenon.services.common.QueryTask.NumericRange;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Occurance;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.SortOrder;
import com.vmware.xenon.services.common.QueryTask.QueryTerm;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.xenon.services.common.QueryValidationTestService.QueryValidationServiceState;
import com.vmware.xenon.services.common.TenantService.TenantState;

public class TestQueryTaskService {

    private static final String TEXT_VALUE = "the decentralized control plane is a nice framework for queries";
    private static final String STRING_VALUE = "First@Last.com";
    private static final String SERVICE_LINK_VALUE = "provisioning/dhcp-subnets/192.4.0.0/16";
    private static final long LONG_START_VALUE = -10;
    private static final double DOUBLE_MIN_OFFSET = -2.0;
    private static final int SERVICE_LINK_COUNT = 10;

    public int iterationCount = 2;
    public int serviceCount = 50;
    public int queryCount = 10;
    public int updateCount = 10;

    private VerificationHost host;

    @Rule
    public TestResults testResults = new TestResults();

    private void setUpHost() throws Throwable {
        setUpHost(0);
    }

    private void setUpHost(int responsePayloadSizeLimit) throws Throwable {
        if (this.host != null) {
            return;
        }

        this.host = VerificationHost.create(0);
        if (responsePayloadSizeLimit > 0) {
            this.host.setResponsePayloadSizeLimit(responsePayloadSizeLimit);
        }
        CommandLineArgumentParser.parseFromProperties(this.host);
        CommandLineArgumentParser.parseFromProperties(this);
        try {
            this.host.setStressTest(this.host.isStressTest);
            // disable synchronization so it does not interfere with the various test assumptions
            // on index stats.
            this.host.setPeerSynchronizationEnabled(false);
            this.host.start();
            if (!this.host.isStressTest()) {
                this.host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS
                        .toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
                this.host.toggleServiceOptions(this.host.getDocumentIndexServiceUri(),
                        EnumSet.of(ServiceOption.INSTRUMENTATION),
                        null);
            }

        } catch (Throwable e) {
            throw new Exception(e);
        }
    }

    @After
    public void tearDown() throws Exception {
        LuceneDocumentIndexService
                .setImplicitQueryResultLimit(LuceneDocumentIndexService.DEFAULT_QUERY_RESULT_LIMIT);
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
        PropertyDescription nestedPd;
        ServiceDocumentDescription.Builder b = ServiceDocumentDescription.Builder.create();

        QueryValidationServiceState s = new QueryValidationServiceState();
        ServiceDocumentDescription sdd = b.buildDescription(s.getClass(),
                EnumSet.of(ServiceOption.PERSISTENCE));

        final int expectedCustomFields = 36;
        final int expectedBuiltInFields = 10;
        // Verify the reflection of the root document
        assertTrue(sdd.propertyDescriptions != null && !sdd.propertyDescriptions.isEmpty());
        assertEquals(expectedCustomFields + expectedBuiltInFields, sdd.propertyDescriptions.size());

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
        assertEquals(descriptionsPerType.get(TypeName.LONG), (Long) (1L + 4L + 3L));
        assertTrue(descriptionsPerType.get(TypeName.PODO) == 3L);
        assertTrue(descriptionsPerType.get(TypeName.COLLECTION) == 8L);
        assertTrue(descriptionsPerType.get(TypeName.STRING) == 6L + 5L);
        assertTrue(descriptionsPerType.get(TypeName.DATE) == 1L);
        assertTrue(descriptionsPerType.get(TypeName.DOUBLE) == 4L);
        assertTrue(descriptionsPerType.get(TypeName.BYTES) == 1L);

        pd = sdd.propertyDescriptions.get("exampleValue");
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.PODO));
        assertTrue(pd.fieldDescriptions != null);
        assertTrue(pd.fieldDescriptions.size() == 8 + expectedBuiltInFields);
        assertTrue(pd.fieldDescriptions.get("keyValues") != null);

        pd = sdd.propertyDescriptions.get("nestedComplexValue");
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.PODO));
        assertTrue(pd.fieldDescriptions != null);
        assertTrue(pd.fieldDescriptions.size() == 4);
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
        assertTrue(pd.typeName.equals(TypeName.COLLECTION));
        assertTrue(pd.elementDescription != null);
        assertTrue(pd.elementDescription.typeName.equals(TypeName.STRING));

        pd = sdd.propertyDescriptions.get("arrayOfExampleValues");
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.COLLECTION));
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
        nestedPd = pd.fieldDescriptions.get("link");
        assertEquals(EnumSet.of(PropertyUsageOption.LINK), nestedPd.usageOptions);
        assertEquals("some/service", nestedPd.exampleValue);
        nestedPd = pd.fieldDescriptions.get("anotherLink");
        assertEquals(EnumSet.of(PropertyUsageOption.LINK), nestedPd.usageOptions);
        assertEquals("some/service", nestedPd.exampleValue);

        // Verify Documentation annotation
        pd = sdd.propertyDescriptions.get("longValue");
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.LONG));
        assertEquals(EnumSet.of(PropertyUsageOption.OPTIONAL), pd.usageOptions);
        assertEquals("a Long value", pd.propertyDocumentation);

        // Verify multiple Usage annotations are processed correctly
        pd = sdd.propertyDescriptions.get(QueryValidationServiceState.FIELD_NAME_SERVICE_LINK);
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.STRING));
        assertEquals(EnumSet.of(PropertyUsageOption.OPTIONAL,
                PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL, PropertyUsageOption.LINK),
                pd.usageOptions);
        assertEquals("some/service", pd.exampleValue);

        // Verify implicit Usage and Indexing annotations are added correctly
        pd = sdd.propertyDescriptions.get(QueryValidationServiceState.FIELD_NAME_SERVICE_LINKS);
        assertTrue(pd != null);
        assertTrue(pd.typeName.equals(TypeName.COLLECTION));
        assertEquals(EnumSet.of(PropertyUsageOption.OPTIONAL, PropertyUsageOption.LINKS),
                pd.usageOptions);
        assertEquals(EnumSet.of(PropertyIndexingOption.EXPAND), pd.indexingOptions);
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

        // Create a continuous query task with no expiration time.
        String textValue = UUID.randomUUID().toString();
        Query query = Query.Builder.create()
                .addFieldClause(QueryValidationServiceState.FIELD_NAME_TEXT_VALUE, textValue)
                .build();
        QueryTask queryTask = QueryTask.Builder.create()
                .setQuery(query)
                .addOptions(EnumSet.of(QueryOption.CONTINUOUS, QueryOption.EXPAND_CONTENT))
                .build();
        queryTask.documentExpirationTimeMicros = Long.MAX_VALUE;
        URI queryTaskUri = this.host.createQueryTaskService(queryTask);

        // Create a subscription to the continuous query task.
        AtomicReference<TestContext> ctxRef = new AtomicReference<>();
        Consumer<Operation> notificationConsumer = (notifyOp) -> {
            QueryTask body = notifyOp.getBody(QueryTask.class);
            if (body.results == null || body.results.documentLinks.isEmpty()) {
                // Query has not completed or no matching services exist yet
                return;
            }

            TestContext ctx = ctxRef.get();
            if (body.results.continuousResults == null) {
                this.host.log("Query task result %s", Utils.toJsonHtml(body.results));
                ctx.fail(new IllegalStateException("Continuous results expected"));
                return;
            }

            for (Object doc : body.results.documents.values()) {
                QueryValidationServiceState state = Utils.fromJson(doc,
                        QueryValidationServiceState.class);
                if (!textValue.equals(state.textValue)) {
                    ctx.fail(new IllegalStateException("Unexpected document: "
                            + Utils.toJsonHtml(state)));
                    return;
                }
            }

            ctx.complete();
        };

        TestContext subscriptionCtx = this.host.testCreate(1);
        Operation post = Operation.createPost(queryTaskUri).setReferer(this.host.getReferer())
                .setCompletion(subscriptionCtx.getCompletion());
        this.host.startSubscriptionService(post, notificationConsumer);
        this.host.testWait(subscriptionCtx);

        // Since the continuous query is not direct, it becomes active in the index asynchronously
        // with respect to the completion of the initial POST operation. Poll the index stats until
        // they indicate that the continuous query filter is active.
        this.host.waitFor("task never activated", () -> {
            ServiceStats indexStats = this.host.getServiceState(null, ServiceStats.class,
                    UriUtils.buildStatsUri(this.host.getDocumentIndexServiceUri()));
            ServiceStat activeQueryStat = indexStats.entries.get(
                    LuceneDocumentIndexService.STAT_NAME_ACTIVE_QUERY_FILTERS
                            + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR);
            return activeQueryStat != null && activeQueryStat.latestValue >= 1.0;
        });

        // Start some services which match the continuous query filter and wait for notifications.
        QueryValidationServiceState newState = new QueryValidationServiceState();
        newState.textValue = textValue;

        TestContext queryCtx = this.host.testCreate(this.serviceCount);
        ctxRef.set(queryCtx);
        List<URI> initialServices = startQueryTargetServices(this.serviceCount, newState);
        queryCtx.await();

        // Update the services with new state and wait for notifications.
        newState = new QueryValidationServiceState();
        String stringValue = UUID.randomUUID().toString();
        newState.stringValue = stringValue;
        newState.textValue = textValue;

        queryCtx = this.host.testCreate(this.serviceCount);
        ctxRef.set(queryCtx);
        putSimpleStateOnQueryTargetServices(initialServices, newState);
        queryCtx.await();

        // Create a new continuous query with the COUNT option and no expiration time. Note that
        // the continuous query filter becomes active in the index before the initial results are
        // sent to the query task, so it's not necessary to poll the index stats to wait for it to
        // become active here.
        Query countQuery = Query.Builder.create()
                .addFieldClause(QueryValidationServiceState.FIELD_NAME_STRING_VALUE, stringValue)
                .build();
        QueryTask countQueryTask = QueryTask.Builder.create()
                .setQuery(countQuery)
                .addOptions(EnumSet.of(QueryOption.CONTINUOUS, QueryOption.COUNT))
                .build();
        countQueryTask.documentExpirationTimeMicros = Long.MAX_VALUE;
        URI countQueryTaskUri = this.host.createQueryTaskService(countQueryTask);

        // Wait for the existing services to be reflected in the COUNT query.
        this.host.waitFor("Continuous query failed to find existing services", () -> {
            QueryTask qt = this.host.getServiceState(null, QueryTask.class, countQueryTaskUri);
            return (qt.results != null && qt.results.documentCount == this.serviceCount);
        });

        // Create a subscription to the continuous COUNT query.
        AtomicReference<TestContext> countCtxRef = new AtomicReference<>();
        AtomicReference<QueryTask> queryTaskRef = new AtomicReference<>();
        Consumer<Operation> countNotificationConsumer = (notifyOp) -> {
            QueryTask body = notifyOp.getBody(QueryTask.class);
            if (body.results == null) {
                return;
            }

            TestContext ctx = countCtxRef.get();
            if (body.results.continuousResults == null) {
                this.host.log("Query task body %s", Utils.toJsonHtml(body));
                ctx.fail(new IllegalStateException("Continuous results expected"));
                return;
            }

            // Subscription notifications are not guaranteed to be delivered in order, and can be
            // processed in parallel since notification target services are StatelessService
            // instances with ServiceOption.CONCURRENT_UPDATE_HANDLING enabled (by default). Use
            // synchronization to ensure that our view of the query task state is consistent and
            // strictly increasing.
            synchronized (queryTaskRef) {
                QueryTask current = queryTaskRef.get();
                if (current == null || body.documentVersion > current.documentVersion) {
                    queryTaskRef.set(body);
                }
            }

            ctx.complete();
        };

        subscriptionCtx = this.host.testCreate(1);
        post = Operation.createPost(countQueryTaskUri).setReferer(this.host.getReferer())
                .setCompletion(subscriptionCtx.getCompletion());
        this.host.startSubscriptionService(post, countNotificationConsumer);
        this.host.testWait(subscriptionCtx);

        // Create some new services which match both queries, wait for notifications, and verify
        // that the COUNT query reflects both the existing and the new services.
        queryCtx = this.host.testCreate(this.serviceCount);
        ctxRef.set(queryCtx);
        TestContext countQueryCtx = this.host.testCreate(this.serviceCount);
        countCtxRef.set(countQueryCtx);
        List<URI> createdServices = startQueryTargetServices(this.serviceCount, newState);
        this.host.testWait(queryCtx);
        this.host.testWait(countQueryCtx);
        assertEquals(2 * this.serviceCount, (long) queryTaskRef.get().results.documentCount);

        QueryTask qt = this.host.getServiceState(null, QueryTask.class, countQueryTaskUri);
        assertEquals(2 * this.serviceCount, (long) qt.results.documentCount);
        assertEquals(this.serviceCount, (long) qt.results.continuousResults.documentCountAdded);
        assertEquals(0, (long) qt.results.continuousResults.documentCountUpdated);
        assertEquals(0, (long) qt.results.continuousResults.documentCountDeleted);

        // Update the initial services with a new state which matches the original query but not
        // the COUNT query, wait for notifications, and verify that the COUNT query did not receive
        // notifications for the updates.
        newState = new QueryValidationServiceState();
        newState.textValue = textValue;
        newState.stringValue = "stringValue";

        queryCtx = this.host.testCreate(this.serviceCount);
        ctxRef.set(queryCtx);
        putSimpleStateOnQueryTargetServices(initialServices, newState);
        this.host.testWait(queryCtx);
        assertEquals(2 * this.serviceCount, (long) queryTaskRef.get().results.documentCount);

        qt = this.host.getServiceState(null, QueryTask.class, countQueryTaskUri);
        assertEquals(2 * this.serviceCount, (long) qt.results.documentCount);
        assertEquals(this.serviceCount, (long) qt.results.continuousResults.documentCountAdded);
        assertEquals(0, (long) qt.results.continuousResults.documentCountUpdated);
        assertEquals(0, (long) qt.results.continuousResults.documentCountDeleted);

        // Update the rest of the services with a new state which matches both queries, wait for
        // notifications, and verify that the COUNT query reflects the updates.
        newState = new QueryValidationServiceState();
        newState.textValue = textValue;
        newState.stringValue = stringValue;

        queryCtx = this.host.testCreate(this.serviceCount);
        ctxRef.set(queryCtx);
        countQueryCtx = this.host.testCreate(this.serviceCount);
        countCtxRef.set(countQueryCtx);
        putSimpleStateOnQueryTargetServices(createdServices, newState);
        this.host.testWait(queryCtx);
        this.host.testWait(countQueryCtx);
        assertEquals(2 * this.serviceCount, (long) queryTaskRef.get().results.documentCount);

        qt = this.host.getServiceState(null, QueryTask.class, countQueryTaskUri);
        assertEquals(2 * this.serviceCount, (long) qt.results.documentCount);
        assertEquals(this.serviceCount, (long) qt.results.continuousResults.documentCountAdded);
        assertEquals(this.serviceCount, (long) qt.results.continuousResults.documentCountUpdated);
        assertEquals(0, (long) qt.results.continuousResults.documentCountDeleted);

        // Delete some of the services from each batch, wait for notifications, and verify that the
        // COUNT query reflects the updates which match its query specification.
        List<URI> initialServicesToDelete = initialServices.stream().limit(this.serviceCount / 2)
                .collect(Collectors.toList());
        List<URI> createdServicesToDelete = createdServices.stream().limit(this.serviceCount / 2)
                .collect(Collectors.toList());

        queryCtx = this.host.testCreate(initialServicesToDelete.size()
                + createdServicesToDelete.size());
        ctxRef.set(queryCtx);
        countQueryCtx = this.host.testCreate(createdServicesToDelete.size());
        countCtxRef.set(countQueryCtx);

        for (URI u : initialServicesToDelete) {
            this.host.send(Operation.createDelete(u));
        }

        for (URI u : createdServicesToDelete) {
            this.host.send(Operation.createDelete(u));
        }

        this.host.testWait(queryCtx);
        this.host.testWait(countQueryCtx);
        long documentCount = 2 * this.serviceCount - createdServicesToDelete.size();
        assertEquals(documentCount, (long) queryTaskRef.get().results.documentCount);

        qt = this.host.getServiceState(null, QueryTask.class, countQueryTaskUri);
        assertEquals(documentCount, (long) qt.results.documentCount);
        assertEquals(this.serviceCount, (long) qt.results.continuousResults.documentCountAdded);
        assertEquals(this.serviceCount, (long) qt.results.continuousResults.documentCountUpdated);
        assertEquals(createdServicesToDelete.size(),
                (long) qt.results.continuousResults.documentCountDeleted);

        // Expire the rest of the services, wait for notifications, and verify that the COUNT query
        // reflects the updates which match its query specification. Note that two notifications
        // are expected for each service: one for the update to set the expiration time, and one
        // for the subsequent delete notification as part of service expiration.
        List<URI> initialServicesToExpire = initialServices.stream()
                .filter(u -> !initialServicesToDelete.contains(u)).collect(Collectors.toList());
        List<URI> createdServicesToExpire = createdServices.stream()
                .filter(u -> !createdServicesToDelete.contains(u)).collect(Collectors.toList());

        queryCtx = this.host.testCreate(2 * initialServicesToExpire.size()
                + 2 * createdServicesToExpire.size());
        ctxRef.set(queryCtx);
        countQueryCtx = this.host.testCreate(2 * createdServicesToExpire.size());
        countCtxRef.set(countQueryCtx);

        newState = new QueryValidationServiceState();
        newState.documentExpirationTimeMicros = 1;

        for (URI u : initialServicesToExpire) {
            this.host.send(Operation.createPatch(u).setBody(newState));
        }

        for (URI u : createdServicesToExpire) {
            this.host.send(Operation.createPatch(u).setBody(newState));
        }

        this.host.testWait(queryCtx);
        this.host.testWait(countQueryCtx);
        assertEquals(this.serviceCount, (long) queryTaskRef.get().results.documentCount);

        qt = this.host.getServiceState(null, QueryTask.class, countQueryTaskUri);
        assertEquals(this.serviceCount, (long) qt.results.documentCount);
        assertEquals(this.serviceCount, (long) qt.results.continuousResults.documentCountAdded);
        assertEquals(this.serviceCount + createdServicesToExpire.size(),
                (long) qt.results.continuousResults.documentCountUpdated);
        assertEquals(this.serviceCount, (long) qt.results.continuousResults.documentCountDeleted);
    }

    /**
     * This tests a specific bug we encountered that a continuous query task
     * with replay would fail when there was state to replay. We never got the
     * notification for the replay because kryo through an exception: Class
     * cannot be created (missing no-arg constructor)
     */
    @Test
    public void continuousQueryTaskWithReplay() throws Throwable {

        setUpHost();

        // Create services before we create the query
        QueryValidationServiceState newState = new QueryValidationServiceState();
        newState.textValue = UUID.randomUUID().toString();
        startQueryTargetServices(1, newState);

        // Create query task
        Query query = Query.Builder.create()
                .addFieldClause(QueryValidationServiceState.FIELD_NAME_TEXT_VALUE,
                        "*", MatchType.WILDCARD)
                .build();
        QueryTask task = QueryTask.Builder.create()
                .addOptions(EnumSet.of(QueryOption.CONTINUOUS, QueryOption.EXPAND_CONTENT))
                .setQuery(query)
                .build();

        // If the expiration time is not set, then the query will receive the default
        // query expiration time of 1 minute.
        task.documentExpirationTimeMicros = Utils
                .fromNowMicrosUtc(TimeUnit.DAYS.toMicros(1));

        URI queryTaskUri = this.host.createQueryTaskService(
                UriUtils.buildUri(this.host.getUri(), ServiceUriPaths.CORE_QUERY_TASKS),
                task, false, false, task, null);

        // Wait for query task to have data
        QueryTask queryTaskResponse = null;
        Date exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {
            queryTaskResponse = this.host.getServiceState(
                    null, QueryTask.class, queryTaskUri);
            if (queryTaskResponse.results != null
                    && queryTaskResponse.results.documentLinks != null
                    && !queryTaskResponse.results.documentLinks.isEmpty()) {
                break;
            }
            Thread.sleep(100);
        }
        assertTrue(queryTaskResponse != null);
        assertTrue(queryTaskResponse.results != null);
        assertTrue(queryTaskResponse.results.documentLinks != null);
        assertTrue(!queryTaskResponse.results.documentLinks.isEmpty());

        // Create notification target
        QueryTask[] notification = new QueryTask[1];
        Consumer<Operation> notificationTarget = (update) -> {
            update.complete();

            if (update.hasBody()) {
                QueryTask taskState = update.getBody(QueryTask.class);
                notification[0] = taskState;
                this.host.completeIteration();
            }
        };

        // Subscribe to the query with replay enabled
        this.host.testStart(1);
        Operation subscribe = Operation.createPost(queryTaskUri)
                .setReferer(this.host.getReferer());
        this.host.startSubscriptionService(subscribe, notificationTarget,
                ServiceSubscriber.create(true));

        // Wait for the notification of the current state
        this.host.testWait();

        assertTrue(notification[0] != null);
        assertTrue(notification[0].results != null);
        assertTrue(notification[0].results.documentLinks != null);
        assertTrue(!notification[0].results.documentLinks.isEmpty());
    }

    @Test
    public void testExpandContent() throws Throwable {
        testExpand(new ExpansionStrategy() {

            @Override
            public ExampleServiceState createInitialDocumentState() {
                ExampleServiceState initialState = new ExampleServiceState();
                initialState.name = UUID.randomUUID().toString();
                return initialState;
            }

            @Override
            public Builder adjustBuilder(Builder builder) {
                return builder.addOption(QueryOption.EXPAND_CONTENT);
            }

            @Override
            public URI adjustURI(URI uri) {
                return UriUtils.buildExpandLinksQueryUri(uri);
            }

            @Override
            public void validateDocumentResults(ServiceDocumentQueryResult taskResult, int expectedCount,
                    int expectedVersion,
                    Action expectedLastAction) {

                String kind = Utils.buildKind(ExampleServiceState.class);
                assertEquals(expectedCount, taskResult.documentLinks.size());
                assertEquals(taskResult.documentLinks.size(), taskResult.documents.size());
                for (Entry<String, Object> e : taskResult.documents.entrySet()) {
                    ExampleServiceState st = Utils.fromJson(e.getValue(), ExampleServiceState.class);

                    assertEquals(e.getKey(), st.documentSelfLink);
                    assertTrue(st.name != null);
                    assertTrue(st.documentVersion == expectedVersion);
                    assertEquals(kind, st.documentKind);
                    assertEquals(expectedLastAction.toString(), st.documentUpdateAction);
                }
            }

            @Override
            public void validateExpandBuiltin(VerificationHost host, Query kindClause, ServiceDocumentQueryResult taskResult) {

                // verify built-in only corresponds to previous expansion
                QueryTask task = QueryTask.Builder.createDirectTask()
                        .setQuery(kindClause)
                        .addOption(QueryOption.EXPAND_CONTENT)
                        .addOption(QueryOption.EXPAND_BUILTIN_CONTENT_ONLY)
                        .build();

                host.createQueryTaskService(task, false,
                        task.taskInfo.isDirect, task, null);
                // confirm that only the built in fields were in the expanded results
                for (Object o : task.results.documents.values()) {
                    String json = Utils.toJson(o);
                    ExampleServiceState st = Utils.fromJson(json, ExampleServiceState.class);
                    assertTrue(st.counter == null);
                    assertTrue(st.name == null);
                    assertTrue(st.keyValues.isEmpty());
                    // compare with fully expanded state from a previous query task
                    ExampleServiceState fullState = Utils.fromJson(
                            taskResult.documents.get(st.documentSelfLink),
                            ExampleServiceState.class);
                    assertEquals(fullState.documentExpirationTimeMicros, st.documentExpirationTimeMicros);
                    assertEquals(fullState.documentVersion, st.documentVersion);
                    assertEquals(fullState.documentUpdateAction, st.documentUpdateAction);
                    assertEquals(fullState.documentKind, st.documentKind);
                    assertEquals(fullState.documentUpdateTimeMicros, st.documentUpdateTimeMicros);
                    assertEquals(fullState.documentOwner, st.documentOwner);
                }
            }

        });

    }

    @Test
    public void testExpandSelectedFields() throws Throwable {
        testExpand(new ExpansionStrategy() {

            private final List<String> fields = Arrays.asList(new String[]{"name", "tags"});

            @Override
            public ExampleServiceState createInitialDocumentState() {
                ExampleServiceState initialState = new ExampleServiceState();
                initialState.name = UUID.randomUUID().toString();
                initialState.tags = Stream.of("a", "b").collect(Collectors.toSet());
                initialState.keyValues = new HashMap<>();
                initialState.keyValues.put("key", "value");
                return initialState;
            }

            @Override
            public Builder adjustBuilder(Builder builder) {
                builder.addOption(QueryOption.EXPAND_SELECTED_FIELDS);
                fields.forEach((field) -> {
                    builder.addSelectTerm(field);
                });
                return builder;
            }

            @Override
            public URI adjustURI(URI uri) {
                return UriUtils.buildSelectFieldsQueryUri(uri, fields);
            }

            @Override
            public void validateDocumentResults(ServiceDocumentQueryResult taskResult, int expectedCount,
                    int expectedVersion,
                    Action expectedLastAction) {

                assertEquals(expectedCount, taskResult.documentLinks.size());
                assertEquals(taskResult.documentLinks.size(), taskResult.documents.size());
                for (Entry<String, Object> e : taskResult.documents.entrySet()) {
                    ExampleServiceState st = Utils.fromJson(e.getValue(), ExampleServiceState.class);
                    // select fields - validate only the requested fields were populated
                    assertTrue(st.name != null);
                    assertEquals(0, st.keyValues.size());
                    assertEquals(2, st.tags.size());
                    assertNull(st.documentSelfLink);
                    assertEquals(0, st.documentVersion);
                    assertNull(st.documentKind);
                    assertNull(st.documentUpdateAction);
                }
            }
        });
    }

    /**
     * Internal interface permitting multiple implementations of expansion
     * modifiers and validators
     */
    private static interface ExpansionStrategy {

        public ExampleServiceState createInitialDocumentState();

        public Builder adjustBuilder(Builder builder);

        public URI adjustURI(URI uri);

        public void validateDocumentResults(ServiceDocumentQueryResult taskResult, int expectedCount,
                int expectedVersion,
                Action expectedLastAction);

        default void validateExpandBuiltin(VerificationHost host, Query kindClause, ServiceDocumentQueryResult taskResult) {
            // do nothing by default, only overridden when a builtin fields are available in expansion
        }

    }

    /** Generic tester for different expansion strategies */
    private void testExpand(ExpansionStrategy expansionStrategy) throws Throwable {

        setUpHost();
        Map<URI, ExampleServiceState> states = this.host.doFactoryChildServiceStart(null,
                this.serviceCount,
                ExampleServiceState.class, (o) -> o.setBody(expansionStrategy.createInitialDocumentState()),
                UriUtils.buildFactoryUri(this.host, ExampleService.class));

        Query kindClause = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();

        Builder builder = QueryTask.Builder.createDirectTask()
                .setQuery(kindClause);

        builder = expansionStrategy.adjustBuilder(builder);

        QueryTask task = builder.build();

        if (task.documentExpirationTimeMicros != 0) {
            // the value was set as an interval by the calling test. Make absolute here so
            // account for service creation above
            task.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(
                    +task.documentExpirationTimeMicros);
        }

        this.host.logThroughput();

        int count = 5;
        this.host.testStart(states.size() * count);
        for (int i = 0; i < count; i++) {
            // issue some updates to move the versions forward
            for (URI u : states.keySet()) {
                ExampleServiceState body = new ExampleServiceState();
                body.counter = Long.MAX_VALUE;
                Operation patch = Operation.createPatch(u)
                        .setCompletion(this.host.getCompletion())
                        .setBody(body);
                this.host.send(patch);
            }
        }
        this.host.testWait();

        this.host.createQueryTaskService(task, false,
                task.taskInfo.isDirect, task, null);

        URI factoryURI = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        // verify expand only from factory, verify expand and select field from both factory and a direct query task

        factoryURI = expansionStrategy.adjustURI(factoryURI);

        ServiceDocumentQueryResult factoryGetResult = this.host
                .getFactoryState(factoryURI);
        expansionStrategy.validateDocumentResults(factoryGetResult, states.size(), count, Action.PATCH);

        ServiceDocumentQueryResult taskResult = task.results;
        expansionStrategy.validateDocumentResults(taskResult, states.size(), count, Action.PATCH);

        // delete a service, verify its filtered from the results
        URI serviceToRemove = states.keySet().iterator().next();
        ExampleServiceState removedState = states.remove(serviceToRemove);
        Operation delete = Operation.createDelete(serviceToRemove)
                .setCompletion(this.host.getCompletion());
        this.host.sendAndWait(delete);

        task.results = null;
        this.host.createQueryTaskService(task, false,
                task.taskInfo.isDirect, task, null);

        assertEquals(states.size(), task.results.documentLinks.size());
        assertEquals(task.results.documentLinks.size(), task.results.documents.size());
        for (Entry<String, Object> e : task.results.documents.entrySet()) {
            ExampleServiceState st = Utils.fromJson(e.getValue(), ExampleServiceState.class);
            assertTrue(!st.name.equals(removedState.name));
        }

        taskResult = task.results;
        expansionStrategy.validateDocumentResults(taskResult, states.size(), count, Action.PATCH);

        // verify expand from the factory too
        factoryGetResult = this.host.getFactoryState(factoryURI);
        expansionStrategy.validateDocumentResults(factoryGetResult, states.size(), count, Action.PATCH);

        // (optionally) validate expansion of jsut builtins matches this expansion
        expansionStrategy.validateExpandBuiltin(this.host, kindClause, taskResult);
    }

    @Test
    public void throughputSimpleQuery() throws Throwable {
        setUpHost();
        List<URI> services = createQueryTargetServices(this.serviceCount);
        QueryValidationServiceState newState = new QueryValidationServiceState();
        newState.textValue = "now";
        newState = putSimpleStateOnQueryTargetServices(services, newState);
        Query q = Query.Builder.create()
                .addFieldClause("id", newState.id, MatchType.PHRASE, Occurance.MUST_OCCUR)
                .addKindFieldClause(QueryValidationServiceState.class).build();

        // first do the test with no concurrent updates to the index, while we query
        boolean interleaveWrites = false;
        for (int i = 0; i < this.iterationCount; i++) {
            doThroughputQuery(services, q, 1, newState, interleaveWrites);
        }

        // now update the index, once for every N queries. This will have a significant
        // impact on performance
        interleaveWrites = true;
        for (int i = 0; i < this.iterationCount; i++) {
            doThroughputQuery(services, q, 1, newState, interleaveWrites);
        }
    }

    @Test
    public void throughputCountQuery() throws Throwable {
        setUpHost();
        List<URI> services = createQueryTargetServices(this.serviceCount);
        QueryValidationServiceState newState = new QueryValidationServiceState();
        for (int i = 0; i < this.updateCount; i++) {
            newState.textValue = i + "";
            putSimpleStateOnQueryTargetServices(services, newState, true);
        }

        Query q = Query.Builder.create()
                .addKindFieldClause(QueryValidationServiceState.class)
                .build();

        // first do the test with no concurrent updates to the index while queries occur
        for (int i = 0; i < this.iterationCount; i++) {
            doThroughputQuery(services, q, EnumSet.of(QueryOption.COUNT), null,
                    newState, false);
        }

        // now update the index once for every N queries. This will have a significant impact on
        // performance.
        for (int i = 0; i < this.iterationCount; i++) {
            doThroughputQuery(services, q, EnumSet.of(QueryOption.COUNT), null,
                    newState, true);
        }
    }

    @Test
    public void documentKindQueryStats() throws Throwable {
        setUpHost();
        Query q = Query.Builder.create()
                .addKindFieldClause(QueryValidationServiceState.class)
                .addKindFieldClause(ExampleServiceState.class)
                .build();

        QueryTask task = QueryTask.Builder.createDirectTask()
                .setQuery(q).build();

        for (int i = 0; i < this.queryCount; i++) {
            this.host.createQueryTaskService(task, false,
                    task.taskInfo.isDirect, task, null);
        }

        Map<String, ServiceStat> stats = this.host.getServiceStats(this.host.getDocumentIndexServiceUri());
        String statName = String.format(LuceneDocumentIndexService.STAT_NAME_DOCUMENT_KIND_QUERY_COUNT_FORMAT,
                Utils.buildKind(QueryValidationServiceState.class) + ", "
                + Utils.buildKind(ExampleServiceState.class));
        ServiceStat kindStat = stats.get(statName);
        ServiceStat nonKindQueryStat = stats.get(
                LuceneDocumentIndexService.STAT_NAME_NON_DOCUMENT_KIND_QUERY_COUNT);
        assertNotNull(kindStat);
        assertNull(nonKindQueryStat);
        assertEquals(this.queryCount, kindStat.latestValue, 0);

        q = Query.Builder.create()
                .addFieldClause("dummy-property", "dummy-value")
                .build();
        task = QueryTask.Builder.createDirectTask()
                .setQuery(q).build();

        for (int i = 0; i < this.queryCount; i++) {
            this.host.createQueryTaskService(task, false,
                    task.taskInfo.isDirect, task, null);
        }

        stats = this.host.getServiceStats(this.host.getDocumentIndexServiceUri());
        nonKindQueryStat = stats.get(
                LuceneDocumentIndexService.STAT_NAME_NON_DOCUMENT_KIND_QUERY_COUNT);
        assertNotNull(nonKindQueryStat);
        assertEquals(this.queryCount, nonKindQueryStat.latestValue, 0);
    }

    private void doThroughputQuery(List<URI> services, Query q, Integer expectedResultCount,
            QueryValidationServiceState template, boolean interleaveWrites) throws Throwable {
        doThroughputQuery(services, q, EnumSet.noneOf(QueryOption.class), expectedResultCount,
                template, interleaveWrites);
    }

    private void doThroughputQuery(List<URI> services, Query q, EnumSet<QueryOption> queryOptions,
            Integer expectedResultCount, QueryValidationServiceState template,
            boolean interleaveWrites) throws Throwable {

        int interleaveFactor = 10;
        this.host.log(
                "Starting QPS test, service count: %d, query count: %d, interleave writes: %s",
                this.serviceCount, this.queryCount, interleaveWrites);

        QueryTask.Builder builder = QueryTask.Builder.createDirectTask().setQuery(q);
        if (queryOptions != null && !queryOptions.isEmpty()) {
            builder.addOptions(queryOptions);
        }
        QueryTask qt = builder.build();

        Random r = new Random();
        double s = System.nanoTime();
        TestContext ctx = this.host.testCreate(this.queryCount);
        for (int i = 0; i < this.queryCount; i++) {
            final int index = i;
            Operation post = Operation.createPost(
                    UriUtils.buildUri(this.host, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS))
                    .setBodyNoCloning(qt)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            ctx.fail(e);
                            return;
                        }

                        QueryTask rsp = o.getBody(QueryTask.class);
                        if (expectedResultCount != null
                                && rsp.results.documentLinks.size() != expectedResultCount) {
                            ctx.fail(new IllegalStateException("Unexpected result count"));
                            return;
                        }

                        if (!interleaveWrites || (index % interleaveFactor) != 0) {
                            ctx.complete();
                            return;
                        }

                        int ri = r.nextInt(services.size());
                        Operation patch = Operation.createPatch(services.get(ri))
                                .setBody(template);
                        this.host.send(patch);
                        ctx.complete();

                    });
            this.host.send(post);
        }
        this.host.testWait(ctx);
        double e = System.nanoTime();
        double thpt = (this.queryCount * TimeUnit.SECONDS.toNanos(1)) / (e - s);
        this.host.log("Queries per second: %f", thpt);
    }

    @Test
    public void throughputSimpleQueryDocumentSearch() throws Throwable {
        setUpHost();

        List<URI> services = createQueryTargetServices(this.serviceCount);

        // start two different types of services, creating two sets of documents
        // first start the query validation service instances, setting the id
        // field
        // to the same value
        QueryValidationServiceState newState = new QueryValidationServiceState();

        // test search performance using a single version per service, single match
        newState = putSimpleStateOnQueryTargetServices(services, newState);
        for (int i = 0; i < 5; i++) {
            this.host.createAndWaitSimpleDirectQuery("id", newState.id, services.size(), 1, this.testResults);
        }

        // all expected as results
        newState.textValue = "hello";
        newState = putSimpleStateOnQueryTargetServices(services, newState);
        for (int i = 0; i < 5; i++) {
            this.host.createAndWaitSimpleDirectQuery(
                    QueryValidationServiceState.FIELD_NAME_TEXT_VALUE,
                    newState.textValue,
                    services.size(),
                    services.size(), this.testResults);
        }

        // make sure throughput is not degraded when multiple versions are added per service
        for (int i = 0; i < 5; i++) {
            newState = putSimpleStateOnQueryTargetServices(services, newState);
        }

        for (int i = 0; i < 5; i++) {
            this.host.createAndWaitSimpleDirectQuery("id", newState.id, services.size(), 1, this.testResults);
        }

    }

    @Test
    public void throughputComplexQueryDocumentSearch() throws Throwable {
        setUpHost();
        LuceneDocumentIndexService
                .setImplicitQueryResultLimit(this.serviceCount * 10);
        List<URI> services = createQueryTargetServices(this.serviceCount);

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

        for (int i = 0; i < this.iterationCount; i++) {
            this.host.log("%d", i);
            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCompositeFieldName("exampleValue", "name"),
                    newState.exampleValue.name, services.size(), 1, this.testResults);

            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCompositeFieldName("nestedComplexValue", "id"),
                    newState.nestedComplexValue.id, services.size(), services.size(), this.testResults);

            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCompositeFieldName("listOfExampleValues", "item",
                            "name"),
                    newState.listOfExampleValues.get(0).name, services.size(), services.size(), this.testResults);

            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCompositeFieldName("arrayOfExampleValues", "item",
                            "name"),
                    newState.arrayOfExampleValues[0].name, services.size(), services.size(), this.testResults);

            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCollectionItemName("listOfStrings"),
                    newState.listOfStrings.get(0), services.size(), services.size(), this.testResults);

            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCollectionItemName("arrayOfStrings"),
                    newState.arrayOfStrings[1], services.size(), services.size(), this.testResults);

            this.host.createAndWaitSimpleDirectQuery(
                    "id",
                    newState.id, services.size(), 1, this.testResults);

            doInQuery("id",
                    newState.id, services.size(), 1);
            doNotInQuery("id",
                    newState.id, services.size(), services.size() - 1);
            doInCollectionQuery("listOfStrings", newState.listOfStrings,
                    services.size(), services.size());

            doMapQuery("mapOfBooleans", newState.mapOfBooleans, services.size(), services.size());
            doMapQuery("mapOfBytesArray", newState.mapOfBytesArrays, services.size(), 0);
            doMapQuery("mapOfStrings", newState.mapOfStrings, services.size(), services.size());
            doMapQuery("mapOfUris", newState.mapOfUris, services.size(), services.size());

            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCompositeFieldName("mapOfNestedTypes", "nested", "id"),
                    newState.mapOfNestedTypes.get("nested").id, services.size(), services.size(), this.testResults);

            // query for a field that SHOULD be ignored. We should get zero links back
            this.host.createAndWaitSimpleDirectQuery(
                    QueryValidationServiceState.FIELD_NAME_IGNORED_STRING_VALUE,
                    newState.ignoredStringValue, services.size(), 0, this.testResults);

            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCollectionItemName("ignoredArrayOfStrings"),
                    newState.ignoredArrayOfStrings[1], services.size(), 0, this.testResults);
        }

        ServiceStat lookupSt = getLuceneStat(
                LuceneDocumentIndexService.STAT_NAME_VERSION_CACHE_LOOKUP_COUNT);
        ServiceStat missSt = getLuceneStat(
                LuceneDocumentIndexService.STAT_NAME_VERSION_CACHE_MISS_COUNT);
        ServiceStat entryCountSt = getLuceneStat(
                LuceneDocumentIndexService.STAT_NAME_VERSION_CACHE_ENTRY_COUNT);
        this.host.log("Version cache size: %f Version cache lookups: %f, misses: %f",
                entryCountSt.latestValue,
                lookupSt.latestValue,
                missSt.latestValue);
        verifyNoPaginatedIndexSearchers();
    }

    @SuppressWarnings({"rawtypes"})
    private void doMapQuery(String mapName, Map map, long documentCount, long expectedResultCount)
            throws Throwable {
        for (Object o : map.entrySet()) {
            Entry e = (Entry) o;
            this.host.createAndWaitSimpleDirectQuery(
                    QuerySpecification.buildCompositeFieldName(mapName, (String) e.getKey()),
                    e.getValue().toString(), documentCount, expectedResultCount, this.testResults);
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

    private void doNotInQuery(String fieldName, String fieldValue, long documentCount,
            long expectedResultCount) throws Throwable {
        QuerySpecification spec = new QuerySpecification();
        spec.query = Query.Builder.create().addInClause(
                fieldName,
                Arrays.asList(
                        UUID.randomUUID().toString(),
                        fieldValue,
                        UUID.randomUUID().toString()),
                Occurance.MUST_NOT_OCCUR)
                .addFieldClause(ServiceDocument.FIELD_NAME_KIND,
                        Utils.buildKind(QueryValidationServiceState.class))
                .build();
        this.host.createAndWaitSimpleDirectQuery(spec,
                documentCount, expectedResultCount);

        // Additional Test to verify that  InClause with Array[1] is treated as TERM Query
        QuerySpecification spec1 = new QuerySpecification();
        spec1.query = Query.Builder.create().addInClause(
                fieldName,
                Arrays.asList(fieldValue),
                Occurance.MUST_NOT_OCCUR)
                .addFieldClause(ServiceDocument.FIELD_NAME_KIND,
                        Utils.buildKind(QueryValidationServiceState.class))
                .build();
        this.host.createAndWaitSimpleDirectQuery(spec1,
                documentCount, expectedResultCount);
    }

    @SuppressWarnings({"rawtypes"})
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

    public static class SelectLinksQueryTargetService extends StatefulService {

        public static class State extends QueryValidationServiceState {
            public String ignored;
        }

        public SelectLinksQueryTargetService() {
            super(State.class);
            super.toggleOption(ServiceOption.PERSISTENCE, true);
        }
    }

    @Test
    public void selectLinks() throws Throwable {
        setUpHost();
        List<URI> services = createQueryTargetServices(this.serviceCount);

        // start two different types of services, creating two sets of documents
        // first start the query validation service instances, setting the id
        // field
        // to the same value
        QueryValidationServiceState newState = new QueryValidationServiceState();
        newState.id = UUID.randomUUID().toString();
        newState = putStateOnQueryTargetServices(services, 1, newState);

        // issue a query that matches kind for the query validation service
        Query query = Query.Builder.create()
                .addKindFieldClause(QueryValidationServiceState.class)
                .build();
        QueryTask queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.SELECT_LINKS)
                .addLinkTerm(QueryValidationServiceState.FIELD_NAME_SERVICE_LINK)
                .setQuery(query).build();

        createWaitAndValidateQueryTask(1, services, queryTask.querySpec, false);

        // issue a another query this time for the field that has a collection of links
        query = Query.Builder.create()
                .addKindFieldClause(QueryValidationServiceState.class)
                .build();
        queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.SELECT_LINKS)
                .addLinkTerm(QueryValidationServiceState.FIELD_NAME_SERVICE_LINKS)
                .setQuery(query).build();

        createWaitAndValidateQueryTask(1, services, queryTask.querySpec, false);

        // update query validation services so the serviceLink field points to a real example
        // service link, then issue a query with EXPAND_LINKS
        patchQueryTargetServiceLinksWithExampleLinks(services);
        query = Query.Builder.create()
                .addKindFieldClause(QueryValidationServiceState.class)
                .build();
        queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.SELECT_LINKS)
                .addOption(QueryOption.EXPAND_LINKS)
                .addLinkTerm("badLinkName")
                .addLinkTerm(QueryValidationServiceState.FIELD_NAME_SERVICE_LINK)
                .setQuery(query).build();

        createWaitAndValidateQueryTask(1, services, queryTask.querySpec, true);

        // issue another query this time for the field that has a collection of links
        query = Query.Builder.create()
                .addKindFieldClause(QueryValidationServiceState.class)
                .build();
        queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.SELECT_LINKS)
                .addOption(QueryOption.EXPAND_LINKS)
                .addLinkTerm("badLinkName")
                .addLinkTerm(QueryValidationServiceState.FIELD_NAME_SERVICE_LINKS)
                .setQuery(query).build();

        createWaitAndValidateQueryTask(1, services, queryTask.querySpec, true);

        // update one of the links to a bogus link value (pointing to a non existent document)
        // and verify the expanded link, for that document with the broken service link, contains
        // the ServiceErrorResponse we expect
        URI queryValidationServiceWithBrokenServiceLink = services.get(0);
        QueryValidationServiceState patchBody = new QueryValidationServiceState();
        patchBody.serviceLink = "/some/non/existent/service/some/where-" + UUID.randomUUID();
        Operation patch = Operation.createPatch(queryValidationServiceWithBrokenServiceLink)
                .setBody(patchBody);
        this.host.sendAndWaitExpectSuccess(patch);
        this.host.createQueryTaskService(queryTask, false,
                true, queryTask, null);
        validatedExpandLinksResultsWithBogusLink(queryTask,
                queryValidationServiceWithBrokenServiceLink);

        // Start another set of query target services which use an inherited state class and verify
        // that links in inherited fields can be selected.
        List<URI> queryTargetServiceUris = createSelectLinksQueryTargetServices();

        query = Query.Builder.create()
                .addKindFieldClause(SelectLinksQueryTargetService.State.class)
                .build();
        queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.SELECT_LINKS)
                .addOption(QueryOption.EXPAND_LINKS)
                .addLinkTerm(QueryValidationServiceState.FIELD_NAME_SERVICE_LINK)
                .setQuery(query).build();

        createWaitAndValidateQueryTask(1, queryTargetServiceUris, queryTask.querySpec, false);

        query = Query.Builder.create()
                .addKindFieldClause(SelectLinksQueryTargetService.State.class)
                .build();
        queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.SELECT_LINKS)
                .addOption(QueryOption.EXPAND_LINKS)
                .addLinkTerm(QueryValidationServiceState.FIELD_NAME_SERVICE_LINKS)
                .setQuery(query).build();

        createWaitAndValidateQueryTask(1, queryTargetServiceUris, queryTask.querySpec, false);
    }

    private List<URI> createSelectLinksQueryTargetServices() {
        ServiceDocumentQueryResult factoryResults = this.host.getFactoryState(
                UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
        List<String> exampleServiceLinks = factoryResults.documentLinks;

        List<Service> queryTargetServices = new ArrayList<>(this.serviceCount);
        TestContext ctx = this.host.testCreate(this.serviceCount);
        for (int i = 0; i < this.serviceCount; i++) {
            SelectLinksQueryTargetService.State initialState = new SelectLinksQueryTargetService.State();
            initialState.serviceLink = exampleServiceLinks.get(i);
            initialState.serviceLinks = exampleServiceLinks;

            Operation post = this.host.createServiceStartPost(ctx).setBody(initialState);

            SelectLinksQueryTargetService service = new SelectLinksQueryTargetService();
            this.host.startService(post, service);
            queryTargetServices.add(service);
        }
        this.host.testWait(ctx);
        return queryTargetServices.stream().map(Service::getUri).collect(Collectors.toList());
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

        List<URI> services = createQueryTargetServices((int) (serviceCount / 2));

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
        queryTask.querySpec.options = EnumSet.of(QueryOption.TOP_RESULTS);
        queryTask.querySpec.resultLimit = (int) (this.serviceCount * versionCount * 10);

        createWaitAndValidateQueryTask((int) versionCount, services, queryTask.querySpec,
                forceRemote);

        // same as above, but ask for COUNT only, no links, explicit limit
        queryTask.querySpec.resultLimit = (int) (this.serviceCount * versionCount * 10);
        queryTask.querySpec.options = EnumSet.of(QueryOption.COUNT,
                QueryOption.INCLUDE_ALL_VERSIONS);
        createWaitAndValidateQueryTask((int) versionCount, services, queryTask.querySpec,
                forceRemote);

        // COUNT, latest version only, explicit resultLimit
        queryTask.querySpec.options = EnumSet.of(QueryOption.COUNT);
        queryTask.querySpec.resultLimit = (int) (this.serviceCount * versionCount * 10);
        createWaitAndValidateQueryTask(0, services, queryTask.querySpec,
                forceRemote);

        // COUNT, latest version only, implicit resultLimit, reduce default so we trigger
        // "paginated" logic
        LuceneDocumentIndexService.setImplicitQueryResultLimit(this.serviceCount / 2);
        queryTask.querySpec.options = EnumSet.of(QueryOption.COUNT);
        queryTask.querySpec.resultLimit = null;
        createWaitAndValidateQueryTask(0, services, queryTask.querySpec,
                forceRemote);
        LuceneDocumentIndexService
                .setImplicitQueryResultLimit(LuceneDocumentIndexService.DEFAULT_QUERY_RESULT_LIMIT);

        // now make sure expand works. Issue same query, but enable expand
        queryTask.querySpec.options = EnumSet.of(QueryOption.EXPAND_CONTENT,
                QueryOption.TOP_RESULTS);
        queryTask.querySpec.resultLimit = (int) (this.serviceCount * versionCount * 10);
        createWaitAndValidateQueryTask(versionCount, services, queryTask.querySpec, forceRemote);

        // now for the minimal test service
        query = Query.Builder.create()
                .addKindFieldClause(MinimalTestServiceState.class)
                .build();
        queryTask = QueryTask.Builder.create().setQuery(query).build();
        queryTask.querySpec.options = EnumSet.of(QueryOption.TOP_RESULTS);
        queryTask.querySpec.resultLimit = (int) (this.serviceCount * versionCount * 10);
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
    public void multiNodeQueryTasksReadAfterWrite() throws Throwable {
        final int nodeCount = 3;
        final int stressTestServiceCountThreshold = 1000;

        setUpHost();

        this.host.setUpPeerHosts(nodeCount);
        this.host.joinNodesAndVerifyConvergence(nodeCount);

        VerificationHost targetHost = this.host.getPeerHost();
        URI exampleFactoryURI = UriUtils.buildUri(targetHost, ExampleService.FACTORY_LINK);

        CommandLineArgumentParser.parseFromProperties(this);
        if (this.serviceCount > stressTestServiceCountThreshold) {
            targetHost.setStressTest(true);
        }

        List<URI> exampleServices = new ArrayList<>();
        createExampleServices(exampleFactoryURI, exampleServices);
        verifyMultiNodeBroadcastQueries(targetHost);
        verifyOnlySupportSortOnSelfLinkInBroadcast(targetHost);
        this.host.deleteAllChildServices(exampleFactoryURI);

        for (int i = 0; i < this.iterationCount; i++) {
            createExampleServices(exampleFactoryURI, exampleServices, true);
            verifyMultiNodeQueries(targetHost, true);
            verifyMultiNodeQueries(targetHost, false);
            this.host.deleteAllChildServices(exampleFactoryURI);
        }

    }

    @Test
    public void groupByQuery() throws Throwable {
        setUpHost();
        VerificationHost targetHost = this.host;
        URI exampleFactoryURI = UriUtils.buildUri(targetHost, ExampleService.FACTORY_LINK);
        String[] groupArray = new String[]{"one", "two", "three", "four"};
        List<String> groups = Arrays.asList(groupArray);
        List<URI> exampleServices = new ArrayList<>();

        createGroupedExampleServices(groups, exampleFactoryURI, exampleServices);

        verifyGroupQueryStateValidation(targetHost, groups);

        Map<String, ServiceDocumentQueryResult> resultsPerGroup = verifyGroupQueryWithExpand(
                targetHost, groups);

        // delete services associated with one group, and confirm we get a null link for that group
        verifyGroupQueryAfterDeletion(targetHost, groups, resultsPerGroup);

        // delete or prior services,start fresh, now do paginated queries on the per group results
        exampleServices.clear();
        this.host.deleteAllChildServices(exampleFactoryURI);

        // create new batch of example services
        createGroupedExampleServices(groups, exampleFactoryURI, exampleServices);

        verifyGroupQueryPaginatedPerGroup(targetHost, groups);

        verifyGroupQueryPaginatedAcrossGroups(targetHost, groups);

        Map<String, ServiceStat> stats = this.host
                .getServiceStats(targetHost.getDocumentIndexServiceUri());
        ServiceStat groupQueryCount = stats
                .get(LuceneDocumentIndexService.STAT_NAME_GROUP_QUERY_COUNT
                        + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
        assertTrue(groupQueryCount != null);
        assertTrue(groupQueryCount.latestValue >= 4.0);

        ServiceStat groupQueryDuration = stats
                .get(LuceneDocumentIndexService.STAT_NAME_GROUP_QUERY_DURATION_MICROS
                        + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
        assertTrue(groupQueryDuration != null);
        assertTrue(groupQueryDuration.logHistogram != null);

        // delete or prior services,start fresh, now do paginated queries on the per group results
        exampleServices.clear();
        this.host.deleteAllChildServices(exampleFactoryURI);

        // create new batch of example services grouped by numeric fields
        Long[] numericGroupArray = new Long[]{1L, 2L, 3L, 4L};
        List<Long> numericGroups = Arrays.asList(numericGroupArray);
        createNumericGroupedExampleServices(numericGroups, exampleFactoryURI, exampleServices);
        verifyNumericGroupQueryPaginatedPerGroup(targetHost, numericGroups);
        verifyNumericGroupQueryPaginatedAcrossGroups(targetHost, numericGroups);
    }

    private void verifyGroupQueryStateValidation(VerificationHost targetHost, List<String> groups)
            throws Throwable {
        URI queryFactoryURI = UriUtils.buildUri(targetHost, ServiceUriPaths.CORE_QUERY_TASKS);

        Query query = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();
        // missing sort on query, should fail
        QueryTask queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.GROUP_BY)
                .groupOrder(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING, SortOrder.ASC)
                .setQuery(query).build();
        Operation post = Operation.createPost(queryFactoryURI)
                .setBody(queryTask);
        this.host.sendAndWaitExpectFailure(post);

        // missing groupByTerm
        queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.GROUP_BY)
                .orderAscending(ExampleServiceState.FIELD_NAME_ID, TypeName.STRING)
                .setQuery(query).build();
        post = Operation.createPost(queryFactoryURI)
                .setBody(queryTask);
        this.host.sendAndWaitExpectFailure(post);

        // with invalid option: COUNT
        queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.GROUP_BY)
                .addOption(QueryOption.COUNT)
                .orderAscending(ExampleServiceState.FIELD_NAME_ID, TypeName.STRING)
                .groupOrder(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING, SortOrder.ASC)
                .setQuery(query).build();
        post = Operation.createPost(queryFactoryURI)
                .setBody(queryTask);
        this.host.sendAndWaitExpectFailure(post);

        // with invalid option: CONTINUOUS
        queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.GROUP_BY)
                .addOption(QueryOption.CONTINUOUS)
                .orderAscending(ExampleServiceState.FIELD_NAME_ID, TypeName.STRING)
                .groupOrder(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING, SortOrder.ASC)
                .setQuery(query).build();
        post = Operation.createPost(queryFactoryURI)
                .setBody(queryTask);
        this.host.sendAndWaitExpectFailure(post);
    }

    private Map<String, ServiceDocumentQueryResult> verifyGroupQueryWithExpand(
            VerificationHost targetHost, List<String> groups)
            throws Throwable {
        // issue a query that matches kind for the query validation service
        Query query = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();
        QueryTask queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.GROUP_BY)
                .addOption(QueryOption.EXPAND_CONTENT)
                .orderAscending(ExampleServiceState.FIELD_NAME_ID, TypeName.STRING)
                .groupOrder(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING, SortOrder.ASC)
                .setQuery(query).build();
        URI queryTaskURI = this.host.createQueryTaskService(queryTask);
        QueryTask finalState = this.host.waitForQueryTask(queryTaskURI, TaskStage.FINISHED);
        Map<String, ServiceDocumentQueryResult> resultsPerGroup = new HashMap<>();
        int expectedCountPerPage = this.serviceCount;
        validateGroupByResults(targetHost, groups, null, finalState, resultsPerGroup,
                expectedCountPerPage);

        // do the same query, direct
        QueryTask directResult = new QueryTask();
        TestContext ctx = this.host.testCreate(1);
        URI queryFactoryURI = UriUtils.buildUri(targetHost, ServiceUriPaths.CORE_QUERY_TASKS);

        queryTask = QueryTask.Builder.createDirectTask()
                .addOption(QueryOption.GROUP_BY)
                .addOption(QueryOption.EXPAND_CONTENT)
                .orderAscending(ExampleServiceState.FIELD_NAME_ID, TypeName.STRING)
                .groupOrder(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING, SortOrder.ASC)
                .setQuery(query).build();
        Operation post = Operation.createPost(queryFactoryURI)
                .setBody(queryTask)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }
                    QueryTask rsp = o.getBody(QueryTask.class);
                    directResult.results = rsp.results;
                    directResult.taskInfo = rsp.taskInfo;
                    directResult.querySpec = rsp.querySpec;
                    ctx.completeIteration();
                });
        this.host.send(post);
        this.host.testWait(ctx);
        validateGroupByResults(targetHost, groups, null, directResult, null,
                expectedCountPerPage);

        return resultsPerGroup;
    }

    private void verifyGroupQueryPaginatedPerGroup(VerificationHost targetHost,
            List<String> groups) throws Throwable {
        Query query = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();
        QueryTask queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.GROUP_BY)
                .addOption(QueryOption.EXPAND_CONTENT)
                .setResultLimit(this.serviceCount / 5)
                .orderAscending(ExampleServiceState.FIELD_NAME_ID, TypeName.STRING)
                .groupOrder(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING, SortOrder.ASC)
                .setQuery(query).build();
        URI queryTaskURI = this.host.createQueryTaskService(queryTask);
        QueryTask finalState = this.host.waitForQueryTask(queryTaskURI, TaskStage.FINISHED);
        int expectedCountPerPage = queryTask.querySpec.resultLimit;
        validateGroupByResults(targetHost, groups, null, finalState, null,
                expectedCountPerPage);
    }

    private void verifyGroupQueryPaginatedAcrossGroups(VerificationHost targetHost,
            List<String> groups) throws Throwable {
        Query query = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();
        // two pagination across two dimensions: number of groups, and documents per group
        QueryTask queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.GROUP_BY)
                .addOption(QueryOption.EXPAND_CONTENT)
                .setResultLimit(this.serviceCount / 5)
                .setGroupResultLimit(2)
                .orderAscending(ExampleServiceState.FIELD_NAME_ID, TypeName.STRING)
                .groupOrder(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING,
                        SortOrder.ASC)
                .setQuery(query).build();
        URI queryTaskURI = this.host.createQueryTaskService(queryTask);
        QueryTask finalState = this.host.waitForQueryTask(queryTaskURI, TaskStage.FINISHED);
        int expectedCountPerPage = queryTask.querySpec.resultLimit;
        validateGroupByResults(targetHost, groups, null, finalState, null,
                expectedCountPerPage);
    }

    private void verifyGroupQueryAfterDeletion(VerificationHost targetHost, List<String> groups,
            Map<String, ServiceDocumentQueryResult> resultsPerGroup)
            throws Throwable {
        String groupToDelete = groups.get(0);
        ServiceDocumentQueryResult resultsToDelete = resultsPerGroup.get(groupToDelete);
        TestContext ctx = this.host.testCreate(resultsToDelete.documentLinks.size());
        for (String link : resultsToDelete.documentLinks) {
            Operation delete = Operation.createDelete(targetHost, link)
                    .setCompletion(ctx.getCompletion());
            this.host.send(delete);
        }
        this.host.testWait(ctx);

        Query query = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();
        QueryTask queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.GROUP_BY)
                .addOption(QueryOption.EXPAND_CONTENT)
                .orderAscending(ExampleServiceState.FIELD_NAME_ID, TypeName.STRING)
                .groupOrder(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING, SortOrder.ASC)
                .setQuery(query).build();

        URI queryTaskURI = this.host.createQueryTaskService(queryTask);
        QueryTask finalState = this.host.waitForQueryTask(queryTaskURI, TaskStage.FINISHED);
        validateGroupByResults(targetHost, groups, groupToDelete, finalState, null,
                this.serviceCount);

    }

    private void createGroupedExampleServices(Collection<String> groups, URI exampleFactoryURI,
            List<URI> exampleServices)
            throws Throwable {
        TestContext ctx = this.host.testCreate(this.serviceCount * groups.size());
        for (String group : groups) {
            for (int i = 0; i < this.serviceCount; i++) {
                ExampleServiceState s = new ExampleServiceState();
                s.name = group;
                s.documentSelfLink = UUID.randomUUID().toString();
                exampleServices.add(UriUtils.buildUri(this.host.getUri(),
                        ExampleService.FACTORY_LINK, s.documentSelfLink));
                this.host.send(Operation.createPost(exampleFactoryURI)
                        .setBody(s)
                        .setCompletion(ctx.getCompletion()));
            }
            this.host.log("Creating %d example services for group %s", this.serviceCount, group);
        }
        this.host.testWait(ctx);
    }

    private void verifyNumericGroupQueryPaginatedPerGroup(VerificationHost targetHost,
            List<Long> groups) throws Throwable {
        Query query = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();
        QueryTask queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.GROUP_BY)
                .addOption(QueryOption.EXPAND_CONTENT)
                .setResultLimit(this.serviceCount / 5)
                .orderAscending(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING)
                .groupOrder(ExampleServiceState.FIELD_NAME_SORTED_COUNTER, TypeName.LONG, SortOrder.ASC)
                .setQuery(query).build();
        URI queryTaskURI = this.host.createQueryTaskService(queryTask);
        QueryTask finalState = this.host.waitForQueryTask(queryTaskURI, TaskStage.FINISHED);
        int expectedCountPerPage = queryTask.querySpec.resultLimit;
        validateNumericGroupByResults(targetHost, groups, null, finalState, null,
                expectedCountPerPage);
    }

    private void verifyNumericGroupQueryPaginatedAcrossGroups(VerificationHost targetHost,
            List<Long> groups) throws Throwable {
        Query query = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();
        // two pagination across two dimensions: number of groups, and documents per group
        QueryTask queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.GROUP_BY)
                .addOption(QueryOption.EXPAND_CONTENT)
                .setResultLimit(this.serviceCount / 5)
                .setGroupResultLimit(2)
                .orderAscending(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING)
                .groupOrder(ExampleServiceState.FIELD_NAME_SORTED_COUNTER, TypeName.LONG,
                        SortOrder.DESC)
                .setQuery(query).build();
        URI queryTaskURI = this.host.createQueryTaskService(queryTask);
        QueryTask finalState = this.host.waitForQueryTask(queryTaskURI, TaskStage.FINISHED);
        int expectedCountPerPage = queryTask.querySpec.resultLimit;
        validateNumericGroupByResults(targetHost, groups, null, finalState, null,
                expectedCountPerPage);
    }

    private void createNumericGroupedExampleServices(Collection<Long> groups, URI exampleFactoryURI,
            List<URI> exampleServices)
            throws Throwable {
        TestContext ctx = this.host.testCreate(this.serviceCount * groups.size());
        for (Long group : groups) {
            for (int i = 0; i < this.serviceCount; i++) {
                ExampleServiceState s = new ExampleServiceState();
                String name = UUID.randomUUID().toString();
                s.id = name;
                s.name = name;
                s.sortedCounter = group;
                s.documentSelfLink = name;
                exampleServices.add(UriUtils.buildUri(this.host.getUri(),
                        ExampleService.FACTORY_LINK, s.documentSelfLink));
                this.host.send(Operation.createPost(exampleFactoryURI)
                        .setBody(s)
                        .setCompletion(ctx.getCompletion()));
            }
            this.host.log("Creating %d example services for group %d", this.serviceCount, group);
        }
        this.host.testWait(ctx);
    }

    private void createExampleServices(URI exampleFactoryURI, List<URI> exampleServices)
            throws Throwable {
        createExampleServices(exampleFactoryURI, exampleServices, false);
    }

    private void createExampleServices(URI exampleFactoryURI, List<URI> exampleServices,
            boolean randomLink)
            throws Throwable {

        TestContext ctx = this.host.testCreate(this.serviceCount);
        for (int i = 0; i < this.serviceCount; i++) {
            ExampleServiceState s = new ExampleServiceState();
            s.name = "document" + i;
            if (randomLink) {
                s.documentSelfLink = UUID.randomUUID().toString();
            } else {
                s.documentSelfLink = s.name;
            }
            exampleServices.add(UriUtils.buildUri(this.host.getUri(),
                    ExampleService.FACTORY_LINK, s.documentSelfLink));
            this.host.send(Operation.createPost(exampleFactoryURI)
                    .setBody(s)
                    .setCompletion(ctx.getCompletion()));
        }
        this.host.testWait(ctx);
    }

    private void validateGroupByResults(VerificationHost targetHost, List<String> groups,
            String groupToDelete, QueryTask finalState,
            Map<String, ServiceDocumentQueryResult> resultsPerGroup,
            int expectedCountPerPage) throws Throwable {
        assertTrue(finalState.results != null);
        assertTrue(finalState.results.documentLinks.isEmpty());
        assertTrue(finalState.results.documents == null
                || finalState.results.documents.isEmpty());
        assertTrue(finalState.results.nextPageLinksPerGroup != null);
        boolean isPaginatedAcrossGroups = finalState.querySpec.groupResultLimit != null;
        int expectedGroupCount = isPaginatedAcrossGroups
                ? finalState.querySpec.groupResultLimit : groups.size();

        assertTrue(finalState.results.nextPageLinksPerGroup.size() == expectedGroupCount);
        int totalGroupsFound = 0;
        while (finalState != null) {
            for (String g : groups) {
                String pageLinkForGroup = finalState.results.nextPageLinksPerGroup.get(g);
                if (isPaginatedAcrossGroups && pageLinkForGroup == null) {
                    continue;
                }
                totalGroupsFound++;
                assertTrue(pageLinkForGroup != null);
                while (pageLinkForGroup != null) {
                    QueryTask perGroupPage = this.host.getServiceState(null,
                            QueryTask.class, UriUtils.buildUri(targetHost, pageLinkForGroup));
                    assertTrue(perGroupPage.results != null);
                    if (resultsPerGroup != null) {
                        resultsPerGroup.computeIfAbsent(g, gg -> perGroupPage.results);
                    }
                    int expectedCount = expectedCountPerPage;
                    if (groupToDelete != null && groupToDelete.equals(g)) {
                        expectedCount = 0;
                    }
                    assertEquals(expectedCount, (long) perGroupPage.results.documentCount);
                    assertEquals(expectedCount, perGroupPage.results.documentLinks.size());
                    assertEquals(expectedCount, perGroupPage.results.documents.size());

                    for (Object doc : perGroupPage.results.documents.values()) {
                        ExampleServiceState st = Utils.fromJson(doc, ExampleServiceState.class);
                        assertEquals(g, st.name);
                    }
                    pageLinkForGroup = perGroupPage.results.nextPageLink;
                }
            }
            if (!isPaginatedAcrossGroups) {
                break;
            }
            if (finalState.results.nextPageLink == null) {
                break;
            }
            URI nextPageUri = UriUtils.buildUri(targetHost, finalState.results.nextPageLink);
            finalState = this.host.getServiceState(null, QueryTask.class, nextPageUri);
        }
        assertEquals(groups.size(), totalGroupsFound);
    }

    private void validateNumericGroupByResults(VerificationHost targetHost, List<Long> groups,
            Long groupToDelete, QueryTask finalState,
            Map<String, ServiceDocumentQueryResult> resultsPerGroup,
            int expectedCountPerPage) throws Throwable {
        assertTrue(finalState.results != null);
        assertTrue(finalState.results.documentLinks.isEmpty());
        assertTrue(finalState.results.documents == null
                || finalState.results.documents.isEmpty());
        assertTrue(finalState.results.nextPageLinksPerGroup != null);
        boolean isPaginatedAcrossGroups = finalState.querySpec.groupResultLimit != null;
        // groups.size() + 1 is to handle the "null" a.k.a. "DocumentsWithNoGroup" group.
        int expectedGroupCount = isPaginatedAcrossGroups
                ? finalState.querySpec.groupResultLimit : groups.size() + 1;

        assertTrue(finalState.results.nextPageLinksPerGroup.size() == expectedGroupCount);
        int totalGroupsFound = 0;
        while (finalState != null) {
            for (Long g : groups) {
                String pageLinkForGroup = finalState.results.nextPageLinksPerGroup.get(g.toString());
                if (isPaginatedAcrossGroups && pageLinkForGroup == null) {
                    continue;
                }
                totalGroupsFound++;
                assertTrue(pageLinkForGroup != null);
                while (pageLinkForGroup != null) {
                    QueryTask perGroupPage = this.host.getServiceState(null,
                            QueryTask.class, UriUtils.buildUri(targetHost, pageLinkForGroup));
                    assertTrue(perGroupPage.results != null);
                    if (resultsPerGroup != null) {
                        resultsPerGroup.computeIfAbsent(g.toString(), gg -> perGroupPage.results);
                    }
                    int expectedCount = expectedCountPerPage;
                    if (groupToDelete != null && groupToDelete.equals(g)) {
                        expectedCount = 0;
                    }
                    assertEquals(expectedCount, (long) perGroupPage.results.documentCount);
                    assertEquals(expectedCount, perGroupPage.results.documentLinks.size());
                    assertEquals(expectedCount, perGroupPage.results.documents.size());

                    for (Object doc : perGroupPage.results.documents.values()) {
                        ExampleServiceState st = Utils.fromJson(doc, ExampleServiceState.class);
                        assertEquals(g, st.sortedCounter);
                    }
                    pageLinkForGroup = perGroupPage.results.nextPageLink;
                }
            }
            if (!isPaginatedAcrossGroups) {
                break;
            }
            if (finalState.results.nextPageLink == null) {
                break;
            }
            URI nextPageUri = UriUtils.buildUri(targetHost, finalState.results.nextPageLink);
            finalState = this.host.getServiceState(null, QueryTask.class, nextPageUri);
        }
        assertEquals(groups.size(), totalGroupsFound);
    }

    private void verifyMultiNodeQueries(VerificationHost targetHost, boolean isDirect)
            throws Throwable {
        Query query = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();

        QueryTask queryTask = QueryTask.Builder.create().setQuery(query).build();
        queryTask.querySpec.options.add(QueryOption.EXPAND_CONTENT);
        URI u = targetHost.createQueryTaskService(queryTask, false, isDirect, queryTask, null);

        QueryTask finishedTaskState = null;
        if (isDirect) {
            // results are placed in the queryTask.result from the createQueryTask method
            finishedTaskState = queryTask;
        } else {
            URI configUri = UriUtils.buildConfigUri(u);
            ServiceConfiguration queryTaskConfig = targetHost.getServiceState(
                    null, ServiceConfiguration.class, configUri);
            assertEquals(ServiceUriPaths.DEFAULT_1X_NODE_SELECTOR,
                    queryTaskConfig.peerNodeSelectorPath);

            finishedTaskState = targetHost.waitForQueryTaskCompletion(queryTask.querySpec,
                    this.serviceCount, 1, u, false, false);
        }

        if (!validateNativeContextIsNull(targetHost, finishedTaskState)) {
            return;
        }

        this.host.log("%s %s", u, finishedTaskState.documentOwner);
        assertTrue(isDirect == finishedTaskState.taskInfo.isDirect);
    }

    private void verifyMultiNodeBroadcastQueries(VerificationHost targetHost) throws Throwable {
        verifyOnlySupportSortOnSelfLinkInBroadcast(targetHost);
        verifyDirectQueryAllowedInBroadcast(targetHost);
        nonpaginatedBroadcastQueryTasksOnExampleStates(targetHost,
                EnumSet.of(QueryOption.EXPAND_CONTENT, QueryOption.BROADCAST));
        paginatedBroadcastQueryTasksOnExampleStates(targetHost);
        paginatedBroadcastQueryTasksWithoutMatching(targetHost);
        paginatedBroadcastQueryTasksRepeatSamePage(targetHost);

        // test with QueryOption.OWNER_SELECTION
        nonpaginatedBroadcastQueryTasksOnExampleStates(targetHost,
                EnumSet.of(QueryOption.EXPAND_CONTENT, QueryOption.BROADCAST, QueryOption.OWNER_SELECTION));

        nonpaginatedBroadcastQueryTasksOnExampleStates(targetHost,
                EnumSet.of(QueryOption.BROADCAST, QueryOption.OWNER_SELECTION));

        // send forwardingService to collect and verify each node's local query result,
        // so QueryOption.BROADCAST is not set here
        lowLevelBroadcastQueryTasksWithOwnerSelection(targetHost,
                EnumSet.of(QueryOption.EXPAND_CONTENT, QueryOption.OWNER_SELECTION));
    }

    private void verifyOnlySupportSortOnSelfLinkInBroadcast(VerificationHost targetHost) throws Throwable {
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

    private void validateBroadcastQueryPostFailure(VerificationHost targetHost, Operation o,
            Throwable e) {
        if (e != null) {
            ServiceErrorResponse rsp = o.getErrorResponseBody();
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

    private void verifyDirectQueryAllowedInBroadcast(VerificationHost targetHost) throws Throwable {
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
        targetHost.send(startPost);

        targetHost.testWait();
    }

    private void nonpaginatedBroadcastQueryTasksOnExampleStates(VerificationHost targetHost,
            EnumSet<QueryOption> queryOptions)
            throws Throwable {
        QuerySpecification q = new QuerySpecification();
        Query kindClause = new Query();
        kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(Utils.buildKind(ExampleServiceState.class));
        q.query = kindClause;
        q.options = queryOptions;

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

    private void paginatedBroadcastQueryTasksOnExampleStates(VerificationHost targetHost)
            throws Throwable {

        // Simulate the scenario that multiple users query documents page by page
        // in broadcast way.
        targetHost.testStart(this.queryCount);
        for (int i = 0; i < this.queryCount; i++) {
            startPagedBroadCastQuery(targetHost);
        }

        // If the query cannot be finished in time, timeout exception would be thrown.
        targetHost.testWait();
    }

    private void paginatedBroadcastQueryTasksWithoutMatching(VerificationHost targetHost) throws Throwable {
        final int minPageCount = 3;
        final int maxPageSize = 100;
        final int resultLimit = Math.min(this.serviceCount / minPageCount, maxPageSize);

        QuerySpecification q = new QuerySpecification();

        Query kindClause = new Query();
        kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(Utils.buildKind(ExampleServiceState.class));
        q.query.addBooleanClause(kindClause);

        Query nameClause = new Query();
        nameClause.setTermPropertyName(ExampleServiceState.FIELD_NAME_NAME)
                .setTermMatchValue("document" + this.serviceCount);
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
                        targetHost.failIteration(new IllegalStateException("Incorrect number of documents returned: "
                                + "0 expected, but " + rsp.results.documentCount + " returned"));
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

    private void paginatedBroadcastQueryTasksRepeatSamePage(VerificationHost targetHost) throws Throwable {
        final int minPageCount = 3;
        final int maxPageSize = 100;
        final int resultLimit = Math.min(this.serviceCount / minPageCount, maxPageSize);

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
                            targetHost.failIteration(new IllegalStateException("Incorrect number of documents "
                                    + "returned: " + resultLimit + " was expected, but " + rsp.results.documentCount
                                    + " was returned."));
                            return;
                        }

                        targetHost.completeIteration();
                    });

            targetHost.send(startGet);
            targetHost.testWait();
        }

        assertTrue(documentLinksList.get(0).equals(documentLinksList.get(1)));
    }

    private void lowLevelBroadcastQueryTasksWithOwnerSelection(VerificationHost targetHost,
            EnumSet<QueryOption> queryOptions)
            throws Throwable {
        QuerySpecification q = new QuerySpecification();
        Query kindClause = new Query();
        kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(Utils.buildKind(ExampleServiceState.class));
        q.query = kindClause;
        q.options = queryOptions;

        QueryTask task = QueryTask.create(q);
        task.setDirect(true);

        URI localQueryTaskFactoryUri = UriUtils.buildUri(targetHost,
                ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);
        URI forwardingService = UriUtils.buildBroadcastRequestUri(localQueryTaskFactoryUri,
                task.nodeSelectorLink);

        TestContext testContext = targetHost.testCreate(1);
        // refer to LuceneQueryTaskService.createAndSendBroadcastQuery() to get the the internal result (before merge)
        // so we can get each node's local query result, then verify whether we have got the authoritative result
        Operation op = Operation
                .createPost(forwardingService)
                .setBody(task)
                .setReferer(targetHost.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        testContext.fail(e);
                        return;
                    }

                    NodeGroupBroadcastResponse rsp = o.getBody((NodeGroupBroadcastResponse.class));
                    NodeGroupBroadcastResult broadcastResponse = NodeGroupUtils.toBroadcastResult(rsp);

                    if (broadcastResponse.hasFailure()) {
                        testContext.fail(new IllegalStateException(
                                "Failures received: " + Utils.toJsonHtml(rsp)));
                        return;
                    }

                    // check the correctness of rsp.jsonResponses, the internal result (before merge)
                    int totalDocumentCount = 0;

                    for (QueryTask queryTask : broadcastResponse.getSuccessesAs(QueryTask.class)) {
                        // calculate the total document count from each node's local query result
                        totalDocumentCount += queryTask.results.documentCount;
                        String queryTaskDocumentOwner = queryTask.documentOwner;
                        // check whether each link's owner is the node itself
                        for (String link : queryTask.results.documentLinks) {
                            String linkOwner = Utils.fromJson(queryTask.results.documents.get(link),
                                    ServiceDocument.class).documentOwner;
                            // find non-authoritative result
                            if (!linkOwner.equals(queryTaskDocumentOwner)) {
                                testContext.fail(new IllegalStateException("Non-authoritative result returned: "
                                        + queryTaskDocumentOwner + " expected, but " + linkOwner + " returned"));
                                return;
                            }
                        }
                    }

                    // check the total documents count
                    if (this.serviceCount != totalDocumentCount) {
                        testContext.fail(new IllegalStateException("Incorrect number of documents returned: "
                                + this.serviceCount + " expected, but " + totalDocumentCount + " returned"));
                        return;
                    }

                    testContext.complete();
                });

        op.toggleOption(OperationOption.CONNECTION_SHARING, true);
        targetHost.send(op);
        testContext.await();
    }

    private void startPagedBroadCastQuery(VerificationHost targetHost) {
        final int documentCount = this.serviceCount;
        Set<String> documentLinks = new HashSet<>();
        try {
            final int minPageCount = 3;
            final int maxPageSize = 100;
            final int resultLimit = Math.min(documentCount / minPageCount, maxPageSize);

            QuerySpecification q = new QuerySpecification();
            Query kindClause = new Query();
            kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                    .setTermMatchValue(Utils.buildKind(ExampleServiceState.class));
            q.query = kindClause;
            q.options = EnumSet.of(QueryOption.EXPAND_CONTENT, QueryOption.BROADCAST);
            q.resultLimit = resultLimit;

            QueryTask task = QueryTask.create(q);
            if (task.documentExpirationTimeMicros == 0) {
                task.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(targetHost
                        .getOperationTimeoutMicros());
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
                TestContext ctx = this.host.testCreate(1);
                Operation get = Operation
                        .createGet(taskUri)
                        .addPragmaDirective(
                                Operation.PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY)
                        .setCompletion((o, e) -> {
                            if (e != null) {
                                targetHost.failIteration(e);
                                return;
                            }

                            QueryTask rsp = o.getBody(QueryTask.class);

                            if (rsp.taskInfo.stage == TaskStage.FINISHED
                                    || rsp.taskInfo.stage == TaskStage.FAILED
                                    || rsp.taskInfo.stage == TaskStage.CANCELLED) {

                                if (rsp.results.documentCount != 0) {
                                    targetHost.failIteration(new IllegalStateException(
                                            "Incorrect number of documents returned: "
                                            + "0 expected, but " + rsp.results.documentCount
                                            + " returned"));
                                    return;
                                }

                                String expectedPageLinkSegment = UriUtils.buildUriPath(
                                        ServiceUriPaths.CORE,
                                        BroadcastQueryPageService.SELF_LINK_PREFIX);
                                if (!rsp.results.nextPageLink.contains(expectedPageLinkSegment)) {
                                    targetHost.failIteration(new IllegalStateException(
                                            "Incorrect next page link returned: "
                                            + rsp.results.nextPageLink));
                                    return;
                                }

                                pageLinks.add(rsp.results.nextPageLink);
                            }

                            ctx.complete();
                        });
                targetHost.send(get);

                ctx.await();

                if (!pageLinks.isEmpty()) {
                    break;
                }

                Thread.sleep(100);
            } while (true);

            String nextPageLink = pageLinks.get(0);
            while (nextPageLink != null) {
                pageLinks.clear();
                URI u = UriUtils.buildUri(targetHost, nextPageLink);

                TestContext ctx = this.host.testCreate(1);
                Operation get = Operation
                        .createGet(u)
                        .addPragmaDirective(
                                Operation.PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY)
                        .setCompletion((o, e) -> {
                            if (e != null) {
                                targetHost.failIteration(e);
                                return;
                            }

                            QueryTask rsp = o.getBody(QueryTask.class);
                            pageLinks.add(rsp.results.nextPageLink);
                            documentLinks.addAll(rsp.results.documentLinks);

                            ctx.complete();
                        });

                targetHost.send(get);
                ctx.await();
                nextPageLink = pageLinks.isEmpty() ? null : pageLinks.get(0);
            }

            assertEquals(documentCount, documentLinks.size());

            for (int i = 0; i < documentCount; i++) {
                assertTrue(documentLinks
                        .contains(ExampleService.FACTORY_LINK + "/document" + i));
            }
            targetHost.completeIteration();
        } catch (Throwable e) {
            targetHost.failIteration(e);
            return;
        }
    }

    private boolean validateNativeContextIsNull(VerificationHost targetHost, QueryTask rsp) {
        if (rsp.querySpec.context.nativePage != null
                || rsp.querySpec.context.nativeQuery != null
                || rsp.querySpec.context.nativeSearcher != null
                || rsp.querySpec.context.nativeSort != null) {
            targetHost.failIteration(new IllegalStateException(
                    "native context fields are not null"));
            return false;
        }
        return true;
    }

    @Test
    public void verifyResponsePayloadSizeLimitChecks() throws Throwable {
        // set response payload limit to a small number to force error
        setUpHost(1024 * 50);
        int sc = this.serviceCount * 2;
        int versionCount = 2;
        List<URI> services = createQueryTargetServices(sc);
        QueryValidationServiceState newState = putStateOnQueryTargetServices(
                services, versionCount);

        // now issue a query that will effectively return a result set greater
        // than the allowed size limit.
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        q.options = EnumSet.of(QueryOption.EXPAND_CONTENT);
        q.query.setTermPropertyName(QueryValidationServiceState.FIELD_NAME_TEXT_VALUE)
                .setTermMatchValue(newState.textValue)
                .setTermMatchType(MatchType.PHRASE);

        boolean limitChecked = false;
        try {
            createWaitAndValidateQueryTask(versionCount, services, q, true, true);
        } catch (ProtocolException ex) {
            assertTrue(ex.getMessage().contains("/core/query-tasks returned error 500 for POST"));
            limitChecked = true;
        }
        assertTrue("Expected QueryTask failure with INTERNAL_SERVER_ERROR because"
                + "response payload size was over limit.", limitChecked);
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
        URI exampleFactoryURI = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        List<URI> exampleServices = new ArrayList<>();
        TestContext ctx = this.host.testCreate(this.serviceCount);
        ctx.logBefore();
        for (int i = 0; i < this.serviceCount; i++) {
            ExampleServiceState s = new ExampleServiceState();
            s.name = UUID.randomUUID().toString();
            s.documentSelfLink = s.name;
            exampleServices.add(UriUtils.buildUri(this.host.getUri(),
                    ExampleService.FACTORY_LINK, s.documentSelfLink));
            this.host.send(Operation.createPost(exampleFactoryURI)
                    .setBody(s)
                    .setCompletion(ctx.getCompletion()));

        }

        ctx.await();

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
            task.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(
                    task.documentExpirationTimeMicros);
        }

        this.host.logThroughput();
        URI taskURI = this.host.createQueryTaskService(task, false,
                task.taskInfo.isDirect, task, null);

        if (!task.taskInfo.isDirect) {
            task = this.host.waitForQueryTaskCompletion(task.querySpec, 0, 0,
                    taskURI, false, false);
        }

        ctx.logAfter();

        assertTrue(task.results.nextPageLink == null);

        if (task.querySpec.options.contains(QueryOption.TOP_RESULTS)) {
            assertTrue(task.results.documentLinks.size() == resultLimit);
        }

        List<String> documentLinks = task.results.documentLinks;
        validateSortedList(documentLinks);

        deleteServices(exampleServices);
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

    @Test
    public void multiFieldSortTestOnExampleStates() throws Throwable {
        doMultiFieldSortTestOnExampleStates(false, Integer.MAX_VALUE);
        doMultiFieldSortTestOnExampleStates(true, Integer.MAX_VALUE);
    }

    @Test
    public void topResultsWithMultiFieldSort() throws Throwable {
        doMultiFieldSortTestOnExampleStates(true, 10);
    }

    public void doMultiFieldSortTestOnExampleStates(boolean isDirect, int resultLimit) throws Throwable {
        setUpHost();
        URI exampleFactoryURI = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        List<URI> exampleServices = new ArrayList<>();
        TestContext ctx = this.host.testCreate(this.serviceCount);
        for (int i = 0; i < this.serviceCount; i++) {
            ExampleServiceState s = new ExampleServiceState();
            s.name = UUID.randomUUID().toString();
            s.documentSelfLink = s.name;
            s.sortedCounter = i % 5L;
            exampleServices.add(UriUtils.buildUri(this.host.getUri(),
                    ExampleService.FACTORY_LINK, s.documentSelfLink));
            this.host.send(Operation.createPost(exampleFactoryURI)
                    .setBody(s)
                    .setCompletion(ctx.getCompletion()));

        }

        ctx.await();

        Query kindClause = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();

        QueryTask.Builder queryTaskBuilder = isDirect ? QueryTask.Builder.createDirectTask()
                : QueryTask.Builder.create();
        QueryTask task = queryTaskBuilder
                .setQuery(kindClause)
                .orderDescending(ExampleServiceState.FIELD_NAME_SORTED_COUNTER, TypeName.LONG)
                .orderAscending(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING)
                .build();

        task.querySpec.resultLimit = resultLimit;
        if (resultLimit < Integer.MAX_VALUE) {
            task.querySpec.options.add(QueryOption.TOP_RESULTS);
        }

        task.querySpec.options.add(QueryOption.EXPAND_CONTENT);

        if (task.documentExpirationTimeMicros != 0) {
            // the value was set as an interval by the calling test. Make absolute here so
            // account for service creation above
            task.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(
                    task.documentExpirationTimeMicros);
        }

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

        validateMultiFieldSortedList(task.results);

        deleteServices(exampleServices);
    }

    private void validateMultiFieldSortedList(ServiceDocumentQueryResult result) {
        List<ExampleServiceState> sortedDocuments = new ArrayList<>();

        assertTrue(result.documents != null);
        result.documentLinks
                .forEach(k -> sortedDocuments.add(Utils.fromJson(result.documents.get(k), ExampleServiceState.class)));

        String lastName = null;
        Long lastSortedCount = null;
        for (ExampleServiceState state : sortedDocuments) {
            if (lastName == null && lastSortedCount == null) {
                lastName = state.name;
                lastSortedCount = state.sortedCounter;
                continue;
            }

            if (lastSortedCount != null) {
                // Verify that sorted counter is always sorted in descending
                assertTrue("Multi-sort on first field failed.", lastSortedCount >= state.sortedCounter);
                if (lastSortedCount.equals(state.sortedCounter)) {
                    // Verify that name is sorted ascending with in the same sortedCounter
                    // Will be skipped if name is the first element of sub-set
                    if (lastName != null) {
                        assertTrue("Multi-sort on second field failed.", state.name.compareTo(lastName) > 0);
                    }
                    lastName = state.name;
                } else {
                    // Switch to next sub-set
                    lastName = null;
                }
                lastSortedCount = state.sortedCounter;
            }
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
        URI exampleFactoryURI = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        List<URI> exampleServices = new ArrayList<>();
        this.host.testStart(serviceCount);
        Random r = new Random();
        for (int i = 0; i < serviceCount; i++) {
            ExampleServiceState s = new ExampleServiceState();
            s.name = UUID.randomUUID().toString();
            s.counter = new Long(Math.abs(r.nextLong()));
            s.sortedCounter = new Long(Math.abs(r.nextLong()));
            s.documentSelfLink = s.name;

            exampleServices.add(UriUtils.buildUri(this.host.getUri(),
                    ExampleService.FACTORY_LINK, s.documentSelfLink));

            this.host.send(Operation.createPost(exampleFactoryURI)
                    .setBody(s)
                    .setCompletion(this.host.getCompletion()));

        }
        this.host.testWait();

        queryAndValidateSortedResults(ExampleServiceState.FIELD_NAME_COUNTER, TypeName.LONG,
                exampleServices, resultLimit, isDirect);

        queryAndValidateSortedResults(ExampleServiceState.FIELD_NAME_SORTED_COUNTER, TypeName.LONG,
                exampleServices, resultLimit, isDirect);

        List<URI> toDelete = queryAndValidateSortedResults(ExampleServiceState.FIELD_NAME_NAME,
                TypeName.STRING, exampleServices, resultLimit, isDirect);

        deleteServices(toDelete);
    }

    private List<URI> queryAndValidateSortedResults(String propertyName, TypeName propertyType,
            List<URI> exampleServices, int resultLimit,
            boolean isDirect) throws Throwable {
        Query kindClause = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();

        QueryTask.Builder queryTaskBuilder = isDirect
                ? QueryTask.Builder.createDirectTask()
                : QueryTask.Builder.create();

        queryTaskBuilder
                .setQuery(kindClause)
                .orderDescending(propertyName, propertyType)
                .addOption(QueryOption.EXPAND_CONTENT)
                .setResultLimit(resultLimit);

        QueryTask task = queryTaskBuilder.build();

        if (task.documentExpirationTimeMicros != 0) {
            // the value was set as an interval by the calling test. Make absolute here so
            // account for service creation above
            task.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(
                    task.documentExpirationTimeMicros);
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
        final int[] numberOfDocumentLinks = {task.results.documentLinks.size()};
        assertTrue(numberOfDocumentLinks[0] == 0);

        List<URI> toDelete = new ArrayList<>(exampleServices);
        List<URI> pageLinks = new ArrayList<>();
        this.host.testStart(1);
        getNextPageResults(nextPageLink, resultLimit, numberOfDocumentLinks, toDelete, documents,
                pageLinks);
        this.host.testWait();

        assertEquals(exampleServices.size(), numberOfDocumentLinks[0]);
        validateSortedResults(documents, propertyName);

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

        assertEquals(exampleServices.size(), numberOfDocumentLinks[0]);
        validateSortedResults(documents, ServiceDocument.FIELD_NAME_SELF_LINK);

        return toDelete;
    }

    private void validateSortedResults(List<ExampleServiceState> documents, String fieldName) {
        ExampleServiceState prevDoc = documents.get(0);

        for (int i = 1; i < documents.size(); i++) {
            ExampleServiceState currentDoc = documents.get(i);
            this.host.log("%s", currentDoc.documentSelfLink);
            if (fieldName.equals(ServiceDocument.FIELD_NAME_SELF_LINK)) {
                int r = currentDoc.documentSelfLink.compareTo(prevDoc.documentSelfLink);
                assertTrue("Sort by self link failed", r > 0);
            } else if (fieldName.equals(ExampleServiceState.FIELD_NAME_COUNTER)) {
                assertTrue("Sort Test Failed", currentDoc.counter < prevDoc.counter);
            } else if (fieldName.equals(ExampleServiceState.FIELD_NAME_SORTED_COUNTER)) {
                assertTrue("Sort Test Failed", currentDoc.sortedCounter < prevDoc.sortedCounter);
            }
            prevDoc = currentDoc;
        }
    }

    public void doSelfLinkTest(int serviceCount, int versionCount, boolean forceRemote)
            throws Throwable {

        String prefix = "testPrefix";
        List<URI> services = createQueryTargetServices(serviceCount);

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

        verifyNoPaginatedIndexSearchers();
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
        List<URI> services = createQueryTargetServices(sc);
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

        long longMin = LONG_START_VALUE;
        long longMax = LONG_START_VALUE + sc - 1;

        // do inclusive range search
        q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(longFieldName).setNumericRange(
                NumericRange.createLongRange(longMin, longMax, true, true));
        u = this.host.createQueryTaskService(QueryTask.create(q));
        finishedTaskState = this.host.waitForQueryTaskCompletion(q, services.size(),
                versionCount, u, false, true);
        assertTrue(finishedTaskState.results != null);
        assertTrue(finishedTaskState.results.documentLinks != null);
        assertEquals(finishedTaskState.results.documentLinks.size(), sc);

        // do min side open range search
        q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(longFieldName).setNumericRange(
                NumericRange.createLongRange(null, longMax, true, true));
        u = this.host.createQueryTaskService(QueryTask.create(q));
        finishedTaskState = this.host.waitForQueryTaskCompletion(q, services.size(),
                versionCount, u, false, true);
        assertTrue(finishedTaskState.results != null);
        assertTrue(finishedTaskState.results.documentLinks != null);
        assertTrue(finishedTaskState.results.documentLinks.size() == sc);

        // do max side open range search
        q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(longFieldName).setNumericRange(
                NumericRange.createLongRange(longMin, null, true, true));
        u = this.host.createQueryTaskService(QueryTask.create(q));
        finishedTaskState = this.host.waitForQueryTaskCompletion(q, services.size(),
                versionCount, u, false, true);
        assertTrue(finishedTaskState.results != null);
        assertTrue(finishedTaskState.results.documentLinks != null);
        assertTrue(finishedTaskState.results.documentLinks.size() == sc);

        double doubleMin = (LONG_START_VALUE * 0.1) + DOUBLE_MIN_OFFSET;
        double doubleMax = (LONG_START_VALUE * 0.1) + DOUBLE_MIN_OFFSET + (sc * 0.1);

        // double fields are 1 / 10 of the long fields
        // do double inclusive range search
        q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(doubleFieldName).setNumericRange(
                NumericRange.createDoubleRange(doubleMin, doubleMax, true, true));
        u = this.host.createQueryTaskService(QueryTask.create(q));
        finishedTaskState = this.host.waitForQueryTaskCompletion(q, services.size(),
                versionCount, u, false, true);
        assertTrue(finishedTaskState.results != null);
        assertTrue(finishedTaskState.results.documentLinks != null);
        assertTrue(finishedTaskState.results.documentLinks.size() == sc);

        // do double range search with min inclusive
        q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(doubleFieldName).setNumericRange(
                NumericRange.createDoubleRange(doubleMin, doubleMin + sc * 0.05,
                        true, false));
        u = this.host.createQueryTaskService(QueryTask.create(q));
        finishedTaskState = this.host.waitForQueryTaskCompletion(q, services.size(),
                versionCount, u, false, true);
        assertTrue(finishedTaskState.results != null);
        assertTrue(finishedTaskState.results.documentLinks != null);
        assertTrue(finishedTaskState.results.documentLinks.size() == sc / 2);
        verifyNoPaginatedIndexSearchers();

        // do min side open range search
        q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(doubleFieldName).setNumericRange(
                NumericRange.createDoubleRange(null, doubleMax, true, true));
        u = this.host.createQueryTaskService(QueryTask.create(q));
        finishedTaskState = this.host.waitForQueryTaskCompletion(q, services.size(),
                versionCount, u, false, true);
        assertTrue(finishedTaskState.results != null);
        assertTrue(finishedTaskState.results.documentLinks != null);
        assertTrue(finishedTaskState.results.documentLinks.size() == sc);

        // do max side open range search
        q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(doubleFieldName).setNumericRange(
                NumericRange.createDoubleRange(doubleMin, null, true, true));
        u = this.host.createQueryTaskService(QueryTask.create(q));
        finishedTaskState = this.host.waitForQueryTaskCompletion(q, services.size(),
                versionCount, u, false, true);
        assertTrue(finishedTaskState.results != null);
        assertTrue(finishedTaskState.results.documentLinks != null);
        assertTrue(finishedTaskState.results.documentLinks.size() == sc);
    }

    @Test
    public void testTextMatch() throws Throwable {
        doStringAndTextMatchTest(false, false);
    }

    @Test
    public void testTextMatchRemote() throws Throwable {
        doStringAndTextMatchTest(true, false);
    }

    @Test
    public void testTextMatchRemoteDirect() throws Throwable {
        doStringAndTextMatchTest(true, true);
    }

    public void doStringAndTextMatchTest(boolean forceRemote, boolean isDirect) throws Throwable {
        setUpHost();
        int sc = this.serviceCount;
        int versionCount = 2;
        List<URI> services = createQueryTargetServices(sc);

        // PUT a new state on all services, with one field set to the same
        // value;
        QueryValidationServiceState newState = putStateOnQueryTargetServices(
                services, versionCount);

        // now issue a query that does an exact match on the String value using
        // the complete phrase
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        q.options = EnumSet.of(QueryOption.EXPAND_CONTENT);

        q.query.setTermPropertyName(QueryValidationServiceState.FIELD_NAME_TEXT_VALUE)
                .setTermMatchValue(newState.textValue)
                .setTermMatchType(MatchType.PHRASE);

        createWaitAndValidateQueryTask(versionCount, services, q, forceRemote, isDirect);

        // to an a lower case conversion then do a term query. Since the field is marked with
        // IndexingOption.CASE_INSENSITIVE, we must convert the query value to lower case
        q = new QueryTask.QuerySpecification();
        q.query = Query.Builder.create().addCaseInsensitiveFieldClause(
                QueryValidationServiceState.FIELD_NAME_STRING_VALUE,
                newState.stringValue, MatchType.TERM, Occurance.MUST_OCCUR).build();
        q.options = EnumSet.of(QueryOption.EXPAND_CONTENT);

        QueryTask taskResult = createWaitAndValidateQueryTask(
                versionCount, services, q, forceRemote, isDirect);
        for (Object doc : taskResult.results.documents.values()) {
            QueryValidationServiceState state = Utils.fromJson(doc,
                    QueryValidationServiceState.class);
            // verify original case is preserved
            assertTrue(state.stringValue.equals(STRING_VALUE));
        }

        // now do a "contains" search on terms using wild cards, although this
        // will be much slower
        String term = newState.textValue.split(" ")[1];
        term = term.substring(1, term.length() - 2);
        term = UriUtils.URI_WILDCARD_CHAR + term + UriUtils.URI_WILDCARD_CHAR;

        q.query = new QueryTask.Query();
        q.query.setTermPropertyName(QueryValidationServiceState.FIELD_NAME_TEXT_VALUE)
                .setTermMatchValue(term)
                .setTermMatchType(MatchType.WILDCARD);
        createWaitAndValidateQueryTask(versionCount, services, q, forceRemote);
        // now do a "contains" search without using wild cards, just a plain
        // term. This only
        // works if the string is a phrase that can be tokenized with the
        // default tokenizers
        String word = TEXT_VALUE.split(" ")[1];

        q.query = new QueryTask.Query();
        q.query.setTermPropertyName(QueryValidationServiceState.FIELD_NAME_TEXT_VALUE)
                .setTermMatchValue(word)
                .setTermMatchType(MatchType.TERM);

        createWaitAndValidateQueryTask(versionCount, services, q, forceRemote);

        String prefix = word.substring(0, word.length() - 1);
        // finally issue a prefix query
        q.query = new QueryTask.Query();
        q.query.setTermPropertyName(QueryValidationServiceState.FIELD_NAME_TEXT_VALUE)
                .setTermMatchValue(prefix)
                .setTermMatchType(MatchType.PREFIX);
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
                .addFieldClause(QueryValidationServiceState.FIELD_NAME_TEXT_VALUE,
                        TEXT_VALUE.split(" ")[1])
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
                .addFieldClause(QueryValidationServiceState.FIELD_NAME_SERVICE_LINK, SERVICE_LINK_VALUE)
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
        List<URI> services = createQueryTargetServices(sc);

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
        q.query.setTermPropertyName(QueryValidationServiceState.FIELD_NAME_TEXT_VALUE)
                .setTermMatchValue(TEXT_VALUE)
                .setTermMatchType(MatchType.PHRASE);

        QueryTask task = QueryTask.create(q);
        task.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(TimeUnit.MILLISECONDS
                .toMicros(250));

        URI taskURI = this.host.createQueryTaskService(task, false, false, task, null);
        this.host.waitFor("task did not expire", () -> {
            if (this.host.getServiceStage(taskURI.getPath()) != null) {
                return false;
            }
            return true;
        });

        verifyTaskAutoExpiration(taskURI);
        verifyPaginatedIndexSearcherExpiration();
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
                List<URI> services = createQueryTargetServices(sc);

                putStateOnQueryTargetServices(services, versions);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        };

        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();

        q.expectedResultCount = Long.valueOf(expectedCount);

        q.query.setTermPropertyName(QueryValidationServiceState.FIELD_NAME_TEXT_VALUE)
                .setTermMatchValue(TEXT_VALUE)
                .setTermMatchType(MatchType.PHRASE);

        long exp = Utils.fromNowMicrosUtc(TimeUnit.SECONDS.toMicros(30));
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
        task.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(TimeUnit.SECONDS
                .toMicros(1));

        taskURI = this.host.createQueryTaskService(task, false,
                false, task, null);

        verifyTaskAutoExpiration(taskURI);
        this.host.log("Query task has expired: %s", taskURI.getPath());

        verifyNoPaginatedIndexSearchers();
    }

    private void verifyNoPaginatedIndexSearchers() throws Throwable {
        // verify that paginated index searchers did not get created
        URI indexStatsUri = UriUtils.buildStatsUri(this.host.getDocumentIndexServiceUri());
        ServiceStats stats = this.host.getServiceState(null, ServiceStats.class,
                indexStatsUri);
        ServiceStat pgqStat = stats.entries
                .get(LuceneDocumentIndexService.STAT_NAME_ACTIVE_PAGINATED_QUERIES);
        if (pgqStat != null && pgqStat.latestValue > 0) {
            throw new IllegalStateException("Found paginated index searchers, not expected");
        }
    }

    private URI doPaginatedQueryTest(QueryTask task, int sc, int resultLimit,
            List<URI> queryPageURIs, List<URI> targetServiceURIs) throws Throwable {
        return doPaginatedQueryTest(task, sc, sc, resultLimit, queryPageURIs, targetServiceURIs);
    }

    private URI doPaginatedQueryTest(QueryTask task, int sc, int expectedResults, int resultLimit,
            List<URI> queryPageURIs, List<URI> targetServiceURIs) throws Throwable {
        List<URI> services = createQueryTargetServices(sc);
        if (targetServiceURIs == null) {
            targetServiceURIs = new ArrayList<>();
        }

        targetServiceURIs.addAll(services);
        QueryValidationServiceState newState = putStateOnQueryTargetServices(
                services, 1);

        if (task.querySpec.options.contains(QueryOption.EXPAND_LINKS)) {
            patchQueryTargetServiceLinksWithExampleLinks(targetServiceURIs);
            task.querySpec.linkTerms = new ArrayList<>();
            QueryTerm linkTerm = new QueryTerm();
            linkTerm.propertyName = QueryValidationServiceState.FIELD_NAME_SERVICE_LINK;
            linkTerm.propertyType = TypeName.STRING;
            task.querySpec.linkTerms.add(linkTerm);
        }

        task.querySpec.resultLimit = resultLimit;

        task.querySpec.query.setTermPropertyName(QueryValidationServiceState.FIELD_NAME_TEXT_VALUE)
                .setTermMatchValue(newState.textValue)
                .setTermMatchType(MatchType.PHRASE);

        if (task.documentExpirationTimeMicros != 0) {
            // the value was set as an interval by the calling test. Make absolute here so
            // account for service creation above
            task.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(
                    task.documentExpirationTimeMicros);
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

        final int[] numberOfDocumentLinks = {task.results.documentLinks.size()};

        assertEquals(0, numberOfDocumentLinks[0]);

        // update the index after the paginated query has been created to verify that its
        // stable while index searchers are updated
        services = createQueryTargetServices(10);
        targetServiceURIs.addAll(services);
        newState = putStateOnQueryTargetServices(services, 1);

        this.host.testStart(1);
        getNextPageLinks(task, nextPageLink, resultLimit, numberOfDocumentLinks, queryPageURIs);
        this.host.testWait();

        assertEquals(expectedResults, numberOfDocumentLinks[0]);

        if (expectedResults != resultLimit) {
            return taskURI;
        }

        // get page results with a modified limit, for the specific GET on this page. Expect a different
        // nextPageLink and a different number of results
        URI firstPageURI = UriUtils.buildUri(this.host, nextPageLink);
        QueryTask defaultResultsFromPage = this.host.getServiceState(null, QueryTask.class,
                firstPageURI);
        int newLimit = resultLimit / 2;
        String modifiedLink = UriUtils.extendQueryPageLinkWithQuery(nextPageLink,
                UriUtils.URI_PARAM_ODATA_LIMIT + "=" + newLimit);
        URI firstPageWithLimitURI = UriUtils.buildUri(this.host, modifiedLink);
        QueryTask modifiedLimitResultsFromPage = this.host.getServiceState(null, QueryTask.class,
                firstPageWithLimitURI);
        assertEquals((long) newLimit, (long) modifiedLimitResultsFromPage.results.documentCount);
        assertEquals(newLimit, modifiedLimitResultsFromPage.results.documentLinks.size());
        assertTrue(
                modifiedLimitResultsFromPage.results.nextPageLink != defaultResultsFromPage.results.nextPageLink);

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
        task.documentExpirationTimeMicros = Utils
                .fromNowMicrosUtc(TimeUnit.DAYS.toMicros(1));

        List<URI> pageServiceURIs = new ArrayList<>();
        List<URI> targetServiceURIs = new ArrayList<>();
        verifyPaginatedQueryWithSearcherRefresh(sc, resultLimit, task, pageServiceURIs,
                targetServiceURIs);

        deleteServices(targetServiceURIs);

        // direct query, with expand links
        task = QueryTask.create(new QuerySpecification()).setDirect(true);
        task.querySpec.options.add(QueryOption.SELECT_LINKS);

        task.querySpec.options.add(QueryOption.EXPAND_LINKS);
        pageServiceURIs = new ArrayList<>();
        targetServiceURIs = new ArrayList<>();
        doPaginatedQueryTest(task, sc, resultLimit, pageServiceURIs, targetServiceURIs);
        String nextPageLink = task.results.nextPageLink;
        assertNotNull(nextPageLink);

        deleteServices(targetServiceURIs);

        Long initialRefreshCount = getPaginatedSearcherRefreshCount();
        assertNotNull(initialRefreshCount);

        // direct query, without searcher refresh
        task = QueryTask.create(new QuerySpecification()).setDirect(true);
        task.querySpec.options.add(QueryOption.DO_NOT_REFRESH);
        pageServiceURIs = new ArrayList<>();
        targetServiceURIs = new ArrayList<>();
        doPaginatedQueryTest(task, 0, sc, resultLimit, pageServiceURIs, targetServiceURIs);

        Long finalRefreshCount = getPaginatedSearcherRefreshCount();
        assertNotNull(finalRefreshCount);
        assertEquals(initialRefreshCount, finalRefreshCount);

        deleteServices(targetServiceURIs);

        sc = 1;
        // direct query, single result expected, plus verify all previously deleted and created
        // documents are ignored
        task = QueryTask.create(new QuerySpecification()).setDirect(true);
        pageServiceURIs = new ArrayList<>();
        targetServiceURIs = new ArrayList<>();
        doPaginatedQueryTest(task, sc, resultLimit, pageServiceURIs, targetServiceURIs);
        nextPageLink = task.results.nextPageLink;
        assertNotNull(nextPageLink);

        // delete target services before doing next query to verify deleted documents are excluded
        deleteServices(targetServiceURIs);

        sc = 0;

        // direct query, no results expected,
        task = QueryTask.create(new QuerySpecification()).setDirect(true);
        pageServiceURIs = new ArrayList<>();
        targetServiceURIs = new ArrayList<>();
        doPaginatedQueryTest(task, sc, resultLimit, pageServiceURIs, targetServiceURIs);
    }

    private Long getPaginatedSearcherRefreshCount() {
        URI luceneStatsUri = UriUtils.buildStatsUri(this.host.getDocumentIndexServiceUri());
        ServiceStats stats = this.host.getServiceState(null, ServiceStats.class, luceneStatsUri);
        ServiceStat paginatedSearcherRefreshCountStat = stats.entries.get(
                LuceneDocumentIndexService.STAT_NAME_PAGINATED_SEARCHER_UPDATE_COUNT
                + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
        if (paginatedSearcherRefreshCountStat == null) {
            return null;
        }

        return (long) paginatedSearcherRefreshCountStat.latestValue;
    }

    private void patchQueryTargetServiceLinksWithExampleLinks(List<URI> targetServiceURIs)
            throws Throwable {
        // patch query target services with links to example services, then request link
        // expansion
        List<URI> exampleServices = new ArrayList<>();
        createExampleServices(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK),
                exampleServices);
        List<String> serviceLinks = exampleServices.stream().map(URI::getPath)
                .collect(Collectors.toList());
        TestContext ctx = this.host.testCreate(this.serviceCount);
        for (int i = 0; i < targetServiceURIs.size(); i++) {
            URI queryTargetService = targetServiceURIs.get(i);
            URI exampleService = exampleServices.get(i);
            QueryValidationServiceState patchBody = new QueryValidationServiceState();
            patchBody.serviceLink = exampleService.getPath();
            patchBody.serviceLinks = serviceLinks;
            Operation patch = Operation.createPatch(queryTargetService)
                    .setBody(patchBody)
                    .setCompletion(ctx.getCompletion());
            this.host.send(patch);
        }
        this.host.testWait(ctx);
    }

    private void deleteServices(List<URI> targetServiceURIs) throws Throwable {
        this.host.testStart(targetServiceURIs.size());
        for (URI u : targetServiceURIs) {
            this.host.send(Operation.createDelete(u)
                    .setBody(new ServiceDocument())
                    .setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();
    }

    private void verifyPaginatedQueryWithSearcherRefresh(int sc, int resultLimit, QueryTask task,
            List<URI> pageServiceURIs, List<URI> targetServiceURIs)
            throws Throwable {

        try {
            // set some aggressive grooming limits on files
            LuceneDocumentIndexService.setIndexFileCountThresholdForWriterRefresh(10);

            doPaginatedQueryTest(task, sc, resultLimit, pageServiceURIs, targetServiceURIs);
            // do some un related updates to force the index to create new index searchers
            putStateOnQueryTargetServices(targetServiceURIs, 10);
            // create some new index searchers by doing a query
            this.throughputSimpleQuery();

            // verify first page is still valid
            this.host.testStart(pageServiceURIs.size());
            boolean isFirstPage = true;
            for (URI u : pageServiceURIs) {

                Operation get = Operation.createGet(u);
                if (isFirstPage) {
                    // First page should succeed either because grooming has not closed the writer yet
                    // or because of the retry. Over many runs, this will prove the recovery code in
                    // LuceneQueryPageService works
                    get.setCompletion(this.host.getCompletion());
                } else {
                    // request to remaining pages might or not fail. Its nearly impossible to avoid
                    // false negatives in this test since it depends on when the maintenance handler
                    // runs for the lucene service and grooms the searcher.
                    get.setCompletion((o, e) -> {
                        this.host.completeIteration();
                    });
                }
                this.host.send(get);
                isFirstPage = false;
            }
            this.host.testWait();
        } finally {
            // restore default numbers
            LuceneDocumentIndexService.setIndexFileCountThresholdForWriterRefresh(
                    LuceneDocumentIndexService
                            .DEFAULT_INDEX_FILE_COUNT_THRESHOLD_FOR_WRITER_REFRESH);
        }
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
        this.host.waitFor("Query task did not expire", () -> {
            TestContext ctx = this.host.testCreate(serviceURIs.size());
            AtomicInteger remaining = new AtomicInteger(serviceURIs.size());
            for (URI u : serviceURIs) {
                Operation get = Operation.createGet(u).setCompletion((o, e) -> {
                    if (e != null && (e instanceof ServiceNotFoundException)) {
                        remaining.decrementAndGet();
                    }
                    ctx.completeIteration();
                });

                this.host.send(get);
            }

            this.host.testWait(ctx);
            return remaining.get() == 0;
        });

        verifyPaginatedIndexSearcherExpiration();
    }

    private void getNextPageLinks(QueryTask task, String nextPageLink, int resultLimit,
            final int[] numberOfDocumentLinks, final List<URI> serviceURIs) {

        URI u = UriUtils.buildUri(this.host, nextPageLink);
        serviceURIs.add(u);

        CompletionHandler c = (o, e) -> {
            try {
                if (e != null) {
                    this.host.failIteration(e);
                    return;
                }

                QueryTask page = o.getBody(QueryTask.class);
                int nlinks = page.results.documentLinks.size();
                this.host.log("page: %s", Utils.toJsonHtml(page));
                assertTrue(nlinks <= resultLimit);
                assertTrue(page.querySpec.context == null);
                assertEquals(task.documentExpirationTimeMicros, page.documentExpirationTimeMicros);
                verifyLinks(nextPageLink, serviceURIs, page);

                numberOfDocumentLinks[0] += nlinks;

                if (page.results.nextPageLink == null || nlinks == 0) {
                    // complete only when we are out of pages
                    this.host.completeIteration();
                    return;
                }

                if (task.querySpec.options.contains(QueryOption.EXPAND_LINKS)) {
                    validateExpandLinksResults(page);
                }

                getNextPageLinks(task, page.results.nextPageLink,
                        resultLimit, numberOfDocumentLinks, serviceURIs);
            } catch (Throwable e1) {
                this.host.failIteration(e1);
            }
        };

        Operation get = Operation
                .createGet(u)
                .setCompletion(c);

        this.host.send(get);
    }

    private void validateSelectLinksQueryResults(QueryTask.QuerySpecification q, QueryTask task) {
        assertTrue(!task.results.selectedLinksPerDocument.isEmpty());
        assertTrue(!task.results.selectedLinks.isEmpty());

        Set<String> uniqueLinks = new HashSet<>();

        for (QueryTerm link : task.querySpec.linkTerms) {
            for (String selflink : task.results.documentLinks) {
                Map<String, String> selectedLinks = task.results.selectedLinksPerDocument.get(selflink);
                assertTrue(!selectedLinks.isEmpty());

                if (QueryValidationServiceState.FIELD_NAME_SERVICE_LINK
                        .equals(link.propertyName)) {
                    String linkValue = selectedLinks.get(link.propertyName);
                    assertEquals(SERVICE_LINK_VALUE, linkValue);
                    uniqueLinks.add(linkValue);
                } else if (QueryValidationServiceState.FIELD_NAME_SERVICE_LINKS
                        .equals(link.propertyName)) {
                    for (Entry<String, String> e : selectedLinks.entrySet()) {
                        assertTrue(e.getKey().startsWith(
                                QueryValidationServiceState.FIELD_NAME_SERVICE_LINKS));
                        uniqueLinks.add(e.getValue());
                        assertTrue(e.getValue().startsWith(SERVICE_LINK_VALUE));
                    }
                } else {
                    throw new IllegalStateException("Unexpected link property: "
                            + Utils.toJsonHtml(task.results));
                }
            }
        }
        assertEquals(uniqueLinks.size(), task.results.selectedLinks.size());
    }

    private void validateExpandLinksResults(QueryTask page) {
        assertEquals(page.results.documentLinks.size(), page.results.selectedLinksPerDocument.size());
        assertEquals(page.results.documentLinks.size(), page.results.selectedLinks.size());
        // since QueryValidationServiceState contains a single "serviceLink" field, we expect
        // a single Map, per document. The map should contain the link property name, and the
        // expanded value of the link, in this case a ExampleService state instance.
        Set<String> uniqueLinks = new HashSet<>();
        for (Map<String, String> selectedLinksPerDocument : page.results.selectedLinksPerDocument.values()) {
            for (Entry<String, String> entry : selectedLinksPerDocument.entrySet()) {
                String key = entry.getKey();
                if (!key.equals(QueryValidationServiceState.FIELD_NAME_SERVICE_LINK)
                        && !key.startsWith(QuerySpecification.buildCollectionItemName(
                        QueryValidationServiceState.FIELD_NAME_SERVICE_LINKS))) {
                    continue;
                }
                String link = entry.getValue();
                uniqueLinks.add(link);
                Object doc = page.results.selectedDocuments.get(link);
                ExampleServiceState expandedState = Utils.fromJson(doc,
                        ExampleServiceState.class);
                assertEquals(Utils.buildKind(ExampleServiceState.class), expandedState.documentKind);
            }
        }
        assertEquals(page.results.documentLinks.size(), uniqueLinks.size());
    }

    private void validatedExpandLinksResultsWithBogusLink(QueryTask queryTask,
            URI queryValidationServiceWithBrokenServiceLink) {
        assertEquals(this.serviceCount, queryTask.results.selectedLinksPerDocument.size());
        for (Entry<String, Map<String, String>> e : queryTask.results.selectedLinksPerDocument.entrySet()) {
            for (Entry<String, String> linkToExpandedState : e.getValue().entrySet()) {
                String link = linkToExpandedState.getValue();
                Object doc = queryTask.results.selectedDocuments.get(link);
                if (!e.getKey().equals(queryValidationServiceWithBrokenServiceLink.getPath())) {
                    ExampleServiceState st = Utils.fromJson(doc, ExampleServiceState.class);
                    assertEquals(Utils.buildKind(ExampleServiceState.class), st.documentKind);
                    continue;
                }
                ServiceErrorResponse error = Utils.fromJson(doc, ServiceErrorResponse.class);
                assertEquals(Operation.STATUS_CODE_NOT_FOUND, error.statusCode);
            }
        }
    }

    private void verifyLinks(String nextPageLink, List<URI> serviceURIs, QueryTask page) {
        assertEquals(QueryPageService.KIND, page.documentKind);
        assertNotEquals(nextPageLink, page.results.nextPageLink);
        assertNotEquals(nextPageLink, page.results.prevPageLink);

        if (serviceURIs.size() >= 1) {
            URI currentPageForwardUri = UriUtils.buildForwardToQueryPageUri(
                    UriUtils.buildUri(this.host, page.documentSelfLink), this.host.getId());
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

        if (includeAllVersions) {
            q.options.add(QueryOption.EXPAND_CONTENT);
            u = this.host.createQueryTaskService(QueryTask.create(q));
            finishedTaskState = this.host.waitForQueryTaskCompletion(q,
                    sc, versionCount, u, false, true);
            for (Object o : finishedTaskState.results.documents.values()) {
                QueryValidationServiceState s = Utils.fromJson(o, QueryValidationServiceState.class);
                assertTrue(!s.documentSelfLink.contains(ServiceDocument.FIELD_NAME_VERSION));
            }
        }
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
        List<URI> services = createQueryTargetServices(sc);
        QueryValidationServiceState newState = new QueryValidationServiceState();
        double currentStat;
        double newStat;
        int counter = 0;

        for (int i = 0; i < iter; i++) {
            newState.textValue = "current";
            newState = putSimpleStateOnQueryTargetServices(services, newState);
            QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
            Query kindClause = new Query();
            kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                    .setTermMatchValue(Utils.buildKind(QueryValidationServiceState.class));
            q.query = kindClause;
            QueryTask task = QueryTask.create(q);
            task.setDirect(true);
            this.host.createQueryTaskService(task, false, task.taskInfo.isDirect, task, null);
            newState.textValue = "new";
            newState = putSimpleStateOnQueryTargetServices(services, newState);

            URI luceneStatsUri = UriUtils.buildStatsUri(this.host.getDocumentIndexServiceUri());
            ServiceStats stats = this.host
                    .getServiceState(null, ServiceStats.class, luceneStatsUri);
            ServiceStat searcherUpdateBeforeQuery = stats.entries
                    .get(LuceneDocumentIndexService.STAT_NAME_SEARCHER_UPDATE_COUNT
                            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
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
                    .get(LuceneDocumentIndexService.STAT_NAME_SEARCHER_UPDATE_COUNT
                            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
            newStat = searcherUpdateAfterQuery.latestValue;
            if (currentStat == newStat) {
                counter++;
            }
        }

        assertTrue(String.format("Could not re-use index searcher in %d attempts", iter),
                counter > 0);
    }

    private QueryTask createWaitAndValidateQueryTask(long versionCount,
            List<URI> services, QueryTask.QuerySpecification q, boolean forceRemote,
            boolean isDirect)
            throws Throwable {
        QueryTask task = QueryTask.create(q).setDirect(isDirect);
        if (q.options == null) {
            q.options = EnumSet.noneOf(QueryOption.class);
        }
        if (isDirect) {
            task.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(
                    +TimeUnit.MILLISECONDS.toMicros(100));
        }
        URI u = this.host.createQueryTaskService(task, forceRemote,
                isDirect, task, null);
        if (!isDirect) {
            task = this.host.waitForQueryTaskCompletion(q, services.size(), (int) versionCount, u,
                    forceRemote, true);
        }

        if (q.options.contains(QueryOption.COUNT)) {
            assertTrue(task.results.documentCount != null);
            assertTrue(task.results.documentCount == services.size() * (versionCount + 1));
            return task;
        }

        validateFinishedQueryTask(services, task);

        if (isDirect) {
            verifyTaskAutoExpiration(u);
        }

        if (q.options.contains(QueryOption.EXPAND_CONTENT)) {
            assertTrue(task.results.documentLinks.size() == task.results.documents
                    .size());
        }

        if (q.options.contains(QueryOption.EXPAND_LINKS)) {
            validateExpandLinksResults(task);
            return task;
        }

        if (q.options.contains(QueryOption.SELECT_LINKS)) {
            validateSelectLinksQueryResults(q, task);
        }

        return task;
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
        this.host.waitFor("task never expired", () -> {
            ServiceDocumentQueryResult r = this.host.getServiceState(null,
                    ServiceDocumentQueryResult.class,
                    UriUtils.buildUri(this.host, QueryTaskFactoryService.class));

            if (r.documentLinks == null) {
                return true;
            }

            for (String link : r.documentLinks) {
                if (u.getPath().equals(link)) {
                    return false;
                }
            }
            return true;
        });
    }

    private void verifyPaginatedIndexSearcherExpiration() throws Throwable {
        this.host.waitFor("Paginated index searchers never expired", () -> {
            URI indexStatsUri = UriUtils.buildStatsUri(this.host.getDocumentIndexServiceUri());
            // check the index statistic tracking paginated queries
            ServiceStats stats = this.host.getServiceState(null, ServiceStats.class,
                    indexStatsUri);
            ServiceStat pgqStat = stats.entries
                    .get(LuceneDocumentIndexService.STAT_NAME_ACTIVE_PAGINATED_QUERIES);
            if (pgqStat != null && pgqStat.latestValue != 0) {
                return false;
            }
            return true;
        });
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
        newState.textValue = TEXT_VALUE;
        return putStateOnQueryTargetServices(services, versionsPerService,
                newState);
    }

    private QueryValidationServiceState putStateOnQueryTargetServices(
            List<URI> services, int versionsPerService,
            QueryValidationServiceState templateState) throws Throwable {

        this.host.testStart(services.size() * versionsPerService);
        Random r = new Random();
        long k = LONG_START_VALUE;
        templateState.mapOfLongs = new HashMap<>();
        templateState.mapOfDoubles = new HashMap<>();
        for (URI u : services) {
            templateState.longValue = k;
            templateState.doubleValue = (double) k++;
            templateState.doubleValue *= 0.1;
            templateState.doubleValue += DOUBLE_MIN_OFFSET;
            templateState.mapOfLongs.put("long", templateState.longValue);
            templateState.mapOfDoubles.put("double", templateState.doubleValue);
            templateState.stringValue = STRING_VALUE;
            templateState.textValue = TEXT_VALUE;
            templateState.serviceLink = SERVICE_LINK_VALUE;

            templateState.serviceLinks = new ArrayList<>();
            for (int i = 0; i < SERVICE_LINK_COUNT; i++) {
                templateState.serviceLinks.add(SERVICE_LINK_VALUE + "." + i);
            }

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
            List<URI> services, QueryValidationServiceState templateState) throws Throwable {
        return putSimpleStateOnQueryTargetServices(services, templateState, false);
    }

    private QueryValidationServiceState putSimpleStateOnQueryTargetServices(
            List<URI> services, QueryValidationServiceState templateState, boolean commitAfter)
            throws Throwable {

        this.host.testStart(services.size());
        for (URI u : services) {
            templateState.id = Utils.getNowMicrosUtc() + "";
            Operation put = Operation.createPut(u).setBody(templateState)
                    .setCompletion(this.host.getCompletion());
            this.host.send(put);
        }
        this.host.testWait();

        if (commitAfter) {
            TestContext ctx = this.host.testCreate(1);
            Operation post = Operation.createPost(this.host.getDocumentIndexServiceUri())
                    .setBody(new LuceneDocumentIndexService.MaintenanceRequest())
                    .setCompletion(ctx.getCompletion());
            this.host.send(post);
            this.host.testWait(ctx);
        }

        this.host.logThroughput();
        return templateState;
    }

    private List<URI> createQueryTargetServices(int serviceCount)
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
        URI exampleFactoryUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        URI tenantFactoryUri = UriUtils.buildUri(this.host, TenantService.FACTORY_LINK);
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

    private ServiceStat getLuceneStat(String name) {
        Map<String, ServiceStat> hostStats = this.host
                .getServiceStats(this.host.getDocumentIndexServiceUri());
        ServiceStat st = hostStats.get(name);
        if (st == null) {
            return new ServiceStat();
        }
        return st;
    }

    @Test
    public void timeSnapshotQuery() throws Throwable {
        setUpHost();
        doTimeSnapshotTest(1, 3, false);
        doPaginatedTimeSnapshotTest(5, 3, false);
    }

    private void doTimeSnapshotTest(int serviceCount, int versionCount, boolean forceRemote)
            throws Throwable {

        String prefix = "testPrefix";
        List<URI> services = createQueryTargetServices(serviceCount);

        // after starting a service, update it's document version
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

        // now make sure expand and include all version works. Issue same query, but enable expand and include all versions
        queryTask.querySpec.options = EnumSet.of(QueryOption.EXPAND_CONTENT,
                QueryOption.INCLUDE_ALL_VERSIONS);
        u = this.host.createQueryTaskService(queryTask, forceRemote);
        // result for finished state will be
        //  Service 1   Version     documentLinks
        //               0       link?documentVersion=0
        //               1       link?documentVersion=1
        //               2       link?documentVersion=2
        //               3       link?documentVersion=3
        finishedTaskState = this.host.waitForQueryTaskCompletion(queryTask.querySpec,
                services.size(), versionCount, u, forceRemote, true);
        assertTrue(finishedTaskState.results.documentLinks.size() == 4);
        assertTrue(finishedTaskState.results.documents.size() == 4);

        // get the document version=1 documentUpdateTimeMicros
        long documentUpdatedTime = -1;
        long firstVersionDocumentUpdatedTime = -1;
        long secondVersionDocumentUpdatedTime = -1;
        for (Map.Entry<String, Object> entry : finishedTaskState.results.documents.entrySet()) {
            QueryValidationServiceState state = Utils.fromJson(entry.getValue(),
                    QueryValidationServiceState.class);
            if (state.documentVersion == 1) {
                firstVersionDocumentUpdatedTime = state.documentUpdateTimeMicros;
            } else if (state.documentVersion == 2) {
                secondVersionDocumentUpdatedTime = state.documentUpdateTimeMicros;
            }
        }

        // documentUpdatedTime asked will be in the mid of first and second version
        documentUpdatedTime = (firstVersionDocumentUpdatedTime + secondVersionDocumentUpdatedTime)
                / 2;

        // create a new query with documentUpdatedTime as a rangeClause
        query = Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, services.get(0).getPath())
                .addRangeClause(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS,
                        NumericRange.createLessThanOrEqualRange(documentUpdatedTime),
                        Occurance.MUST_OCCUR)
                .build();
        queryTask = QueryTask.Builder.create().setQuery(query).build();
        queryTask.querySpec.options = EnumSet.of(QueryOption.EXPAND_CONTENT,
                QueryOption.TIME_SNAPSHOT);
        queryTask.querySpec.timeSnapshotBoundaryMicros = documentUpdatedTime;
        u = this.host.createQueryTaskService(queryTask, forceRemote);
        finishedTaskState = this.host.waitForQueryTaskCompletion(queryTask.querySpec,
                services.size(), versionCount, u, forceRemote, true);

        QueryValidationServiceState finishedState = Utils.fromJson(
                finishedTaskState.results.documents
                        .get(finishedTaskState.results.documentLinks.get(0)),
                QueryValidationServiceState.class);
        assertTrue(finishedTaskState.results.documentLinks.size() == 1);
        assertTrue(finishedTaskState.results.documents.size() == 1);
        assertTrue(finishedState.documentVersion == 1);
        verifyNoPaginatedIndexSearchers();
        deleteServices(services);
    }

    private void doPaginatedTimeSnapshotTest(int count, int versionCount, boolean forceRemote) throws Throwable {
        String prefix = "testPrefix";

        List<URI> services = createQueryTargetServices(count);

        // after starting a service, update it's document version
        QueryValidationServiceState newState = new QueryValidationServiceState();
        newState.id = prefix + UUID.randomUUID().toString();
        putStateOnQueryTargetServices(services, versionCount, newState);

        // Mark a time to query for
        long timeSnapshotBoundaryMicros = Utils.getNowMicrosUtc();

        // Update the documents versionCount more times
        newState.id = prefix + UUID.randomUUID().toString();
        putStateOnQueryTargetServices(services, versionCount, newState);

        // create a new query with documentUpdatedTime as a rangeClause
        Query query = Query.Builder.create()
                .addKindFieldClause(QueryValidationServiceState.class)
                .build();
        QueryTask queryTask = QueryTask.Builder.create().setQuery(query).build();
        queryTask.querySpec.options = EnumSet.of(QueryOption.EXPAND_CONTENT,
                QueryOption.TIME_SNAPSHOT);
        queryTask.querySpec.timeSnapshotBoundaryMicros = timeSnapshotBoundaryMicros;
        // only one result for page ( expect 5 pages )
        queryTask.querySpec.resultLimit = 1;
        URI u = this.host.createQueryTaskService(queryTask, forceRemote);
        QueryTask finishedTaskState = this.host.waitForQueryTaskCompletion(queryTask.querySpec,
                services.size(), versionCount, u, forceRemote, false);

        assertTrue(finishedTaskState.results.nextPageLink != null);

        TestRequestSender sender = this.host.getTestRequestSender();

        int totalPages = 0;
        while (finishedTaskState.results.nextPageLink != null) {
            totalPages++;
            finishedTaskState = sender.sendGetAndWait(
                    UriUtils.buildUri(this.host, finishedTaskState.results.nextPageLink), QueryTask.class);
            assertTrue(finishedTaskState.results.documentLinks != null);
            assertTrue(finishedTaskState.results.documentLinks.size() == 1);
            assertTrue(finishedTaskState.results.documents.size() == 1);
            String documentLink = finishedTaskState.results.documentLinks.get(0);
            QueryValidationServiceState state =
                    Utils.fromJson(finishedTaskState.results.documents.get(documentLink), QueryValidationServiceState.class);

            assertTrue(state.documentUpdateTimeMicros < timeSnapshotBoundaryMicros);
            // Should match versionCount which is the version latest before timeSnapshotBoundaryMicros
            assertTrue(state.documentVersion == versionCount);
        }
        assertTrue(totalPages == count);

        deleteServices(services);
    }

    @Test
    public void expandContentBinaryResult() throws Throwable {
        setUpHost();
        Long counter = 10L;
        this.host.doFactoryChildServiceStart(null,
                this.serviceCount,
                ExampleServiceState.class, (o) -> {
                    ExampleServiceState initialState = new ExampleServiceState();
                    initialState.name = UUID.randomUUID().toString();
                    initialState.counter = counter;
                    o.setBody(initialState);
                },
                UriUtils.buildFactoryUri(this.host, ExampleService.class));

        Query kindClause = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();

        QueryTask task = QueryTask.Builder.createDirectTask()
                .setQuery(kindClause)
                .addOption(QueryOption.EXPAND_BINARY_CONTENT)
                .build();

        if (task.documentExpirationTimeMicros != 0) {
            // the value was set as an interval by the calling test. Make absolute here so
            // account for service creation above
            task.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(
                    +task.documentExpirationTimeMicros);
        }

        this.host.logThroughput();

        this.host.createQueryTaskService(
                UriUtils.buildUri(this.host, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS), task, false,
                task.taskInfo.isDirect, task, null);

        // verify expand from direct query task
        ServiceDocumentQueryResult taskResult = task.results;
        assertNotNull(taskResult.documents);
        assertEquals(this.serviceCount, taskResult.documents.size());
        Object queryResult = taskResult.documents.get(taskResult.documentLinks.get(0));
        validateBinaryExampleServiceState(queryResult, taskResult.documentLinks.get(0), counter);

        // verify expand from factory
        URI factoryURI = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        factoryURI = UriUtils.buildExpandLinksQueryUri(factoryURI);
        ServiceDocumentQueryResult factoryGetResult = this.host.getFactoryState(factoryURI);
        assertNotNull(factoryGetResult.documents);
        assertEquals(this.serviceCount, factoryGetResult.documents.size());
        Object factoryResult = taskResult.documents.get(taskResult.documentLinks.get(0));
        validateBinaryExampleServiceState(factoryResult, taskResult.documentLinks.get(0), counter);
    }

    private void validateBinaryExampleServiceState(Object queryResult, String link, Long counter) {
        ServiceDocument serviceDocument = Utils.fromQueryBinaryDocument(link, queryResult);
        assertNotNull(serviceDocument);
        ExampleServiceState resultState = (ExampleServiceState) serviceDocument;
        assertEquals(counter, resultState.counter);
        assertEquals(Action.POST.toString(), resultState.documentUpdateAction);
        assertEquals(Utils.buildKind(ExampleServiceState.class), resultState.documentKind);
        assertNotNull(resultState.name);
    }

    @Test
    public void expandContentBinaryResultNegative() throws Throwable {
        setUpHost();
        Query kindClause = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();

        QueryTask task = QueryTask.Builder.createDirectTask()
                .setQuery(kindClause)
                .addOption(QueryOption.EXPAND_CONTENT)
                .addOption(QueryOption.EXPAND_BINARY_CONTENT)
                .build();

        try {
            this.host.createQueryTaskService(
                    UriUtils.buildUri(this.host, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS), task,
                    false, task.taskInfo.isDirect, task, null);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("EXPAND_BINARY_CONTENT is not compatible with OWNER_SELECTION / "
                    + "EXPAND_BUILTIN_CONTENT_ONLY / EXPAND_CONTENT"));
        }

        try {
            this.host.createQueryTaskService(task, true,
                    task.taskInfo.isDirect, task, null);
        } catch (Throwable e) {
            assertTrue(e.getMessage().contains("EXPAND_BINARY_CONTENT is not allowed for remote clients."));
        }
    }

    @Test
    public void indexSearcherReuseBasedOnDocumentKind() throws Throwable {
        setUpHost();

        String searcherUpdateStatName = LuceneDocumentIndexService.STAT_NAME_SEARCHER_UPDATE_COUNT
                + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;

        String searcherReuseStatName = LuceneDocumentIndexService.STAT_NAME_SEARCHER_REUSE_BY_DOCUMENT_KIND_COUNT
                + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;

        // Get the index searcher update count before making a query to the factory
        ServiceStat searcherUpdateCountBefore = getLuceneStat(searcherUpdateStatName);

        URI exampleFactoryURI = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK);
        URI userFactoryURI = UriUtils.buildUri(this.host, UserService.FACTORY_LINK);

        List<URI> exampleServices = new ArrayList<>();
        List<URI> userServices = new ArrayList<>();
        createExampleServices(exampleFactoryURI, exampleServices);

        // Issue a multiple documentKind query
        Query kindClause = Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_KIND,
                        Utils.buildKind(ExampleServiceState.class), MatchType.TERM, Occurance.SHOULD_OCCUR)
                .addFieldClause(ServiceDocument.FIELD_NAME_KIND,
                        Utils.buildKind(UserService.UserState.class), MatchType.TERM, Occurance.SHOULD_OCCUR)
                .build();

        QuerySpecification qs = new QuerySpecification();
        qs.query = kindClause;
        qs.resultLimit = 100;

        this.host.createAndWaitSimpleDirectQuery(qs, this.serviceCount, this.serviceCount);

        // Get the index searcher update count after the query to the factory
        ServiceStat searcherUpdateCountAfter = getLuceneStat(searcherUpdateStatName);
        // Index searcher is updated because documents of same 'kind' were modified.
        assertTrue(searcherUpdateCountAfter.latestValue > searcherUpdateCountBefore.latestValue);

        // Create a set of users
        ServiceStat searcherReuseCountBefore = getLuceneStat(searcherReuseStatName);
        createUserServices(userFactoryURI, userServices);

        // Query for example services again for a few times ( no updates were done to user documents )
        this.host.waitFor("Lucene IndexSearchers were not reused for documentKind queries", () -> {
            this.host.createAndWaitSimpleDirectQuery(qs, this.serviceCount, 2 * this.serviceCount);
            ServiceStat searcherReuseCountAfter = getLuceneStat(searcherReuseStatName);

            // Verify that the index searcher has been reused at least once.
            return searcherReuseCountAfter.latestValue > searcherReuseCountBefore.latestValue;
        });
    }

    public static class ServiceWithConflictingFieldName extends StatefulService {

        public static class State extends ServiceDocument {
            @PropertyOptions(indexing = PropertyIndexingOption.SORT)
            public String stringValue;

            @PropertyOptions(indexing = { PropertyIndexingOption.EXPAND,
                    PropertyIndexingOption.FIXED_ITEM_NAME, PropertyIndexingOption.SORT })
            public Map<String, String> mapValue;
        }

        public ServiceWithConflictingFieldName() {
            super(State.class);
            toggleOption(ServiceOption.PERSISTENCE, true);
        }
    }

    @Test
    public void testServiceWithConflictingSortedFieldName() throws Throwable {
        setUpHost();

        // Issue a query for documents which don't exist.
        Query query = Query.Builder.create()
                .addFieldClause(QueryValidationServiceState.FIELD_NAME_STRING_VALUE,
                        "a special string value")
                .addKindFieldClause(ServiceWithConflictingFieldName.State.class)
                .build();
        QueryTask queryTask = QueryTask.Builder.createDirectTask()
                .orderAscending(QueryValidationServiceState.FIELD_NAME_STRING_VALUE,
                        TypeName.STRING)
                .setQuery(query).build();
        this.host.createQueryTaskService(queryTask, false, true, queryTask, null);
        assertEquals(0, (long) queryTask.results.documentCount);

        // Now create some documents of the expected type, re-issue the same query once again, and
        // verify that the newly-created documents are sorted correctly in the results
        ServiceWithConflictingFieldName.State initialState1 = new ServiceWithConflictingFieldName.State();
        initialState1.stringValue = "a special string value";
        initialState1.mapValue = new HashMap<>();
        initialState1.mapValue.put("a special key", "a special value");
        initialState1.mapValue.put("another special key", "another special value");
        this.host.doThroughputServiceStart(this.serviceCount, ServiceWithConflictingFieldName.class,
                initialState1, null, null);

        queryTask = QueryTask.Builder.createDirectTask()
                .orderAscending(QueryValidationServiceState.FIELD_NAME_STRING_VALUE,
                        TypeName.STRING)
                .setQuery(query).build();
        this.host.createQueryTaskService(queryTask, false, true, queryTask, null);
        assertEquals(this.serviceCount, (long) queryTask.results.documentCount);

        // Now create some documents with the expected field name but the wrong indexing type and
        // re-issue the same query.
        List<URI> services = createQueryTargetServices(this.serviceCount);
        QueryValidationServiceState initialState = new QueryValidationServiceState();
        initialState.stringValue = "a special string value";
        putSimpleStateOnQueryTargetServices(services, initialState);

        queryTask = QueryTask.Builder.createDirectTask()
                .orderAscending(QueryValidationServiceState.FIELD_NAME_STRING_VALUE,
                        TypeName.STRING)
                .setQuery(query).build();
        this.host.createQueryTaskService(queryTask, false, true, queryTask, null);
        assertEquals(this.serviceCount, (long) queryTask.results.documentCount);
    }

    private void createUserServices(URI userFactorURI, List<URI> userServices)
            throws Throwable {

        TestContext ctx = this.host.testCreate(this.serviceCount);
        for (int i = 0; i < this.serviceCount; i++) {
            UserService.UserState s = new UserService.UserState();
            s.email = UUID.randomUUID().toString() + "@example.org";

            userServices.add(UriUtils.buildUri(this.host.getUri(),
                    ExampleService.FACTORY_LINK, s.documentSelfLink));
            this.host.send(Operation.createPost(userFactorURI)
                    .setBody(s)
                    .setCompletion(ctx.getCompletion()));
        }
        this.host.testWait(ctx);
    }
}
