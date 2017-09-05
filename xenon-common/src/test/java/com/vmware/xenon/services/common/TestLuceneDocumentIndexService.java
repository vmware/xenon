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

import static java.util.stream.Collectors.toList;
import static javax.xml.bind.DatatypeConverter.printBase64Binary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;

import javax.xml.bind.DatatypeConverter;

import org.apache.lucene.search.IndexSearcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.AuthorizationSetupHelper;
import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.FileUtils;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationContext;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ProcessingStage;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceConfigUpdateRequest;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.TypeName;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.SynchronizationTaskService;
import com.vmware.xenon.common.TestResults;
import com.vmware.xenon.common.TestUtilityService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestProperty;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.TestRequestSender.FailureResponse;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleImmutableService;
import com.vmware.xenon.services.common.ExampleService.ExampleODLService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.LuceneDocumentIndexService.BackupResponse;
import com.vmware.xenon.services.common.LuceneDocumentIndexService.CommitInfo;
import com.vmware.xenon.services.common.LuceneDocumentIndexService.PaginatedSearcherInfo;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Occurance;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;

class FaultInjectionLuceneDocumentIndexService extends LuceneDocumentIndexService {

    public void forceClosePaginatedSearchers() {

        logInfo("Closing all paginated searchers (%d)",
                this.paginatedSearchersByCreationTime.size());

        for (PaginatedSearcherInfo info : this.paginatedSearchersByCreationTime.values()) {
            try {
                IndexSearcher s = info.searcher;
                s.getIndexReader().close();
                this.searcherUpdateTimesMicros.remove(s.hashCode());
            } catch (Throwable ignored) {
            }
        }
    }

    public Map<Long, List<PaginatedSearcherInfo>> verifyPaginatedSearcherListsEqual() {

        logInfo("Verifying paginated searcher lists are equal");

        Map<Long, List<PaginatedSearcherInfo>> searchersByExpirationTime = new HashMap<>();
        long searcherCount = 0;

        synchronized (this.searchSync) {
            for (Entry<Long, List<PaginatedSearcherInfo>> entry :
                    this.paginatedSearchersByExpirationTime.entrySet()) {
                List<PaginatedSearcherInfo> expirationList = entry.getValue();
                for (PaginatedSearcherInfo info : expirationList) {
                    assertTrue(this.paginatedSearchersByCreationTime.containsValue(info));
                }
                searchersByExpirationTime.put(entry.getKey(), new ArrayList<>(expirationList));
                searcherCount += expirationList.size();
            }

            assertEquals(this.paginatedSearchersByCreationTime.size(), searcherCount);
        }

        return searchersByExpirationTime;
    }

    /*
     * Called by test code to abruptly close the index writer simulating spurious
     * self close of the index writer, in production environments, due to out of memory
     * or other recoverable failures
     */
    public void closeWriter() {
        try {
            this.logWarning("Closing writer abruptly to induce failure");
            int permits = QUERY_THREAD_COUNT + UPDATE_THREAD_COUNT;
            this.writerSync.acquire(permits);
            super.writer.commit();
            super.writer.close();
            this.writerSync.release(permits);
        } catch (Throwable e) {
        }
    }

    public ExecutorService setQueryExecutorService(ExecutorService es) {
        ExecutorService existing = this.privateQueryExecutor;
        this.privateQueryExecutor = es;
        return existing;
    }

    public ExecutorService setIndexingExecutorService(ExecutorService es) {
        ExecutorService existing = this.privateIndexingExecutor;
        this.privateIndexingExecutor = es;
        return existing;
    }
}

public class TestLuceneDocumentIndexService {

    public static class InMemoryExampleService extends ExampleService {
        public static final String FACTORY_LINK = "test/in-memory-examples";

        public InMemoryExampleService() {
            super();
            super.setDocumentIndexPath(ServiceUriPaths.CORE_IN_MEMORY_DOCUMENT_INDEX);
        }

        public static FactoryService createFactory() {
            return FactoryService.create(InMemoryExampleService.class);
        }
    }

    public static class IndexedMetadataExampleService extends ExampleService {
        public static final String FACTORY_LINK = "/test/indexed-metadata-examples";

        public static FactoryService createFactory() {
            return FactoryService.create(IndexedMetadataExampleService.class);
        }

        @Override
        public ServiceDocument getDocumentTemplate() {
            ServiceDocument template = super.getDocumentTemplate();
            template.documentDescription.documentIndexingOptions = EnumSet.of(
                    ServiceDocumentDescription.DocumentIndexingOption.INDEX_METADATA);
            return template;
        }
    }

    /**
     * Parameter that specifies number of durable service instances to create
     */
    public long serviceCount = 10;

    /**
     * Parameter that specifies number of concurrent update requests
     */
    public int updateCount = 10;

    /**
     * Parameter that specifies query interleaving factor
     */
    public int updatesPerQuery = 10;

    /**
     * Parameter that specifies required number of document in index before
     * tests start
     */
    public int documentCountAtStart = 10;

    /**
     * Parameter that specifies iterations per top level test method
     */
    public int iterationCount = 1;

    /**
     * Parameter that specifies service cache clear
     */
    public long serviceCacheClearIntervalSeconds = 0;

    /**
     * Parameter that specifies authorized user count for auth enabled tests
     */
    public int authUserCount = Utils.DEFAULT_THREAD_COUNT;

    /**
     * Parameter that specifies long running test duration in seconds
     */
    public long testDurationSeconds;

    /**
     * Parameter that specifies the version retention limit for {@link MinimalTestService}
     * documents.
     */
    public Long retentionLimit = MinimalTestService.DEFAULT_VERSION_RETENTION_LIMIT;

    /**
     * Parameter that specifies the version retention floor for {@link MinimalTestService}
     * documents.
     */
    public Long retentionFloor = MinimalTestService.DEFAULT_VERSION_RETENTION_FLOOR;

    /**
     * Parameter that specifies whether instrumentation is enabled for the
     * {@link LuceneDocumentIndexService}.
     */
    public boolean enableInstrumentation = false;

    /**
     * Parameter that specifies the document expiration time (delta) in
     * {@link #throughputPostWithExpirationLongRunning}.
     */
    public Integer expirationSeconds = null;

    private final String EXAMPLES_BODIES_FILE = "example_bodies.json";
    private final String INDEX_DIR_NAME = "lucene510";

    private FaultInjectionLuceneDocumentIndexService indexService;

    private VerificationHost host;

    @Rule
    public TestResults testResults = new TestResults();

    private String indexLink = ServiceUriPaths.CORE_DOCUMENT_INDEX;

    private URI inMemoryIndexServiceUri;

    private void setUpHost(boolean isAuthEnabled) throws Throwable {
        if (this.host != null) {
            return;
        }

        this.host = VerificationHost.create(0);
        CommandLineArgumentParser.parseFromProperties(this.host);
        CommandLineArgumentParser.parseFromProperties(this);
        try {
            // disable synchronization so it does not interfere with the various test assumptions
            // on index stats.
            this.host.setPeerSynchronizationEnabled(false);
            this.indexService = new FaultInjectionLuceneDocumentIndexService();
            InMemoryLuceneDocumentIndexService inMemoryIndexService = new InMemoryLuceneDocumentIndexService();
            if (this.host.isStressTest()) {
                this.indexService.toggleOption(ServiceOption.INSTRUMENTATION,
                        this.enableInstrumentation);
                this.host.setStressTest(this.host.isStressTest);
                this.host.setMaintenanceIntervalMicros(
                        ServiceHostState.DEFAULT_MAINTENANCE_INTERVAL_MICROS);
                Utils.setTimeDriftThreshold(TimeUnit.SECONDS.toMicros(120));
                LuceneDocumentIndexService.setImplicitQueryResultLimit(10000000);
            } else {
                this.indexService.toggleOption(ServiceOption.INSTRUMENTATION, true);
                this.host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS
                        .toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
                inMemoryIndexService.toggleOption(ServiceOption.INSTRUMENTATION, true);
            }

            this.host.setDocumentIndexingService(this.indexService);
            this.host.setPeerSynchronizationEnabled(false);

            if (isAuthEnabled) {
                this.host.setAuthorizationService(new AuthorizationContextService());
                this.host.setAuthorizationEnabled(true);
            }

            if (this.serviceCacheClearIntervalSeconds != 0) {
                this.host.setServiceCacheClearDelayMicros(
                        TimeUnit.SECONDS.toMicros(this.serviceCacheClearIntervalSeconds));
            }

            this.host.addPrivilegedService(InMemoryLuceneDocumentIndexService.class);

            this.host.start();

            this.host.setSystemAuthorizationContext();

            // start in-memory index service AFTER host has started, so that it can get correct service uri
            this.host.startServiceAndWait(inMemoryIndexService, InMemoryLuceneDocumentIndexService.SELF_LINK, null);
            this.inMemoryIndexServiceUri = inMemoryIndexService.getUri();
            createInMemoryExampleServiceFactory(this.host);

            this.host.resetAuthorizationContext();

            if (isAuthEnabled) {
                createUsersAndRoles();
            }

        } catch (Throwable e) {
            throw new Exception(e);
        }
    }

    @Before
    public void setUp() {
        // re-initialize
        this.indexLink = ServiceUriPaths.CORE_DOCUMENT_INDEX;
    }

    @After
    public void tearDown() throws Exception {
        Utils.setTimeDriftThreshold(Utils.DEFAULT_TIME_DRIFT_THRESHOLD_MICROS);
        if (this.host == null) {
            return;
        }
        try {
            this.host.setSystemAuthorizationContext();
            this.host.logServiceStats(this.host.getDocumentIndexServiceUri(), this.testResults);
            this.host.logServiceStats(this.inMemoryIndexServiceUri, this.testResults);
        } catch (Throwable e) {
            this.host.log("Error logging stats: %s", e.toString());
        }
        this.host.tearDown();
        this.host = null;
        LuceneDocumentIndexService.setIndexFileCountThresholdForWriterRefresh(
                LuceneDocumentIndexService.DEFAULT_INDEX_FILE_COUNT_THRESHOLD_FOR_WRITER_REFRESH);
        LuceneDocumentIndexService.setExpiredDocumentSearchThreshold(
                LuceneDocumentIndexService.DEFAULT_EXPIRED_DOCUMENT_SEARCH_THRESHOLD);
    }

    @Test
    public void initialStateCorrectness() throws Throwable {
        setUpHost(false);
        List<MinimalTestService> services = new ArrayList<>();
        TestContext ctx = this.host.testCreate(this.serviceCount);
        for (int i = 0; i < this.serviceCount; i++) {
            MinimalTestService s = new MinimalTestService();
            s.toggleOption(ServiceOption.PERSISTENCE, true);
            MinimalTestServiceState initialState = new MinimalTestServiceState();
            initialState.id = UUID.randomUUID().toString();
            Operation post = Operation.createPost(this.host, Utils.getNowMicrosUtc() + "")
                    .setBody(initialState)
                    .setCompletion(ctx.getCompletion());
            this.host.startService(post, s);
            services.add(s);
        }
        this.host.testWait(ctx);
        for (MinimalTestService s : services) {
            assertTrue(!s.isStateModifiedPostCompletion);
        }
    }

    @Test
    public void documentGetStats() throws Throwable {
        setUpHost(false);
        List<String> docLinks = new ArrayList<String>();
        MinimalFactoryTestService f = new MinimalFactoryTestService();
        MinimalFactoryTestService factoryService = (MinimalFactoryTestService) this.host
                .startServiceAndWait(f, UUID.randomUUID().toString(), null);
        factoryService.setChildServiceCaps(
                EnumSet.of(ServiceOption.PERSISTENCE));
        for (int i = 0; i < this.serviceCount; i++) {
            MinimalTestServiceState initialState = new MinimalTestServiceState();
            initialState.documentSelfLink = initialState.id = UUID.randomUUID().toString();
            this.host.sendAndWaitExpectSuccess(
                    Operation.createPost(factoryService.getUri()).setBody(initialState));
            docLinks.add(initialState.documentSelfLink);
        }
        for (String selfLink: docLinks) {
            this.host.sendAndWaitExpectSuccess(Operation.createGet(this.host,
                    UriUtils.buildUriPath(factoryService.getUri().getPath(), selfLink)));
        }

        Map<String, ServiceStat> stats = this.host.getServiceStats(this.host.getDocumentIndexServiceUri());
        String statKey = String.format(
                LuceneDocumentIndexService.STAT_NAME_SINGLE_QUERY_BY_FACTORY_COUNT_FORMAT,
                factoryService.getUri().getPath());
        ServiceStat kindStat = stats.get(statKey);
        assertNotNull(kindStat);
        assertEquals(this.serviceCount, kindStat.latestValue, 0);
    }

    @Test
    public void testQueueDepthStats() throws Throwable {
        setUpHost(false);

        MinimalFactoryTestService factoryService = (MinimalFactoryTestService)
                this.host.startServiceAndWait(new MinimalFactoryTestService(),
                        UUID.randomUUID().toString(), null);

        factoryService.setChildServiceCaps(EnumSet.of(ServiceOption.PERSISTENCE));

        this.host.doFactoryChildServiceStart(null, this.serviceCount, MinimalTestServiceState.class,
                (o) -> {
                    MinimalTestServiceState initialState = new MinimalTestServiceState();
                    initialState.documentSelfLink = initialState.id = UUID.randomUUID().toString();
                    o.setBody(initialState);
                }, factoryService.getUri());

        // Create an executor service which rejects all attempts to create new threads.
        ExecutorService blockingExecutor = Executors.newSingleThreadExecutor((r) -> null);
        ExecutorService queryExecutor = this.indexService.setQueryExecutorService(blockingExecutor);
        queryExecutor.shutdown();
        queryExecutor.awaitTermination(this.host.getTimeoutSeconds(), TimeUnit.SECONDS);

        // Now submit a query and wait for the queue depth stat to be set.
        QueryTask queryTask = QueryTask.Builder.create()
                .setQuery(Query.Builder.create()
                        .addKindFieldClause(MinimalTestServiceState.class)
                        .build())
                .build();

        this.host.createQueryTaskService(queryTask);

        this.host.waitFor("Query queue depth stat was not set", () -> {
            Map<String, ServiceStat> indexStats = this.host.getServiceStats(
                    this.host.getDocumentIndexServiceUri());
            ServiceStat queueDepthStat = indexStats.entrySet().stream()
                    .filter((entry) -> entry.getKey().startsWith(
                            LuceneDocumentIndexService.STAT_NAME_PREFIX_QUERY_QUEUE_DEPTH))
                    .filter((entry) -> entry.getKey().endsWith(
                            ServiceStats.STAT_NAME_SUFFIX_PER_DAY))
                    .map(Entry::getValue).findFirst().orElse(null);
            if (queueDepthStat == null) {
                return false;
            }

            assertTrue(queueDepthStat.name.contains(ServiceUriPaths.CORE_AUTHZ_GUEST_USER));
            return queueDepthStat.latestValue == 1;
        });
    }

    @Test
    public void corruptedIndexRecovery() throws Throwable {
        setUpHost(false);
        this.doDurableServiceUpdate(Action.PUT, MinimalTestService.class, 100, 2, null);
        Thread.sleep(this.host.getMaintenanceIntervalMicros() / 1000);

        // Stop the host, without cleaning up storage.
        this.host.stop();
        this.host.setPort(0);

        corruptLuceneIndexFiles();

        try {
            // Restart host with the same storage sandbox. If host does not throw, we are good.
            this.indexService.toggleOption(ServiceOption.INSTRUMENTATION, true);
            this.host.start();
        } catch (java.nio.file.AccessDeniedException e) {
            // see next catch {} clause for explanation
            return;
        } catch (org.apache.lucene.store.LockObtainFailedException e) {
            // The process of corrupting files (deleting them) or stopping the host and committing
            // the index, might leave the index lock file under use. The attempt to restart might
            // rarely timeout because the FS did not release lock in time
            return;
        } catch (IllegalStateException e) {
            // on occasion, the lock held exception is caught by the index service and it attempts
            // recovery, which swallows the exception but leads to host not starting
            if (e.getMessage().toLowerCase().contains("not started")) {
                return;
            } else {
                throw e;
            }
        }

        // now *prove* that the index retry code was invoke, by looking at stats
        URI luceneServiceStats = UriUtils.buildStatsUri(this.host,
                LuceneDocumentIndexService.SELF_LINK);
        ServiceStats stats = this.host
                .getServiceState(null, ServiceStats.class, luceneServiceStats);
        assertTrue(stats.entries.size() > 0);
        ServiceStat retryStat = stats.entries
                .get(LuceneDocumentIndexService.STAT_NAME_INDEX_LOAD_RETRY_COUNT);
        assertTrue(retryStat != null);
        assertTrue(retryStat.latestValue > 0);

        File storageSandbox = new File(this.host.getStorageSandbox());

        int total = Files
                .list(storageSandbox.toPath())
                .map((Path fileP) -> {
                    try {
                        if (!fileP.toString().contains(LuceneDocumentIndexService.FILE_PATH_LUCENE)) {
                            return 0;
                        }
                        if (fileP.toAbsolutePath().toString().contains(".")) {
                            assertTrue(fileP.toFile().list().length > 0);
                        }
                        FileUtils.deleteFiles(fileP.toFile());
                    } catch (Throwable e) {

                    }
                    return 1;
                }).reduce(0, Integer::sum);

        final int expectedDirectoryPathsWithLuceneInName = 2;
        assertEquals(expectedDirectoryPathsWithLuceneInName, total);
    }

    @Test
    public void corruptIndexWhileRunning() throws Throwable {
        setUpHost(false);
        this.host.setOperationTimeOutMicros(TimeUnit.SECONDS.toMicros(5));
        this.host.setServiceStateCaching(false);

        Map<URI, ExampleServiceState> exampleServices = this.host.doFactoryChildServiceStart(null,
                this.serviceCount, ExampleServiceState.class,
                (o) -> {
                    ExampleServiceState b = new ExampleServiceState();
                    b.name = Utils.getNowMicrosUtc() + " before stop";
                    o.setBody(b);
                }, UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));

        exampleServices = updateUriMapWithNewPort(this.host.getPort(), exampleServices);
        // make sure all services have started
        this.host.getServiceState(null, ExampleServiceState.class, exampleServices.keySet());

        // close the writer!
        this.indexService.closeWriter();

        // issue some updates, which at least some failing and expect the host to stay alive. There
        // is no guarantee at this point that future writes will succeed since the writer re-open
        // is asynchronous and happens on maintenance intervals
        updateServices(exampleServices, true);

        // now induce a failure we can NOT recover from
        corruptLuceneIndexFiles();

        // try to poke the services we created before we corrupted the index. Some if not all should
        // fail and we should also see the host self stop
        updateServices(exampleServices, true);

        Date exp = this.host.getTestExpiration();
        while (this.host.isStarted()) {

            if (new Date().after(exp)) {
                this.host
                        .log("Host never stopped after index corruption, but appears healthy, verifiying");
                updateServices(exampleServices, true);
                break;
            }
            Thread.sleep(TimeUnit.MICROSECONDS.toMillis(this.host.getMaintenanceIntervalMicros()));
        }

    }

    @Test
    public void forceExpirePaginatedSearchersWithoutRecovery() throws Throwable {
        setUpHost(false);
        TestRequestSender sender = this.host.getTestRequestSender();

        Map<URI, ExampleServiceState> exampleServices = this.host.doFactoryChildServiceStart(null,
                this.serviceCount, ExampleServiceState.class,
                (o) -> {
                    ExampleServiceState b = new ExampleServiceState();
                    b.name = Utils.getNowMicrosUtc() + " before stop";
                    o.setBody(b);
                }, UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));

        // create a paginated query that is bound to fail after we close the searchers
        // underneath it
        Query query = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();
        QueryTask queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.EXPAND_CONTENT)
                .setResultLimit(2)
                .setQuery(query).build();
        boolean isDirect = true;
        this.host.createQueryTaskService(queryTask, false, isDirect, queryTask, null);

        // fetch the first page, since the QueryPageService will only retry refresh, transparently,
        // if the first page has never been fetched and we want to avoid that: we want to see
        // the query page fail
        QueryTask firstPageResults = sender.sendGetAndWait(
                UriUtils.buildUri(this.host, queryTask.results.nextPageLink), QueryTask.class);
        assertTrue(!firstPageResults.results.documentLinks.isEmpty());
        assertTrue(firstPageResults.results.nextPageLink != null);

        // close the index searchers supporting paginated queries, then attempt to grab
        // a query page.
        this.indexService.forceClosePaginatedSearchers();

        Operation getSecondPage = Operation.createGet(this.host,
                firstPageResults.results.nextPageLink);
        FailureResponse rsp = sender.sendAndWaitFailure(getSecondPage);
        ServiceErrorResponse errorBody = rsp.op.getErrorResponseBody();
        assertTrue(errorBody.message.contains("IndexReader"));

        ServiceStat readerClosedStat = this.getLuceneStat(
                LuceneDocumentIndexService.STAT_NAME_READER_ALREADY_CLOSED_EXCEPTION_COUNT);
        assertEquals(1, readerClosedStat.version);

        // issue some updates, they should all succeed, the index should be healthy
        updateServices(exampleServices, false);

        // make sure index recovery did not run
        ServiceStat writerClosedStat = this.getLuceneStat(
                LuceneDocumentIndexService.STAT_NAME_WRITER_ALREADY_CLOSED_EXCEPTION_COUNT);
        assertEquals(0, writerClosedStat.version);
    }

    @Test
    public void testLuceneQueryConversion() throws Throwable {
        Query topLevelQuery = Query.Builder.create()
                .addFieldClause("name", "foo", Occurance.SHOULD_OCCUR)
                .addFieldClause("id", "foo-id", Occurance.SHOULD_OCCUR)
                .build();

        org.apache.lucene.search.Query luceneQuery = LuceneQueryConverter.convertToLuceneQuery(topLevelQuery, null);

        // Assert that the top level MUST_OCCUR is ignored ( old behavior )
        assertEquals(luceneQuery.toString(), "name:foo id:foo-id");

        luceneQuery = LuceneQueryConverter.convert(topLevelQuery, null);

        // Assert that the top level MUST_OCCUR is not ignored
        assertEquals(luceneQuery.toString(), "+(name:foo id:foo-id)");
    }

    @Test
    public void testPaginatedSearcherLists() throws Throwable {
        for (int i = 0; i < this.iterationCount; i++) {
            tearDown();
            setUpHost(false);
            doPaginatedSearcherLists();
        }
    }

    private void doPaginatedSearcherLists() throws Throwable {

        this.host.doFactoryChildServiceStart(null, this.serviceCount, ExampleServiceState.class,
                (o) -> {
                    ExampleServiceState b = new ExampleServiceState();
                    b.name = Utils.getNowMicrosUtc() + " before stop";
                    o.setBody(b);
                }, UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));

        // Assert that the paginated searcher lists in the index service have the same content
        // at the start (an empty list).
        Map<Long, List<PaginatedSearcherInfo>> paginatedSearchers =
                this.indexService.verifyPaginatedSearcherListsEqual();
        assertEquals(0, paginatedSearchers.size());

        long queryExpirationTimeMicros = Utils.fromNowMicrosUtc(TimeUnit.MINUTES.toMicros(10));

        // create a paginated query with an explicit expiration time
        Query query = Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, ExampleService.FACTORY_LINK, QueryTask.QueryTerm.MatchType.PREFIX)
                .build();
        QueryTask queryTask = QueryTask.Builder.create()
                .setQuery(query)
                .setResultLimit(2)
                .build();
        queryTask.documentExpirationTimeMicros = queryExpirationTimeMicros;
        this.host.createQueryTaskService(queryTask, false, true, queryTask, null);

        // create another paginated query with the same expiration time
        queryTask = QueryTask.Builder.create()
                .setQuery(query)
                .setResultLimit(2)
                .build();
        queryTask.documentExpirationTimeMicros = queryExpirationTimeMicros;
        this.host.createQueryTaskService(queryTask, false, true, queryTask, null);

        // Assert that the paginated searcher lists in the index service have the same content.
        paginatedSearchers = this.indexService.verifyPaginatedSearcherListsEqual();
        assertEquals(1, paginatedSearchers.size());
        for (Entry<Long, List<PaginatedSearcherInfo>> entry : paginatedSearchers.entrySet()) {
            assertEquals(queryExpirationTimeMicros, (long) entry.getKey());
            List<PaginatedSearcherInfo> expirationList = entry.getValue();
            assertEquals(2, expirationList.size());
            for (PaginatedSearcherInfo info : expirationList) {
                assertEquals(queryExpirationTimeMicros, info.expirationTimeMicros);
            }
        }

        // create a paginated query with DO_NOT_REFRESH and an extended expiration time.
        long extendedQueryExpirationTimeMicros = queryExpirationTimeMicros
                + TimeUnit.MINUTES.toMicros(5);
        queryTask = QueryTask.Builder.create()
                .setQuery(query)
                .setResultLimit(2)
                .addOption(QueryOption.DO_NOT_REFRESH)
                .build();
        queryTask.documentExpirationTimeMicros = extendedQueryExpirationTimeMicros;
        this.host.createQueryTaskService(queryTask, false, true, queryTask, null);

        // Assert that the paginated searcher lists in the index service have the same content and
        // that the expiration time of one searcher was updated.
        paginatedSearchers = this.indexService.verifyPaginatedSearcherListsEqual();
        assertEquals(2, paginatedSearchers.size());
        for (Entry<Long, List<PaginatedSearcherInfo>> entry : paginatedSearchers.entrySet()) {
            long expirationMicros = entry.getKey();
            assertTrue(expirationMicros == queryExpirationTimeMicros
                    || expirationMicros == extendedQueryExpirationTimeMicros);
            List<PaginatedSearcherInfo> expirationList = entry.getValue();
            assertEquals(1, expirationList.size());
            for (PaginatedSearcherInfo info : expirationList) {
                assertEquals(expirationMicros, info.expirationTimeMicros);
            }
        }

        // Create a new paginated searcher with a short expiration and wait for it to expire.
        queryTask = QueryTask.Builder.create()
                .setQuery(query)
                .setResultLimit(2)
                .build();
        queryTask.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(
                this.host.getMaintenanceIntervalMicros());
        this.host.createQueryTaskService(queryTask, false, true, queryTask, null);

        this.host.waitFor("Paginated query searcher failed to expire", () -> {
            Map<Long, List<PaginatedSearcherInfo>> searcherInfo =
                    this.indexService.verifyPaginatedSearcherListsEqual();
            if (searcherInfo.size() > 2) {
                return false;
            }

            assertEquals(2, searcherInfo.size());
            for (Entry<Long, List<PaginatedSearcherInfo>> entry : searcherInfo.entrySet()) {
                long expirationMicros = entry.getKey();
                assertTrue(expirationMicros == queryExpirationTimeMicros
                        || expirationMicros == extendedQueryExpirationTimeMicros);
                List<PaginatedSearcherInfo> expirationList = entry.getValue();
                assertEquals(1, expirationList.size());
                for (PaginatedSearcherInfo info : expirationList) {
                    assertEquals(expirationMicros, info.expirationTimeMicros);
                }
            }

            return true;
        });

        // verify that a new index searcher is picked up when DO_NOT_REFRESH is specified
        // and the query is issued one maintenance interval after the index has been updated
        this.host.doFactoryChildServiceStart(null, this.serviceCount, ExampleServiceState.class,
                (o) -> {
                    ExampleServiceState b = new ExampleServiceState();
                    b.name = UUID.randomUUID().toString();
                    o.setBody(b);
                }, UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
        // Check to see we will use an existing searcher by default
        queryTask = QueryTask.Builder.create()
                .setQuery(query)
                .setResultLimit(2)
                .addOption(QueryOption.DO_NOT_REFRESH)
                .build();
        queryTask.documentExpirationTimeMicros = extendedQueryExpirationTimeMicros;
        this.host.createQueryTaskService(queryTask, false, true, queryTask, null);
        assertEquals(2, this.indexService.paginatedSearchersByCreationTime.values().size());
        // next, set the searcher refresh interval to the maintenance interval and ensure
        // a new searcher is created
        LuceneDocumentIndexService.setSearcherRefreshIntervalMicros(
                ServiceHostState.DEFAULT_MAINTENANCE_INTERVAL_MICROS);
        this.host.waitFor("New index searcher was not created", () -> {
            QueryTask qTask = QueryTask.Builder.create()
                    .setQuery(query)
                    .setResultLimit(2)
                    .addOption(QueryOption.DO_NOT_REFRESH)
                    .build();
            qTask.documentExpirationTimeMicros = extendedQueryExpirationTimeMicros;
            this.host.createQueryTaskService(qTask, false, true, qTask, null);
            // we should have 3 index searchers after the searcher refresh interval has elapsed
            if (this.indexService.paginatedSearchersByCreationTime.values().size() >= 3) {
                return true;
            }
            return false;
        });
        LuceneDocumentIndexService.setSearcherRefreshIntervalMicros(null);
        // Create a new direct paginated searcher with a long expiration and the SINGLE_USE option,
        // traverse all of the results pages, and verify that the pages and the index searcher were
        // closed.
        queryTask = QueryTask.Builder.create()
                .setQuery(query)
                .setResultLimit(2)
                .addOption(QueryOption.SINGLE_USE)
                .build();
        queryTask.documentExpirationTimeMicros = extendedQueryExpirationTimeMicros;

        this.host.log("Creating a query task with SINGLE_USE with expiration time %d",
                queryTask.documentExpirationTimeMicros);

        this.host.createQueryTaskService(queryTask, false, true, queryTask, null);
        assertNotNull(queryTask.results.nextPageLink);

        ServiceStat forceDeletionStat = getLuceneStat(
                LuceneDocumentIndexService.STAT_NAME_PAGINATED_SEARCHER_FORCE_DELETION_COUNT
                        + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
        assertEquals(0.0, forceDeletionStat.latestValue, 0.01);

        paginatedSearchers = this.indexService.verifyPaginatedSearcherListsEqual();
        assertEquals(2, paginatedSearchers.size());
        for (Entry<Long, List<PaginatedSearcherInfo>> entry : paginatedSearchers.entrySet()) {
            long expirationMicros = entry.getKey();
            List<PaginatedSearcherInfo> expirationList = entry.getValue();
            if (expirationMicros == queryExpirationTimeMicros) {
                assertEquals(1, expirationList.size());
            } else if (expirationMicros == extendedQueryExpirationTimeMicros) {
                assertEquals(3, expirationList.size());
            } else {
                throw new IllegalStateException("Unexpected expiration time: " + expirationMicros);
            }
        }

        TestContext ctx = this.host.testCreate(1);
        List<String> pageLinks = traversePageLinks(ctx, queryTask.results.nextPageLink);
        this.host.testWait(ctx);

        // Verify that the query page services have been stopped. Note this should happen
        // synchronously during GET processing.
        TestContext getCtx = this.host.testCreate(pageLinks.size());
        AtomicInteger remaining = new AtomicInteger(pageLinks.size());
        for (String pageLink : pageLinks) {
            Operation get = Operation.createGet(this.host, pageLink).setCompletion((o, e) -> {
                if (e != null && (e instanceof ServiceHost.ServiceNotFoundException)) {
                    remaining.decrementAndGet();
                }
                getCtx.complete();
            });

            this.host.send(get);
        }
        this.host.testWait(getCtx);

        assertEquals(0, remaining.get());

        this.host.waitFor("Failed to delete index searcher (stat)", () -> {
            ServiceStat deletionStat = getLuceneStat(
                    LuceneDocumentIndexService.STAT_NAME_PAGINATED_SEARCHER_FORCE_DELETION_COUNT
                            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
            return deletionStat.latestValue == 1.0;
        });

        this.host.waitFor("Failed to delete index searcher (list)", () -> {
            Map<Long, List<PaginatedSearcherInfo>> searcherInfo =
                    this.indexService.verifyPaginatedSearcherListsEqual();
            assertEquals(2, searcherInfo.size());
            for (Entry<Long, List<PaginatedSearcherInfo>> entry : searcherInfo.entrySet()) {
                long expirationMicros = entry.getKey();
                List<PaginatedSearcherInfo> expirationList = entry.getValue();
                if (expirationMicros == queryExpirationTimeMicros) {
                    assertEquals(1, expirationList.size());
                } else if (expirationMicros != extendedQueryExpirationTimeMicros) {
                    throw new IllegalStateException("Unexpected expiration time: "
                            + expirationMicros);
                } else {
                    return expirationList.size() == 2;
                }
            }

            throw new IllegalStateException("Unreachable");
        });
    }

    private List<String> traversePageLinks(TestContext ctx, String nextPageLink) {
        List<String> nextPageLinks = new ArrayList<>();
        traversePageLinks(ctx, nextPageLink, nextPageLinks);
        return nextPageLinks;
    }

    private void traversePageLinks(TestContext ctx, String nextPageLink, List<String> nextPageLinks) {
        nextPageLinks.add(nextPageLink);
        Operation get = Operation.createGet(this.host, nextPageLink)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.fail(e);
                        return;
                    }

                    QueryTask page = o.getBody(QueryTask.class);
                    if (page.results.nextPageLink == null) {
                        ctx.complete();
                        return;
                    }

                    traversePageLinks(ctx, page.results.nextPageLink, nextPageLinks);
                });

        this.host.send(get);
    }

    private void updateServices(Map<URI, ExampleServiceState> exampleServices, boolean expectFailure)
            throws Throwable {
        AtomicInteger g = new AtomicInteger();
        Throwable[] failure = new Throwable[1];
        for (URI service : exampleServices.keySet()) {
            ExampleServiceState b = new ExampleServiceState();
            b.name = Utils.getNowMicrosUtc() + " after stop";
            this.host.send(Operation.createPut(service)
                    .forceRemote()
                    .setBody(b).setCompletion((o, e) -> {
                        if (expectFailure) {
                            g.incrementAndGet();
                            return;
                        }

                        if (e != null && !expectFailure) {
                            g.incrementAndGet();
                            failure[0] = e;
                            return;
                        }

                        g.incrementAndGet();
                    }));
        }

        // if host self stops some requests might not complete, so we use a counter and
        // a polling loop. If the counter reaches expected count, or there is failure or host
        // stops, we exit

        Date exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {
            if (failure[0] != null) {
                throw failure[0];
            }

            if (!this.host.isStarted()) {
                return;
            }

            if (g.get() == exampleServices.size()) {
                return;
            }
            this.host.log("Remaining: %d", g.get());
            Thread.sleep(250);
        }

        if (new Date().after(exp)) {
            throw new TimeoutException();
        }
    }

    private void corruptLuceneIndexFiles() throws IOException {
        // Corrupt lucene sandbox (delete one of the index files).
        File baseDir = new File(this.host.getStorageSandbox());
        File luceneDir = new File(baseDir, LuceneDocumentIndexService.FILE_PATH_LUCENE);

        // Delete writer lock file so new host can acquire it without having to wait for
        // the old host to clean it up asynchronously...
        try {
            Files.delete(new File(luceneDir, "write.lock").toPath());
        } catch (IOException e) {
            this.host.log(Level.WARNING, "Unable to delete writer.lock: %s", e.toString());
            return;
        }

        Files.list(luceneDir.toPath()).forEach((Path fileP) -> {
            String name = fileP.toString();
            this.host.log(name);
            if (!name.endsWith(".si") && !name.endsWith(".fdx")) {
                return;
            }
            try {
                Files.delete(fileP);
            } catch (Throwable e) {

            }
        });
    }

    @Test
    public void immutableServiceQueryLongRunning() throws Throwable {
        setUpHost(false);
        URI factoryUri = createImmutableFactoryService(this.host);
        doServiceQueryLongRunning(factoryUri, EnumSet.of(QueryOption.INCLUDE_ALL_VERSIONS));
    }

    @Test
    public void indexedMetadataServiceQueryLongRunning() throws Throwable {
        setUpHost(false);
        URI factoryUri = createIndexedMetadataFactoryService(this.host);
        doServiceQueryLongRunning(factoryUri, EnumSet.of(QueryOption.INDEXED_METADATA));
    }

    private void doServiceQueryLongRunning(URI factoryUri, EnumSet<QueryOption> queryOptions) {

        QueryTask.Query prefixQuery = QueryTask.Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, factoryUri.getPath(),
                        QueryTask.QueryTerm.MatchType.PREFIX)
                .build();

        ExampleServiceState state = new ExampleServiceState();
        state.counter = 123L;
        state.name = "name";

        Date testExpiration = this.host.getTestExpiration();
        do {

            state.documentExpirationTimeMicros = Utils.getSystemNowMicrosUtc()
                    + TimeUnit.HOURS.toMicros(1);

            this.host.testStart(this.serviceCount);
            for (int i = 0; i < this.serviceCount; i++) {
                this.host.send(Operation.createPost(factoryUri)
                        .setBody(state)
                        .setCompletion(this.host.getCompletion()));
            }
            this.host.testWait();

            QueryTask queryTask = QueryTask.Builder.createDirectTask()
                    .addOption(QueryOption.SORT)
                    .addOption(QueryOption.TOP_RESULTS)
                    .addOption(QueryOption.EXPAND_CONTENT)
                    .addOptions(queryOptions)
                    .orderDescending(ServiceDocument.FIELD_NAME_SELF_LINK,
                            ServiceDocumentDescription.TypeName.STRING)
                    .setResultLimit((int) this.serviceCount)
                    .setQuery(prefixQuery)
                    .build();

            this.host.createQueryTaskService(queryTask, false, true, queryTask, null);
            assertTrue(queryTask.results.documentLinks.size() == this.serviceCount);

        } while (this.testDurationSeconds > 0 && new Date().before(testExpiration));
    }

    @Test
    public void immutableServiceLifecycle() throws Throwable {
        setUpHost(false);
        URI factoryUri = createImmutableFactoryService(this.host);
        doThroughputPostWithNoQueryResults(false, factoryUri);
        ServiceDocumentQueryResult res = this.host.getFactoryState(factoryUri);
        assertEquals(this.serviceCount, res.documentLinks.size());

        TestContext ctx = this.host.testCreate(res.documentLinks.size());
        ctx.setTestName("DELETE").logBefore();
        for (String link : res.documentLinks) {
            Operation del = Operation.createDelete(this.host, link).setCompletion((o, e) -> {
                ctx.completeIteration();
            });
            this.host.send(del);
        }

        ctx.await();
        ctx.logAfter();

        // verify option validation
        MinimalFactoryTestService f = new MinimalFactoryTestService();
        MinimalFactoryTestService factoryService = (MinimalFactoryTestService) this.host
                .startServiceAndWait(f, UUID.randomUUID().toString(), null);
        factoryService.setChildServiceCaps(
                EnumSet.of(ServiceOption.PERSISTENCE, ServiceOption.IMMUTABLE));
        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = "id";
        Operation post = Operation.createPost(factoryService.getUri()).setBody(body);
        // should fail, missing ON_DEMAND_LOAD
        this.host.sendAndWaitExpectFailure(post);

        post = Operation.createPost(factoryService.getUri()).setBody(body);
        factoryService.setChildServiceCaps(
                EnumSet.of(ServiceOption.PERSISTENCE, ServiceOption.ON_DEMAND_LOAD,
                        ServiceOption.IMMUTABLE, ServiceOption.PERIODIC_MAINTENANCE));
        // should fail, has PERIODIC_MAINTENANCE
        this.host.sendAndWaitExpectFailure(post);

        post = Operation.createPost(factoryService.getUri()).setBody(body);
        factoryService.setChildServiceCaps(
                EnumSet.of(ServiceOption.PERSISTENCE, ServiceOption.ON_DEMAND_LOAD,
                        ServiceOption.IMMUTABLE, ServiceOption.INSTRUMENTATION));
        // should fail, has INSTRUMENTATION
        this.host.sendAndWaitExpectFailure(post);
    }

    @Test
    public void implicitQueryResultLimit() throws Throwable {
        try {
            LuceneDocumentIndexService.setImplicitQueryResultLimit((int) (this.serviceCount / 2));
            setUpHost(false);
            URI factoryUri = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK);
            doThroughputPost(false, factoryUri, null, null);
            // a GET to the factory is a query without a result limit explicitly set. Since we
            // created 2x the documents of the new, low, implicit result limit, we expect failure
            TestRequestSender sender = this.host.getTestRequestSender();
            sender.sendAndWaitFailure(Operation.createGet(factoryUri));
        } finally {
            LuceneDocumentIndexService.setImplicitQueryResultLimit(
                    LuceneDocumentIndexService.DEFAULT_QUERY_RESULT_LIMIT);
        }
    }

    @Test
    public void throughputSelfLinkQuery() throws Throwable {
        setUpHost(false);
        this.indexService.toggleOption(ServiceOption.INSTRUMENTATION, true);

        // Immutable factory first:
        // we perform two passes per factory: first pass creates new services and
        // does NOT interleave updates in between queries.
        boolean doPostOrUpdates = true;
        boolean interleaveUpdate = false;
        URI immutableFactoryUri = createImmutableFactoryService(this.host);
        doThroughputSelfLinkQuery(immutableFactoryUri, null, doPostOrUpdates, interleaveUpdate,
                null);
        // second pass does not create new services, but does do interleaving
        doPostOrUpdates = false;
        interleaveUpdate = true;
        doThroughputSelfLinkQuery(immutableFactoryUri, null, doPostOrUpdates, interleaveUpdate,
                null);

        // repeat for example factory (mutable)
        doPostOrUpdates = true;
        interleaveUpdate = false;
        URI exampleFactoryUri = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK);
        // notice that we also create K versions per link, for the mutable factory to
        // better quantify query processing throughput when multiple versions per link are
        // present in the index results
        doThroughputSelfLinkQuery(exampleFactoryUri, this.updateCount, doPostOrUpdates,
                interleaveUpdate, null);
        doPostOrUpdates = false;
        interleaveUpdate = true;
        doThroughputSelfLinkQuery(exampleFactoryUri, this.updateCount, doPostOrUpdates,
                interleaveUpdate, null);

        // repeat for example factory with indexted metadata
        URI indexedMetadataFactoryUri = createIndexedMetadataFactoryService(this.host);
        doThroughputSelfLinkQuery(indexedMetadataFactoryUri, this.updateCount, true, false,
                this.serviceCount * this.updateCount);
        doThroughputSelfLinkQuery(indexedMetadataFactoryUri, this.updateCount, false, true, null);

        // now for the in memory index and the example services associated with it
        doPostOrUpdates = true;
        interleaveUpdate = false;
        this.indexLink = ServiceUriPaths.CORE_IN_MEMORY_DOCUMENT_INDEX;
        URI inMemExampleFactoryUri = UriUtils.buildUri(this.host,
                InMemoryExampleService.FACTORY_LINK);
        // notice that we also create K versions per link, for the mutable factory to
        // better quantify query processing throughput when multiple versions per link are
        // present in the index results
        doThroughputSelfLinkQuery(inMemExampleFactoryUri, this.updateCount, doPostOrUpdates,
                interleaveUpdate, null);
        doPostOrUpdates = false;
        interleaveUpdate = true;
        doThroughputSelfLinkQuery(inMemExampleFactoryUri, this.updateCount, doPostOrUpdates,
                interleaveUpdate, null);
    }

    private void doThroughputSelfLinkQuery(URI factoryUri, Integer updateCount,
            boolean doPostOrUpdates, boolean interleaveUpdates, Long metadataUpdateCount)
            throws Throwable {
        if (doPostOrUpdates) {
            doThroughputPostWithNoQueryResults(false, factoryUri);
        }

        if (doPostOrUpdates && updateCount != null && updateCount > 0) {
            doUpdates(factoryUri, updateCount);
        }

        if (metadataUpdateCount != null) {
            this.host.waitFor("Metadata indexing failed to occur", () -> {
                Map<String, ServiceStat> indexStats = this.host.getServiceStats(
                        this.host.getDocumentIndexServiceUri());
                ServiceStat stat = indexStats.get(
                        LuceneDocumentIndexService.STAT_NAME_METADATA_INDEXING_UPDATE_COUNT
                                + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
                if (stat == null) {
                    return false;
                }
                if (stat.accumulatedValue > metadataUpdateCount) {
                    throw new IllegalStateException("Update count is " + stat.accumulatedValue);
                }
                return stat.accumulatedValue == metadataUpdateCount;
            });
        }

        double durationNanos = 0;
        long s = System.nanoTime();
        TestContext ctx = this.host.testCreate(this.iterationCount);
        int postCount = this.iterationCount / this.updatesPerQuery;
        if (postCount < 1) {
            postCount = 1;
        }

        int posts = 0;
        TestContext postCtx = this.host.testCreate(postCount);
        for (int i = 0; i < this.iterationCount; i++) {
            this.host.send(Operation.createGet(factoryUri).setCompletion(ctx.getCompletion()));
            if (posts < postCount && interleaveUpdates) {
                if (postCount > 1 && i % this.updatesPerQuery != 0) {
                    continue;
                }
                ExampleServiceState st = new ExampleServiceState();
                st.name = "a-" + i;
                this.host.send(Operation.createPost(factoryUri)
                        .setBody(st)
                        .setCompletion(postCtx.getCompletion()));
                posts++;
            }
        }
        this.host.testWait(ctx);
        if (interleaveUpdates) {
            this.host.testWait(postCtx);
        }

        long e = System.nanoTime();
        durationNanos += e - s;
        double thput = this.serviceCount * this.iterationCount;
        if (updateCount != null) {
            // self link processing has to filter out old versions, so include that overhead. We
            // add one to account for the initial version
            thput *= (updateCount + 1);
        }
        thput /= durationNanos;
        thput *= TimeUnit.SECONDS.toNanos(1);

        double qps = this.iterationCount / durationNanos;
        qps *= TimeUnit.SECONDS.toNanos(1);

        this.host.log(
                "Interleave: %s, Factory:%s, Results per query:%d, Queries: %d, QPS: %f, Thpt (links/sec): %f",
                interleaveUpdates,
                factoryUri.getPath(),
                this.serviceCount,
                this.iterationCount,
                qps,
                thput);
        String m = String.format("GET %s links/sec thpt (upd: %s) ",
                factoryUri.getPath(), interleaveUpdates);
        this.testResults.getReport().all(m, thput);
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
    }

    private void doUpdates(URI factoryUri, Integer updateCount) {
        factoryUri = UriUtils.extendUriWithQuery(factoryUri,
                UriUtils.URI_PARAM_ODATA_TOP,
                this.serviceCount * 10 + "");
        ServiceDocumentQueryResult res = this.host.getFactoryState(factoryUri);
        assertEquals(this.serviceCount, (long) res.documentCount);
        TestContext ctx = this.host.testCreate(this.serviceCount * updateCount);
        ctx.setTestName("PATCH").logBefore();
        for (String link : res.documentLinks) {
            for (int i = 0; i < updateCount; i++) {
                ExampleServiceState st = new ExampleServiceState();
                st.name = i + "";
                Operation patch = Operation.createPatch(this.host, link).setBody(st)
                        .setCompletion(ctx.getCompletion());
                this.host.send(patch);
            }
        }
        this.host.testWait(ctx);
        ctx.logAfter();
    }

    @Test
    public void serviceHostRestartWithDurableServices() throws Throwable {
        for (int i = 0; i < this.iterationCount; i++) {
            doServiceHostRestartWithDurableServices();
            tearDown();
        }
    }

    private void doServiceHostRestartWithDurableServices() throws Throwable {
        setUpHost(false);
        VerificationHost h = VerificationHost.create();
        TemporaryFolder tmpFolder = new TemporaryFolder();
        tmpFolder.create();
        try {
            if (this.host.isStressTest()) {
                this.host.setOperationTimeOutMicros(TimeUnit.MINUTES.toMicros(5));
            }

            ServiceHost.Arguments args = new ServiceHost.Arguments();
            args.port = 0;
            args.sandbox = tmpFolder.getRoot().toPath();
            h.initialize(args);
            h.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(250));
            h.setOperationTimeOutMicros(this.host.getOperationTimeoutMicros());
            h.start();
            this.host.toggleServiceOptions(h.getDocumentIndexServiceUri(),
                    EnumSet.of(ServiceOption.INSTRUMENTATION),
                    null);

            convertExampleFactoryToIdempotent(h);

            String onDemandFactoryLink = OnDemandLoadFactoryService.create(h);
            createOnDemandLoadServices(h, onDemandFactoryLink);

            Map<URI, ExampleServiceState> beforeState = verifyIdempotentServiceStartDeleteWithStats(h);
            List<URI> exampleURIs = new ArrayList<>(beforeState.keySet());

            verifyInitialStatePost(h);

            ServiceHostState initialState = h.getState();

            // stop the host, create new one
            logServiceStats(h);
            h.stop();

            h = VerificationHost.create();
            args.port = 0;
            h.initialize(args);

            if (!this.host.isStressTest()) {
                h.setServiceStateCaching(false);
                h.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(250));
            }

            if (!VerificationHost.restartStatefulHost(h, true)) {
                this.host.log("Failed restart of host, aborting");
                return;
            }

            this.host.toggleServiceOptions(h.getDocumentIndexServiceUri(),
                    EnumSet.of(ServiceOption.INSTRUMENTATION),
                    null);

            verifyIdempotentFactoryAfterHostRestart(h, initialState, exampleURIs, beforeState);

            verifyOnDemandLoad(h);

        } finally {
            logServiceStats(h);
            h.stop();
            tmpFolder.delete();
        }
    }

    private void logServiceStats(VerificationHost h) {
        try {
            this.host.logServiceStats(UriUtils.buildUri(h, LuceneDocumentIndexService.SELF_LINK), this.testResults);
        } catch (Throwable e) {
            this.host.log("Error logging stats: %s", e.toString());
        }
    }

    private void convertExampleFactoryToIdempotent(VerificationHost h) {
        URI exampleFactoryUri = UriUtils.buildUri(h, ExampleService.FACTORY_LINK);
        h.waitForServiceAvailable(exampleFactoryUri);
        this.host.toggleServiceOptions(exampleFactoryUri,
                EnumSet.of(ServiceOption.IDEMPOTENT_POST), null);
    }

    private void createOnDemandLoadServices(ServiceHost h, String factoryLink)
            throws Throwable {
        this.host.testStart(this.serviceCount);
        for (int i = 0; i < this.serviceCount; i++) {
            ExampleServiceState body = new ExampleServiceState();
            body.name = "prefix" + UUID.randomUUID().toString();
            Operation post = Operation.createPost(UriUtils.buildUri(h, factoryLink))
                    .setCompletion(this.host.getCompletion())
                    .setBody(body);
            this.host.send(post);

        }
        this.host.testWait();
    }

    private void verifyInitialStatePost(VerificationHost h) throws Throwable {
        URI factoryUri = createImmutableFactoryService(h);
        doThroughputPostWithNoQueryResults(false, factoryUri);
        ServiceDocumentQueryResult r = this.host.getFactoryState(factoryUri);
        assertEquals(this.serviceCount, (long) r.documentCount);
        TestContext ctx = h.testCreate(this.serviceCount);
        for (String link : r.documentLinks) {
            Operation get = Operation.createGet(h, link).setCompletion((o, e) -> {
                if (e != null) {
                    ctx.fail(e);
                    return;
                }
                ExampleServiceState rsp = o.getBody(ExampleServiceState.class);
                if (rsp.name == null) {
                    ctx.fail(new IllegalStateException("missing name field value"));
                    return;
                }
                ctx.complete();
            });
            h.send(get);
        }
        h.testWait(ctx);
    }

    private void verifyIdempotentFactoryAfterHostRestart(VerificationHost h,
            ServiceHostState initialState, List<URI> exampleURIs,
            Map<URI, ExampleServiceState> beforeState) throws Throwable {
        long start = System.nanoTime() / 1000;

        // make sure synchronization has run, so we can verify if synch produced index updates
        this.host.waitForReplicatedFactoryServiceAvailable(
                UriUtils.buildUri(h, ExampleService.FACTORY_LINK));

        URI indexStatsUris = UriUtils.buildStatsUri(h.getDocumentIndexServiceUri());
        ServiceStats afterRestartIndexStats = this.host.getServiceState(null,
                ServiceStats.class, indexStatsUris);

        String indexedFieldCountStatName = LuceneDocumentIndexService.STAT_NAME_INDEXED_FIELD_COUNT;

        ServiceStat afterRestartIndexedFieldCountStat = afterRestartIndexStats.entries
                .get(indexedFieldCountStatName);
        // estimate of fields per example and on demand load service state
        int fieldCountPerService = 13;
        if (afterRestartIndexedFieldCountStat != null) {
            double latest = afterRestartIndexedFieldCountStat.latestValue;
            // if we had re-indexed all state on restart, the field update count would be approximately
            // the number of example services times their field count. We require less than that to catch
            // re-indexing that might occur before instrumentation is enabled in the index service
            assertTrue(latest < (this.serviceCount * fieldCountPerService) / 2);
        }

        beforeState = updateUriMapWithNewPort(h.getPort(), beforeState);
        List<URI> updatedExampleUris = new ArrayList<>();
        for (URI u : exampleURIs) {
            updatedExampleUris.add(UriUtils.updateUriPort(u, h.getPort()));
        }
        exampleURIs = updatedExampleUris;

        ServiceHostState stateAfterRestart = h.getState();

        assertTrue(initialState.id.equals(stateAfterRestart.id));

        URI exampleFactoryUri = UriUtils.buildUri(h, ExampleService.FACTORY_LINK);
        URI exampleFactoryStatsUri = UriUtils.buildStatsUri(exampleFactoryUri);
        this.host.waitForServiceAvailable(exampleFactoryUri);

        this.host.toggleServiceOptions(exampleFactoryUri,
                EnumSet.of(ServiceOption.IDEMPOTENT_POST), null);

        String statName = Service.STAT_NAME_NODE_GROUP_CHANGE_MAINTENANCE_COUNT;
        this.host.waitFor("node group change stat missing", () -> {
            ServiceStats stats = this.host.getServiceState(null, ServiceStats.class,
                    exampleFactoryStatsUri);
            ServiceStat st = stats.entries.get(statName);
            if (st != null && st.latestValue >= 1) {
                return true;
            }
            return false;
        });

        long end = System.nanoTime() / 1000;

        this.host.log("Example Factory available %d micros after host start", end - start);

        verifyCreateStatCount(exampleURIs, 0.0);

        // make sure all services are there
        Map<URI, ExampleServiceState> afterState = this.host.getServiceState(null,
                ExampleServiceState.class, exampleURIs);

        assertTrue(afterState.size() == beforeState.size());
        ServiceDocumentDescription sdd = this.host.buildDescription(ExampleServiceState.class);

        for (Entry<URI, ExampleServiceState> e : beforeState.entrySet()) {
            ExampleServiceState before = e.getValue();
            ExampleServiceState after = afterState.get(e.getKey());
            assertTrue(before.documentUpdateAction != null);
            assertTrue(after.documentUpdateAction != null);
            assertTrue(after != null);
            assertTrue(ServiceDocument.equals(sdd, before, after));
            assertEquals(after.documentVersion, before.documentVersion);
        }

        ServiceDocumentQueryResult rsp = this.host.getFactoryState(UriUtils.buildUri(h,
                ExampleService.FACTORY_LINK));
        assertEquals(beforeState.size(), rsp.documentLinks.size());

        if (this.host.isStressTest()) {
            return;
        }

        ExampleServiceState bodyAfter = new ExampleServiceState();
        bodyAfter.name = UUID.randomUUID().toString();
        this.host.testStart(beforeState.size());
        // issue some updates to force creation of link update time entries
        for (URI u : beforeState.keySet()) {
            Operation put = Operation.createPut(u)
                    .setCompletion(this.host.getCompletion())
                    .setBody(bodyAfter);
            this.host.send(put);
        }
        this.host.testWait();

        verifyChildServiceCountByOptionQuery(h, afterState);

        int putCount = 2;
        // issue some additional updates, per service, to verify that having clear self link info entries is OK
        this.host.testStart(exampleURIs.size() * putCount);
        for (int i = 0; i < putCount; i++) {
            for (URI u : exampleURIs) {
                this.host.send(Operation.createPut(u).setBody(bodyAfter)
                        .setCompletion(this.host.getCompletion()));
            }
        }
        this.host.testWait();

        verifyFactoryStartedAndSynchronizedAfterNodeSynch(h, statName);
    }

    private Map<URI, ExampleServiceState> verifyIdempotentServiceStartDeleteWithStats(VerificationHost h) throws Throwable {
        int vc = 2;
        ExampleServiceState bodyBefore = new ExampleServiceState();
        bodyBefore.name = UUID.randomUUID().toString();

        // create example, IDEMPOTENT services
        List<URI> exampleURIs = this.host.createExampleServices(h, this.serviceCount, null);

        verifyCreateStatCount(exampleURIs, 1.0);

        TestContext ctx = this.host.testCreate(exampleURIs.size() * vc);
        for (int i = 0; i < vc; i++) {
            for (URI u : exampleURIs) {
                this.host.send(Operation.createPut(u).setBody(bodyBefore)
                        .setCompletion(ctx.getCompletion()));
            }
        }
        this.host.testWait(ctx);

        verifyDeleteRePost(h, exampleURIs);

        Map<URI, ExampleServiceState> beforeState = this.host.getServiceState(null,
                ExampleServiceState.class, exampleURIs);

        verifyChildServiceCountByOptionQuery(h, beforeState);
        return beforeState;
    }

    private void verifyFactoryStartedAndSynchronizedAfterNodeSynch(ExampleServiceHost h,
            String statName) throws Throwable {
        // start another instance of the example factory, verify that node synchronization maintenance
        // happened, even if it was started after the initial synch occurred
        Service factory = ExampleService.createFactory();
        factory.toggleOption(ServiceOption.INSTRUMENTATION, true);
        Operation post = Operation.createPost(
                UriUtils.buildUri(h, UUID.randomUUID().toString()))
                .setCompletion(this.host.getCompletion());
        this.host.testStart(1);
        h.startService(post, factory);
        this.host.testWait();

        URI newExampleFactoryStatsUri = UriUtils.buildStatsUri(factory.getUri());

        this.host.waitFor("node group change stat missing", () -> {
            ServiceStats stats = this.host.getServiceState(null, ServiceStats.class,
                    newExampleFactoryStatsUri);
            ServiceStat st = stats.entries.get(statName);
            if (st != null && st.latestValue >= 1) {
                return true;
            }
            return false;
        });

        this.host.waitForServiceAvailable(factory.getUri());

    }

    private void verifyCreateStatCount(List<URI> exampleURIs, double expectedStat) throws Throwable {
        // verify create method was called
        List<URI> exampleStatUris = new ArrayList<>();
        exampleURIs.forEach((u) -> {
            exampleStatUris.add(UriUtils.buildStatsUri(u));
        });

        Map<URI, ServiceStats> stats = this.host.getServiceState(null, ServiceStats.class,
                exampleStatUris);
        stats.values().forEach(
                (sts) -> {
                    ServiceStat st = sts.entries.get(Service.STAT_NAME_CREATE_COUNT);
                    if (st == null && expectedStat == 0.0) {
                        return;
                    }
                    if (st == null || st.latestValue != expectedStat) {
                        throw new IllegalStateException("Expected create stat count of "
                                + expectedStat);
                    }
                });
    }

    private void verifyDeleteRePost(ExampleServiceHost h, List<URI> exampleURIs)
            throws Throwable {
        ExampleServiceState body;
        // delete one of the services (with no body)
        URI deletedService = exampleURIs.remove(0);
        this.host.testStart(1);
        this.host.send(Operation.createDelete(deletedService)
                .setCompletion(this.host.getCompletion()));
        this.host.testWait();

        // delete another, with body, verifying that delete works either way
        deletedService = exampleURIs.remove(0);
        this.host.testStart(1);
        this.host.send(Operation.createDelete(deletedService)
                .setBody(new ServiceDocument())
                .setCompletion(this.host.getCompletion()));
        this.host.testWait();

        URI deletedUri = deletedService;
        // recreate the service we just deleted, it should fail
        this.host.testStart(1);
        body = new ExampleServiceState();
        body.name = UUID.randomUUID().toString();
        body.documentSelfLink = deletedUri.getPath().replace(ExampleService.FACTORY_LINK, "");
        URI factory = UriUtils.buildUri(h, ExampleService.FACTORY_LINK);
        this.host.send(Operation.createPost(factory)
                .setBody(body)
                .setCompletion(this.host.getExpectedFailureCompletion()));
        this.host.testWait();

        int count = Utils.DEFAULT_THREAD_COUNT;

        for (int i = 0; i < count; i++) {
            this.host.testStart(2);
            ExampleServiceState clonedBody = Utils.clone(body);
            this.host.send(Operation.createPost(factory)
                    .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)
                    .setBody(clonedBody)
                    .setCompletion(this.host.getCompletion()));

            // in parallel, attempt to POST it AGAIN, it should be converted to PUT
            this.host.send(Operation.createPost(factory)
                    .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)
                    .setBody(clonedBody)
                    .setCompletion(this.host.getCompletion()));
            this.host.testWait();
            // DELETE
            this.host.testStart(1);
            this.host.send(Operation.createDelete(deletedUri)
                    .setCompletion(this.host.getCompletion()));
            this.host.testWait();
        }
    }

    private void verifyOnDemandLoad(ServiceHost h) throws Throwable {
        this.host.log("ODL verification starting");
        // make sure on demand load does not have INSTRUMENTATION enabled since that
        // will prevent stop
        String onDemandFactoryLink = OnDemandLoadFactoryService.create(h);
        URI factoryUri = UriUtils.buildUri(h, onDemandFactoryLink);
        ServiceDocumentQueryResult rsp = this.host.getFactoryState(factoryUri);
        // verify that for every factory child reported by the index, through the GET (query), the service is NOT
        // started
        assertEquals(this.serviceCount, rsp.documentLinks.size());
        List<URI> childUris = new ArrayList<>();
        for (String childLink : rsp.documentLinks) {
            assertTrue(h.getServiceStage(childLink) == null);
            childUris.add(UriUtils.buildUri(h, childLink));
        }

        // explicitly trigger synchronization and verify on demand load services did NOT start
        this.host.log("Triggering synchronization to verify on demand load is not affected");
        h.scheduleNodeGroupChangeMaintenance(ServiceUriPaths.DEFAULT_NODE_SELECTOR);
        Thread.sleep(TimeUnit.MICROSECONDS.toMillis(h.getMaintenanceIntervalMicros()) * 2);
        for (String childLink : rsp.documentLinks) {
            assertTrue(h.getServiceStage(childLink) == null);
        }

        int startCount = MinimalTestService.HANDLE_START_COUNT.get();
        // attempt to on demand load a service that *never* existed
        Operation getToNowhere = Operation.createGet(new URI(childUris.get(0) + "random"))
                .setCompletion(
                        this.host.getExpectedFailureCompletion(Operation.STATUS_CODE_NOT_FOUND));
        this.host.sendAndWait(getToNowhere);

        // verify that no attempts to start service occurred
        assertTrue(startCount == MinimalTestService.HANDLE_START_COUNT.get());

        ExampleServiceState st = new ExampleServiceState();
        st.name = Utils.getNowMicrosUtc() + "";

        verifyOnDemandLoadDeleteOnUnknown(factoryUri);

        // delete some of the services, not using a body, emulation DELETE through expiration
        URI serviceToDelete = childUris.remove(0);
        Operation delete = Operation.createDelete(serviceToDelete)
                .setCompletion(this.host.getCompletion());
        this.host.sendAndWait(delete);

        verifyOnDemandLoadDeleteInterleaving(st, serviceToDelete);

        // verify that attempting to start a service, through factory POST, that was previously created,
        // but not yet loaded/started, fails, with ServiceAlreadyStarted exception
        int count = Math.min(100, childUris.size());
        this.host.testStart(count);
        final String prefix = "prefix";
        for (int i = 0; i < count; i++) {
            ExampleServiceState body = new ExampleServiceState();
            // use a link hint for a previously created service, guaranteeing a collision
            URI u = childUris.get(i);
            body.documentSelfLink = u.getPath();
            body.name = prefix + UUID.randomUUID().toString();
            Operation post = Operation.createPost(factoryUri)
                    .setCompletion(this.host.getExpectedFailureCompletion())
                    .setBody(body);
            this.host.send(post);
        }
        this.host.testWait();

        // issue a GET per child link, which should force the on-demand load to take place, implicitly
        Map<URI, ExampleServiceState> childStates = this.host.getServiceState(null,
                ExampleServiceState.class,
                childUris);

        for (ExampleServiceState s : childStates.values()) {
            assertTrue(s.name != null);
            assertTrue(s.name.startsWith(prefix));
        }

        // mark a service for expiration, a few seconds in the future
        serviceToDelete = childUris.remove(0);
        ExampleServiceState body = new ExampleServiceState();
        body.name = UUID.randomUUID().toString();
        body.documentExpirationTimeMicros = Utils
                .fromNowMicrosUtc(TimeUnit.SECONDS.toMicros(2));
        Operation patch = Operation.createPatch(serviceToDelete)
                .setBody(body)
                .setCompletion(this.host.getCompletion());
        this.host.sendAndWait(patch);

        // Lets try stop now, then delete, on a service that should be on demand loaded
        this.host.log("Stopping service before expiration: %s", serviceToDelete.getPath());
        Operation stopDelete = Operation.createDelete(serviceToDelete)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)
                .setCompletion(this.host.getCompletion());
        this.host.sendAndWait(stopDelete);

        // also do a regular delete, it should make no difference.
        Operation regularDelete = Operation.createDelete(serviceToDelete)
                .setCompletion((o, e) -> {
                    this.host.completeIteration();
                });
        this.host.sendAndWait(regularDelete);

        String path = serviceToDelete.getPath();
        this.host.waitFor("never stopped", () -> {
            return this.host.getServiceStage(path) == null;
        });

        h.setServiceCacheClearDelayMicros(TimeUnit.MILLISECONDS.toMicros(250));
        this.host.log("Waiting for on demand load services to stop, due to maintenance");
        // verify on demand load services have been stopped, after a few maintenance intervals
        this.host.waitFor("on demand loaded services never stopped", () -> {
            for (URI u : childUris) {
                ProcessingStage stg = h.getServiceStage(u.getPath());
                if (stg != null) {
                    this.host.log("%s %s", u.getPath(), stg);
                    return false;
                }
            }
            return true;
        });

        verifyOnDemandLoadWithPragmaQueueForService(factoryUri);
        this.host.log("ODL verification done");
    }

    private void verifyOnDemandLoadDeleteInterleaving(ExampleServiceState st, URI serviceToDelete) {
        Operation delete;
        for (int i = 0; i < 10; i++) {
            // attempt to use service we just deleted, we should get failure
            // do a PATCH, expect 404
            this.host.log("Doing patch on deleted, expect failure");
            Operation patch = Operation
                    .createPatch(serviceToDelete)
                    .setBody(st)
                    .setCompletion(
                            this.host
                                    .getExpectedFailureCompletion(Operation.STATUS_CODE_NOT_FOUND));
            this.host.sendAndWait(patch);

            // do a GET, expect 404
            this.host.log("Doing GET on deleted, expect failure");
            Operation get = Operation
                    .createGet(serviceToDelete)
                    .setCompletion(
                            this.host
                                    .getExpectedFailureCompletion(Operation.STATUS_CODE_NOT_FOUND));
            this.host.sendAndWait(get);

            // do a PUT, expect 404
            this.host.log("Doing PUT on deleted, expect failure");
            Operation put = Operation
                    .createPut(serviceToDelete)
                    .setBody(st)
                    .setCompletion(
                            this.host
                                    .getExpectedFailureCompletion(Operation.STATUS_CODE_NOT_FOUND));
            this.host.sendAndWait(put);

            // do a POST, expect 409
            this.host.log("Doing POST on deleted, expect conflict failure");
            Operation post = Operation.createPost(serviceToDelete)
                    .setCompletion(
                            this.host.getExpectedFailureCompletion(Operation.STATUS_CODE_CONFLICT));
            this.host.sendAndWait(post);

            // do a DELETE again, expect success
            delete = Operation.createDelete(serviceToDelete);
            this.host.sendAndWaitExpectSuccess(delete);
        }
    }

    private void verifyOnDemandLoadDeleteOnUnknown(URI factoryUri) {
        for (int i = 0; i < 10; i++) {
            // do a DELETE for a completely unknown service, expect 200.
            // The 200 status is to stay consistent with the behavior for
            // non-ODL services.
            String unknownServicePath = "unknown-" + this.host.nextUUID();
            URI unknownServiceUri = UriUtils.extendUri(factoryUri, unknownServicePath);
            Operation delete = Operation
                    .createDelete(unknownServiceUri);
            this.host.sendAndWaitExpectSuccess(delete);

            ExampleServiceState body = new ExampleServiceState();
            // now create the service, expect success
            body.name = this.host.nextUUID();
            body.documentSelfLink = unknownServicePath;
            Operation post = Operation.createPost(factoryUri)
                    .setBody(body);
            this.host.sendAndWaitExpectSuccess(post);

            // delete the "unknown" service
            delete = Operation
                    .createDelete(unknownServiceUri);
            this.host.sendAndWaitExpectSuccess(delete);
        }
    }

    void verifyOnDemandLoadWithPragmaQueueForService(URI factoryUri) throws Throwable {

        Operation get;
        Operation post;
        ExampleServiceState body;
        // verify request gets queued, for a ODL service, not YET created
        // attempt to on demand load a service that *never* existed
        body = new ExampleServiceState();
        body.documentSelfLink = this.host.nextUUID();
        body.name = "queue-for-avail-" + UUID.randomUUID().toString();
        URI yetToBeCreatedChildUri = UriUtils.extendUri(factoryUri, body.documentSelfLink);

        // in parallel issue a GET to the yet to be created service, with a PRAGMA telling the
        // runtime to queue the request, until service start
        long getCount = this.serviceCount;
        TestContext ctx = this.host.testCreate(getCount + 1);
        for (int gi = 0; gi < getCount; gi++) {
            get = Operation.createGet(yetToBeCreatedChildUri)
                    .setConnectionSharing(true)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            ctx.fail(e);
                            return;
                        }
                        this.host.log("(%d) GET rsp from %s", o.getId(), o.getUri().getPath());
                        ctx.complete();
                    })
                    .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY);
            this.host.log("(%d) sending GET to %s", get.getId(), get.getUri().getPath());
            this.host.send(get);
            if (gi == getCount / 2) {
                // now issue the POST to create the service, in parallel with most of the GETs
                post = Operation.createPost(factoryUri)
                        .setConnectionSharing(true)
                        .setCompletion((o, e) -> {
                            if (e != null) {
                                ctx.fail(e);
                                return;
                            }
                            this.host.log("POST for %s done", yetToBeCreatedChildUri);
                            ctx.complete();
                        })
                        .setBody(body);
                this.host.send(post);
            }
        }
        this.host.testWait(ctx);
    }

    private Map<URI, ExampleServiceState> updateUriMapWithNewPort(int port,
            Map<URI, ExampleServiceState> beforeState) {
        Map<URI, ExampleServiceState> updatedExampleMap = new HashMap<>();
        for (Entry<URI, ExampleServiceState> e : beforeState.entrySet()) {
            URI oldUri = e.getKey();
            URI newUri = UriUtils.updateUriPort(oldUri, port);
            updatedExampleMap.put(newUri, e.getValue());
        }
        beforeState = updatedExampleMap;
        return beforeState;
    }

    private void verifyChildServiceCountByOptionQuery(
            ExampleServiceHost h, Map<URI, ExampleServiceState> beforeState) throws Throwable {

        this.host.testStart(1);
        Operation get = Operation.createGet(h.getUri()).setCompletion(
                (o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    ServiceDocumentQueryResult r = o.getBody(ServiceDocumentQueryResult.class);
                    int count = 0;
                    for (String u : r.documentLinks) {
                        if (u.contains(ExampleService.FACTORY_LINK)
                                && !u.contains(SynchronizationTaskService.FACTORY_LINK)) {
                            count++;
                        }
                    }
                    if (count != beforeState.size()) {
                        this.host.failIteration(new IllegalStateException("Unexpected result:"
                                + Utils.toJsonHtml(r)));
                    } else {
                        this.host.completeIteration();
                    }
                });
        h.queryServiceUris(EnumSet.of(ServiceOption.FACTORY_ITEM), false, get);
        this.host.testWait();
    }

    @Test
    public void accessODLAfterRemovedByMemoryPressure() throws Throwable {

        LuceneDocumentIndexService indexService = new LuceneDocumentIndexService();
        this.host = VerificationHost.create(0);
        this.host.setDocumentIndexingService(indexService);
        this.host.start();

        this.host.startFactory(new ExampleODLService());
        this.host.waitForServiceAvailable(ExampleODLService.FACTORY_LINK);

        TestRequestSender sender = this.host.getTestRequestSender();

        List<Operation> posts = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = "foo-" + i;
            state.documentSelfLink = state.name;

            posts.add(Operation.createPost(this.host, ExampleODLService.FACTORY_LINK).setBody(state));
        }
        List<ExampleServiceState> states = sender.sendAndWait(posts, ExampleServiceState.class);

        // perform deletes mimicking ODL stop
        List<Operation> deletes = states.stream().map(state ->
                Operation.createDelete(this.host, state.documentSelfLink)
                        .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)
                        .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_FORWARDING)
        ).collect(toList());
        sender.sendAndWait(deletes);

        // memory pressure to update document update info. set limit=1 so that all entries will be updated.
        indexService.updateMapMemoryLimit = 1;
        indexService.applyMemoryLimitToDocumentUpdateInfo();

        // verify those removed ODL should be accessible
        List<Operation> gets = states.stream()
                .map(state -> Operation.createGet(this.host, state.documentSelfLink))
                .collect(toList());
        sender.sendAndWait(gets);

    }

    @Test
    public void queryImmutableDocsAfterDeletion() throws Throwable {

        LuceneDocumentIndexService indexService = new LuceneDocumentIndexService();
        indexService.toggleOption(ServiceOption.INSTRUMENTATION, true);
        this.host = VerificationHost.create(0);
        this.host.setDocumentIndexingService(indexService);
        this.host.start();

        LuceneDocumentIndexService.setExpiredDocumentSearchThreshold(10);

        this.host.startFactory(new ExampleImmutableService());
        this.host.waitForServiceAvailable(ExampleImmutableService.FACTORY_LINK);

        TestRequestSender sender = this.host.getTestRequestSender();

        // create a set of immutable service documents
        List<Operation> posts = new ArrayList<>();
        for (int i = 0; i < this.serviceCount; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = "foo-" + i;
            state.documentSelfLink = state.name;
            state.documentExpirationTimeMicros = Utils.getNowMicrosUtc();

            posts.add(Operation.createPost(this.host, ExampleImmutableService.FACTORY_LINK).setBody(state));
        }
        sender.sendAndWait(posts, ExampleServiceState.class);

        // wait for the services to expire and removed from the index
        this.host.waitFor("Timeout waiting for services to be deleted", () -> {
            Map<String, ServiceStat> statMap = this.host.getServiceStats(
                    this.host.getDocumentIndexServiceUri());
            ServiceStat maintExpiredCount = statMap
                    .get(LuceneDocumentIndexService.STAT_NAME_DOCUMENT_EXPIRATION_COUNT
                            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
            if (maintExpiredCount != null && maintExpiredCount.latestValue >= this.serviceCount) {
                return true;
            }
            return false;
        });

        QueryTask queryTask = QueryTask.Builder.createDirectTask()
                .setQuery(QueryTask.Query.Builder.create()
                        .addKindFieldClause(ExampleServiceState.class)
                        .build())
                .addOption(QueryOption.INCLUDE_ALL_VERSIONS)
                .setResultLimit(10)
                .build();

        // invoke a new paginated query to create a searcher
        this.host.waitFor("Timeout waiting for service to be deleted", () -> {
            QueryTask returnTask = sender.sendPostAndWait(
                    UriUtils.buildUri(this.host.getUri(), ServiceUriPaths.CORE_LOCAL_QUERY_TASKS),
                    queryTask, QueryTask.class);
            if (returnTask.results.nextPageLink == null) {
                return true;
            }
            return false;
        });

        // create a set of new instances
        posts = new ArrayList<>();
        for (int i = 0; i < this.serviceCount; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = "new-foo-" + i;
            state.documentSelfLink = state.name;
            posts.add(Operation.createPost(this.host, ExampleImmutableService.FACTORY_LINK).setBody(state));
        }
        sender.sendAndWait(posts, ExampleServiceState.class);
        // invoke another query; we should see the newly created documents
        this.host.waitFor("Timeout waiting for service to be queried", () -> {
            QueryTask returnTask = sender.sendPostAndWait(
                    UriUtils.buildUri(this.host.getUri(), ServiceUriPaths.CORE_LOCAL_QUERY_TASKS),
                    queryTask, QueryTask.class);
            if (returnTask.results.nextPageLink != null) {
                return true;
            }
            return false;
        });
    }

    @Test
    public void interleavedUpdatesWithQueries() throws Throwable {
        setUpHost(false);
        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        final String initialServiceNameValue = "initial-" + UUID.randomUUID().toString();
        URI factoryUri = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK);
        Consumer<Operation> setInitialStateBody = (o) -> {
            ExampleServiceState body = new ExampleServiceState();
            body.name = initialServiceNameValue;
            o.setBody(body);
        };

        Map<URI, ExampleServiceState> services = this.host.doFactoryChildServiceStart(
                null,
                this.serviceCount, ExampleServiceState.class,
                setInitialStateBody, factoryUri);

        // for the next N seconds issue GETs to the factory, which translates to a self link
        // prefix query, while at the same time issuing updates to the existing services and
        // creating new services. Verify that that the results from the query are always the
        // same
        int expSeconds = this.expirationSeconds == null ? 1 : this.expirationSeconds;
        long endTime = Utils.getSystemNowMicrosUtc() + TimeUnit.SECONDS.toMicros(expSeconds);
        Throwable[] failure = new Throwable[1];

        AtomicInteger inFlightRequests = new AtomicInteger();

        do {
            Operation getFactoryState = Operation
                    .createGet(
                            UriUtils.buildExpandLinksQueryUri(factoryUri))
                    .setCompletion(
                            (o, e) -> {
                                inFlightRequests.decrementAndGet();
                                if (e != null) {
                                    failure[0] = e;
                                    return;
                                }

                                ServiceDocumentQueryResult rsp = o
                                        .getBody(ServiceDocumentQueryResult.class);
                                if (rsp.documents.size() != services.size()) {
                                    failure[0] = new IllegalStateException(
                                            "wrong number of services:" + Utils.toJsonHtml(rsp));
                                    return;
                                }

                                for (Object body : rsp.documents.values()) {
                                    ExampleServiceState s = Utils.fromJson(body,
                                            ExampleServiceState.class);
                                    if (s.documentVersion < 1) {
                                        if (!s.documentUpdateAction.equals(Action.POST.toString())) {
                                            failure[0] = new IllegalStateException(
                                                    "documentUpdateAction not expected:"
                                                            + Utils.toJsonHtml(s));
                                            return;
                                        }
                                    } else {
                                        if (!s.documentUpdateAction.equals(Action.PATCH.toString())) {
                                            failure[0] = new IllegalStateException(
                                                    "documentUpdateAction not expected:"
                                                            + Utils.toJsonHtml(s));
                                            return;
                                        }
                                    }
                                    if (!initialServiceNameValue.equals(s.name)) {
                                        failure[0] = new IllegalStateException("unexpected state:"
                                                + Utils.toJsonHtml(s));
                                        return;
                                    }
                                }
                            });
            inFlightRequests.incrementAndGet();
            this.host.send(getFactoryState);

            if (failure[0] != null) {
                throw failure[0];
            }

            long i = 0;
            for (URI u : services.keySet()) {
                ExampleServiceState s = new ExampleServiceState();
                s.name = initialServiceNameValue;
                s.counter = i++;
                Operation patchState = Operation.createPatch(u).setBody(s)
                        .setCompletion((o, e) -> {
                            inFlightRequests.decrementAndGet();
                            if (e != null) {
                                failure[0] = e;
                            }
                        });
                inFlightRequests.incrementAndGet();
                this.host.send(patchState);

                if (failure[0] != null) {
                    throw failure[0];
                }
            }

            // we need a small sleep otherwise we will have millions of concurrent requests issued, even within the span of
            // a few seconds (and we will end up waiting for a while for all of them to complete)
            Thread.sleep(50);
        } while (endTime > Utils.getSystemNowMicrosUtc());

        Date exp = new Date(TimeUnit.MICROSECONDS.toMillis(Utils.getSystemNowMicrosUtc())
                + TimeUnit.SECONDS.toMillis(expSeconds));
        if (exp.before(this.host.getTestExpiration())) {
            exp = this.host.getTestExpiration();
        }
        while (inFlightRequests.get() > 0) {
            Thread.sleep(100);
            if (failure[0] != null) {
                throw failure[0];
            }
            if (new Date().after(exp)) {
                throw new TimeoutException("Requests never completed");
            }
        }
    }

    @Test
    public void updateAndQueryByVersion() throws Throwable {
        setUpHost(false);
        this.host.doExampleServiceUpdateAndQueryByVersion(this.host.getUri(),
                (int) this.serviceCount);
    }

    @Test
    public void patchLargeServiceState() throws Throwable {
        setUpHost(false);
        // create on demand load services
        String factoryLink = OnDemandLoadFactoryService.create(this.host);
        ServiceDocumentQueryResult res = this.host.getFactoryState(
                UriUtils.buildUri(this.host, factoryLink));

        ExampleServiceState patchBody = new ExampleServiceState();
        patchBody.name = UUID.randomUUID().toString();
        byte[] body = new byte[4096 * 5];
        for (int i = 0; i < 30; i++) {
            new Random().nextBytes(body);
            String v = DatatypeConverter.printBase64Binary(body);
            this.host.log("Adding key/value of length %d", v.length());
            patchBody.keyValues.put(UUID.randomUUID().toString(), v);
        }

        byte[] bufferNeededForBinaryState = new byte[OnDemandLoadService.MAX_STATE_SIZE];
        int byteCount = Utils.toBytes(patchBody, bufferNeededForBinaryState, 0);
        this.host.log("Expected binary serialized state size %d", byteCount);

        TestContext ctx = this.host.testCreate(res.documentLinks.size());
        for (String link : res.documentLinks) {
            Operation patch = Operation.createPatch(this.host, link)
                    .setBody(patchBody)
                    .setCompletion(ctx.getCompletion());
            this.host.send(patch);
        }
        this.host.testWait(ctx);
    }

    @Test
    public void throughputPost() throws Throwable {
        if (this.serviceCacheClearIntervalSeconds == 0) {
            // effectively disable ODL stop/start behavior while running throughput tests
            this.serviceCacheClearIntervalSeconds = TimeUnit.MICROSECONDS.toSeconds(
                    ServiceHostState.DEFAULT_OPERATION_TIMEOUT_MICROS);
        }
        setUpHost(false);

        // throughput test for immutable factory
        URI factoryUri = createImmutableFactoryService(this.host);
        prePopulateIndexWithServiceDocuments(factoryUri);
        verifyImmutableEagerServiceStop(factoryUri, this.documentCountAtStart);

        boolean interleaveQueries = true;
        long stVersion = doThroughputImmutablePost(0, interleaveQueries, factoryUri);
        interleaveQueries = false;
        doThroughputImmutablePost(stVersion, interleaveQueries, factoryUri);

        // similar test but with regular, mutable, example factory
        double initialStopCount = getHostStopCount();
        interleaveQueries = true;
        factoryUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        doMultipleIterationsThroughputPost(interleaveQueries, this.iterationCount, factoryUri);

        interleaveQueries = false;
        doMultipleIterationsThroughputPost(interleaveQueries, this.iterationCount, factoryUri);

        // similar test but with indexed metadata factory
        factoryUri = createIndexedMetadataFactoryService(this.host);
        interleaveQueries = true;
        doMultipleIterationsThroughputPost(interleaveQueries, this.iterationCount, factoryUri);

        interleaveQueries = false;
        doMultipleIterationsThroughputPost(interleaveQueries, this.iterationCount, factoryUri);

        double finalStopCount = getHostStopCount();
        assertTrue(initialStopCount == finalStopCount);
    }

    @Test
    public void throughputPostWithExpirationLongRunning() throws Throwable {
        if (this.serviceCacheClearIntervalSeconds == 0) {
            // effectively disable ODL stop/start behavior while running throughput tests
            this.serviceCacheClearIntervalSeconds = TimeUnit.MICROSECONDS.toSeconds(
                    ServiceHostState.DEFAULT_OPERATION_TIMEOUT_MICROS);
        }

        // This code path is designed to simulate POST and query throughput under heavy load,
        // processing queries which match many results.
        QueryTask queryTask = QueryTask.Builder.createDirectTask()
                .setQuery(QueryTask.Query.Builder.create()
                        .addKindFieldClause(ExampleServiceState.class)
                        .build())
                .build();

        setUpHost(false);

        // Set the document expiration limit to something low enough that document expiration will
        // be forced to run in batches.
        LuceneDocumentIndexService.setExpiredDocumentSearchThreshold(10);

        URI factoryUri = createImmutableFactoryService(this.host);
        this.indexService.toggleOption(ServiceOption.INSTRUMENTATION, true);
        this.host.log("Starting throughout POST, expiration: %d", this.expirationSeconds);

        Long expirationMicros = null;
        if (this.expirationSeconds != null) {
            expirationMicros = TimeUnit.SECONDS.toMicros(this.expirationSeconds);
        }

        long testExpiration = Utils.getSystemNowMicrosUtc()
                + TimeUnit.SECONDS.toMicros(this.testDurationSeconds);
        do {
            this.host.log("Starting POST test to %s, count:%d", factoryUri, this.serviceCount);
            doThroughputPost(true, factoryUri, expirationMicros, queryTask);
            Map<String, ServiceStat> stats = this.host.getServiceStats(
                    this.host.getDocumentIndexServiceUri());
            ServiceStat expiredDocumentStat = stats.get(
                    LuceneDocumentIndexService.STAT_NAME_DOCUMENT_EXPIRATION_COUNT
                            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
            if (expiredDocumentStat != null) {
                this.host.log("Expired documents: %f", expiredDocumentStat.latestValue);
            }

        } while (this.testDurationSeconds > 0 && Utils.getSystemNowMicrosUtc() < testExpiration);

        Map<String, ServiceStat> indexServiceStats = this.host.getServiceStats(
                this.host.getDocumentIndexServiceUri());
        logServiceStatHistogram(indexServiceStats,
                LuceneDocumentIndexService.STAT_NAME_COMMIT_DURATION_MICROS);
        logServiceStatHistogram(indexServiceStats,
                LuceneDocumentIndexService.STAT_NAME_MAINTENANCE_DOCUMENT_EXPIRATION_DURATION_MICROS);
        logServiceStatHistogram(indexServiceStats,
                LuceneDocumentIndexService.STAT_NAME_QUERY_DURATION_MICROS);
        logServiceStatHistogram(indexServiceStats,
                LuceneDocumentIndexService.STAT_NAME_RESULT_PROCESSING_DURATION_MICROS);
    }

    private void logServiceStatHistogram(Map<String, ServiceStat> serviceStats, String statName) {
        ServiceStat stat = serviceStats.get(statName + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR);
        if (stat == null) {
            return;
        }
        ServiceStats.ServiceStatLogHistogram logHistogram = stat.logHistogram;
        if (logHistogram == null) {
            return;
        }

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(String.format("Stat name: %s\n", statName));
        for (int i = 0; i < 15; i++) {
            stringBuilder.append(String.format("%15d: %8d (%.2f percent)\n",
                    (long) Math.pow(10.0, i),
                    logHistogram.bins[i],
                    logHistogram.bins[i] * 100.0 / stat.version));
        }

        this.host.log(stringBuilder.toString());
    }

    private long doThroughputImmutablePost(long statVersion, boolean interleaveQueries,
            URI immutableFactoryUri)
            throws Throwable {
        this.host.log("Starting throughput POST, query interleaving: %s", interleaveQueries);

        // the version cache stats are updated once per maintenance so we must make sure
        // they are updated at least once initially, and then at least once after the
        // test has run

        this.host.waitFor("stat did not update", () -> {
            ServiceStat st = getLuceneStat(
                    LuceneDocumentIndexService.STAT_NAME_VERSION_CACHE_ENTRY_COUNT);
            return st.version >= statVersion;
        });
        ServiceStat initialVersionStat = getLuceneStat(
                LuceneDocumentIndexService.STAT_NAME_VERSION_CACHE_ENTRY_COUNT);

        doMultipleIterationsThroughputPost(interleaveQueries, this.iterationCount,
                immutableFactoryUri);
        if (!this.enableInstrumentation && this.host.isStressTest()) {
            return initialVersionStat.version;
        }
        this.host.waitFor("stat did not update", () -> {
            ServiceStat st = getLuceneStat(
                    LuceneDocumentIndexService.STAT_NAME_VERSION_CACHE_ENTRY_COUNT);
            return st.version > initialVersionStat.version + 1;
        });
        ServiceStat afterVersionStat = getLuceneStat(
                LuceneDocumentIndexService.STAT_NAME_VERSION_CACHE_ENTRY_COUNT);

        if (afterVersionStat.latestValue > initialVersionStat.latestValue) {
            throw new IllegalStateException("Immutable services caused version cache increase");
        }

        return afterVersionStat.version;
    }

    void prePopulateIndexWithServiceDocuments(URI factoryUri) throws Throwable {
        if (this.documentCountAtStart == 0) {
            return;
        }
        this.host.log("Pre populating index with %d documents on %s", this.documentCountAtStart,
                factoryUri);
        long serviceCountCached = this.serviceCount;
        this.serviceCount = this.documentCountAtStart;
        doThroughputPostWithNoQueryResults(false, factoryUri);
        this.serviceCount = serviceCountCached;
    }

    void verifyImmutableEagerServiceStop(URI factoryUri, int expectedStopCount) {
        double initialStopCount = getHostODLStopCount();
        double initialMaintCount = getMaintCount();
        this.host.waitFor("eager ODL stop not seen", () -> {
            double maintCount = getMaintCount();
            if (maintCount <= initialMaintCount + 1) {
                return false;
            }
            double stopCount = getHostODLStopCount();
            this.host.log("Stop count: %f, initial: %f, maint count delta: %f",
                    stopCount,
                    initialStopCount,
                    maintCount - initialMaintCount);
            boolean allStopped = stopCount >= expectedStopCount;
            if (!allStopped) {
                return false;
            }

            if (maintCount > initialMaintCount + 20) {
                // service cache clear is essentially turned off, but IMMUTABLE services should stop
                // within a maintenance interval of start. We are being forgiving and allow for 20, but
                // either way if it does not happen before waitFor timeout, something is broken
                throw new IllegalStateException("Eager service stop took too long");
            }
            return true;
        });
        this.host.log("All services for %s stopped", factoryUri);
    }

    URI createImmutableFactoryService(VerificationHost h) throws Throwable {
        Service immutableFactory = ExampleImmutableService.createFactory();
        immutableFactory = h.startServiceAndWait(immutableFactory,
                "immutable-examples", null);

        URI factoryUri = immutableFactory.getUri();
        return factoryUri;
    }

    URI createIndexedMetadataFactoryService(VerificationHost h) throws Throwable {
        Service indexedMetadataFactory = IndexedMetadataExampleService.createFactory();
        indexedMetadataFactory = h.startServiceAndWait(indexedMetadataFactory,
                IndexedMetadataExampleService.FACTORY_LINK, null);
        return indexedMetadataFactory.getUri();
    }

    URI createInMemoryExampleServiceFactory(VerificationHost h) throws Throwable {
        Service exampleFactory = InMemoryExampleService.createFactory();
        exampleFactory = h.startServiceAndWait(exampleFactory,
                InMemoryExampleService.FACTORY_LINK, null);

        URI factoryUri = exampleFactory.getUri();
        return factoryUri;
    }

    private double getHostStopCount() {
        return getMgmtStat(ServiceHostManagementService.STAT_NAME_ODL_STOP_COUNT);
    }

    private double getHostODLStopCount() {
        return getMgmtStat(ServiceHostManagementService.STAT_NAME_ODL_STOP_COUNT);
    }

    private double getMaintCount() {
        return getMgmtStat(ServiceHostManagementService.STAT_NAME_SERVICE_HOST_MAINTENANCE_COUNT);
    }

    private ServiceStat getLuceneStat(String name) {
        URI indexUri = UriUtils.buildUri(this.host, this.indexLink);
        Map<String, ServiceStat> hostStats = this.host
                .getServiceStats(indexUri);
        ServiceStat st = hostStats.get(name);
        if (st == null) {
            return new ServiceStat();
        }
        return st;
    }

    private double getMgmtStat(String name) {
        Map<String, ServiceStat> hostStats = this.host.getServiceStats(
                UriUtils.buildUri(this.host, ServiceHostManagementService.SELF_LINK));
        ServiceStat st = hostStats.get(name);
        if (st == null) {
            return 0.0;
        }
        return st.latestValue;
    }

    private void doMultipleIterationsThroughputPost(boolean interleaveQueries, int iterationCount,
            URI factoryUri) throws Throwable {

        for (int ic = 0; ic < iterationCount; ic++) {
            this.host.log("(%d) Starting POST test to %s, count:%d",
                    ic, factoryUri, this.serviceCount);

            doThroughputPostWithNoQueryResults(interleaveQueries, factoryUri);
            this.host.deleteOrStopAllChildServices(factoryUri, true, true);
            logQuerySingleStat();
        }
    }

    @Test
    public void throughputPostWithAuthz() throws Throwable {
        setUpHost(true);
        URI factoryUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        // assume system identity so we can create roles
        this.host.setSystemAuthorizationContext();

        List<String> userLinks = new ArrayList<>();
        for (int i = 0; i < this.authUserCount; i++) {
            userLinks.add(buildExampleUserLink(i));
        }

        // first test throughput sequentially, no contention among users
        for (String userLink : userLinks) {
            // assume authorized user identity
            this.host.assumeIdentity(userLink);
            this.host.log("(%d) (%s), Starting sequential factory POST, count:%d",
                    0,
                    OperationContext.getAuthorizationContext().getClaims().getSubject(),
                    this.serviceCount);
            doThroughputPostWithNoQueryResults(false, factoryUri);
            this.host.deleteAllChildServices(factoryUri);
        }

        // now test service creation with contention across different authorized subjects
        TestContext ctx = this.host.testCreate(userLinks.size());
        Map<String, Runnable> postTasksPerSubject = new ConcurrentSkipListMap<>();
        Map<String, Long> durationPerSubject = new ConcurrentSkipListMap<>();
        for (String userLink : userLinks) {
            Runnable r = () -> {
                try {
                    // assume authorized user identity
                    this.host.assumeIdentity(userLink);
                    this.host.log("(%d) (%s), Starting Factory POST, count:%d",
                            0,
                            OperationContext.getAuthorizationContext().getClaims().getSubject(),
                            this.serviceCount);
                    long start = System.nanoTime();
                    doThroughputPostWithNoQueryResults(false, factoryUri);
                    long end = System.nanoTime();
                    durationPerSubject.put(userLink, end - start);
                    ctx.complete();
                } catch (Throwable e) {
                    ctx.fail(e);
                }
            };
            postTasksPerSubject.put(userLink, r);
        }

        // Confirm fairness: Start one task before the two others, and see if the average throughput
        // for the last N tasks is similar. The initial task has a head start with no content, so its
        // expected that it has a higher throughput
        Runnable firstTask = postTasksPerSubject.remove(userLinks.remove(0));
        this.host.run(firstTask);
        // add a fixed delay per 1000 services, to allow the queue / index service to see lots of concurrent
        // requests for one subject, before others start in parallel
        Thread.sleep(10 * (this.serviceCount / 1000));
        for (Runnable r : postTasksPerSubject.values()) {
            ForkJoinPool.commonPool().execute(r);
        }
        this.host.testWait(ctx);

        // Until we confirm correctness, do not assert on durations. this will be enabled soon
        for (Entry<String, Long> e : durationPerSubject.entrySet()) {
            this.host.log("Subject: %s, duration(micros): %d", e.getKey(), e.getValue() / 1000);
        }
    }

    private String buildExampleUserEmail(int i) {
        return "example-user-" + i + "@somewhere.com";
    }

    private String buildExampleUserLink(int i) {
        return UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHZ_USERS, "example-user-" + i);
    }

    private void createUsersAndRoles() {

        TestContext ctx = this.host.testCreate(this.authUserCount);
        AuthorizationSetupHelper.AuthSetupCompletion authCompletion = (ex) -> {
            if (ex == null) {
                ctx.completeIteration();
            } else {
                ctx.failIteration(ex);
            }
        };

        this.host.setSystemAuthorizationContext();
        for (int i = 0; i < this.authUserCount; i++) {
            AuthorizationSetupHelper.create()
                    .setHost(this.host)
                    .setUserEmail(buildExampleUserEmail(i))
                    .setUserPassword(buildExampleUserEmail(i))
                    .setUserSelfLink(buildExampleUserLink(i))
                    .setIsAdmin(false)
                    .setUpdateUserGroupForUser(true)
                    .setDocumentKind(Utils.buildKind(ExampleServiceState.class))
                    .setCompletion(authCompletion)
                    .start();
        }
        this.host.testWait(ctx);
        this.host.resetAuthorizationContext();
    }

    private void doThroughputPostWithNoQueryResults(boolean interleaveQueries, URI factoryUri)
            throws Throwable {
        // Create a query which will match no documents. This code path is designed to simulate
        // the cost of queries on the index without incurring the cost of processing results.
        QueryTask queryTask = QueryTask.Builder.createDirectTask()
                .setQuery(Query.Builder.create()
                        .addFieldClause(ExampleServiceState.FIELD_NAME_ID, "saffsdfs")
                        .build())
                .build();
        queryTask.indexLink = this.indexLink;
        doThroughputPost(interleaveQueries, factoryUri, null, queryTask);
    }

    private void doThroughputPost(boolean interleaveQueries, URI factoryUri, Long expirationMicros,
            QueryTask queryTask) throws Throwable {
        long startTimeMicros = System.nanoTime() / 1000;
        int queryCount = 0;
        AtomicLong queryResultCount = new AtomicLong();
        long totalQueryCount = this.serviceCount / this.updatesPerQuery;
        TestContext ctx = this.host.testCreate((int) this.serviceCount);
        TestContext queryCtx = this.host.testCreate(totalQueryCount);

        for (int i = 0; i < this.serviceCount; i++) {

            Operation createPost = Operation.createPost(factoryUri);
            ExampleServiceState body = new ExampleServiceState();
            body.name = i + "";
            body.id = i + "";
            body.counter = (long) i;
            if (expirationMicros != null) {
                body.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(expirationMicros);
            }

            createPost.setBody(body);

            // create a start service POST with an initial state
            createPost.setCompletion(ctx.getCompletion());
            this.host.send(createPost);
            if (!interleaveQueries) {
                continue;
            }

            if ((queryCount >= totalQueryCount) || i % this.updatesPerQuery != 0) {
                continue;
            }

            queryCount++;
            Operation createQuery = Operation.createPost(this.host,
                    ServiceUriPaths.CORE_LOCAL_QUERY_TASKS)
                    .setBody(queryTask)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            queryCtx.fail(e);
                            return;
                        }
                        QueryTask rsp = o.getBody(QueryTask.class);
                        queryResultCount.addAndGet(rsp.results.documentCount);
                        queryCtx.complete();
                    });

            this.host.send(createQuery);
        }
        this.host.testWait(ctx);
        if (interleaveQueries) {
            this.host.testWait(queryCtx);
        }
        long endTimeMicros = System.nanoTime() / 1000;
        double deltaSeconds = (endTimeMicros - startTimeMicros) / 1000000.0;
        double ioCount = this.serviceCount;
        double throughput = ioCount / deltaSeconds;
        String subject = "(none)";
        if (this.host.isAuthorizationEnabled()) {
            subject = OperationContext.getAuthorizationContext().getClaims().getSubject();
        }

        String timeSeriesStatName = LuceneDocumentIndexService.STAT_NAME_INDEXED_DOCUMENT_COUNT
                + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;
        double docCount = this.getLuceneStat(timeSeriesStatName).accumulatedValue;

        this.host.log(
                "(%s) Factory: %s, Services: %d Docs: %f, Ops: %f, Queries: %d, Per query results: %d, ops/sec: %f",
                subject,
                factoryUri.getPath(),
                this.host.getState().serviceCount,
                docCount,
                ioCount, queryCount, queryResultCount.get(), throughput);

        this.testResults.getReport().all("POSTs/sec", throughput);
    }

    @Test
    public void putWithFailureAndCacheValidation() throws Throwable {
        setUpHost(false);
        List<Service> services = this.host.doThroughputServiceStart(
                1, MinimalTestService.class, this.host.buildMinimalTestState(),
                EnumSet.of(Service.ServiceOption.PERSISTENCE), null);

        // Write state that exceeds the default serialization limit and observe exception
        URI uri = services.get(0).getUri();
        MinimalTestServiceState largeBody = new MinimalTestServiceState();
        Random r = new Random();
        byte[] data = new byte[ServiceDocumentDescription.DEFAULT_SERIALIZED_STATE_LIMIT * 2];
        r.nextBytes(data);
        largeBody.id = printBase64Binary(data);
        this.host.testStart(1);
        Operation put = Operation
                .createPut(uri)
                .setBody(largeBody)
                .setCompletion(
                        (o, e) -> {
                            if (e == null) {
                                this.host.failIteration(new IllegalStateException(
                                        "Request should have failed"));
                                return;
                            }
                            ServiceErrorResponse rsp = o.getErrorResponseBody();
                            if (!rsp.message.contains("size limit")) {
                                this.host.failIteration(new IllegalStateException(
                                        "Error message not expected"));
                                return;
                            }
                            this.host.completeIteration();
                        });
        this.host.send(put);
        this.host.testWait();

        this.host.doServiceUpdates(Action.PUT, 1,
                EnumSet.of(TestProperty.LARGE_PAYLOAD, TestProperty.FORCE_FAILURE),
                services);

        Map<URI, MinimalTestServiceState> states = this.host.getServiceState(null,
                MinimalTestServiceState.class, services);
        for (MinimalTestServiceState s : states.values()) {
            if (s.documentVersion > 0) {
                throw new IllegalStateException("version should have not incremented");
            }
        }

    }

    @Test
    public void forcedIndexUpdateDuplicateVersionRemoval() throws Throwable {
        setUpHost(false);
        URI factoryUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        long originalExpMicros = Utils.getSystemNowMicrosUtc() + TimeUnit.MINUTES.toMicros(1);
        Consumer<Operation> setBody = (o) -> {
            ExampleServiceState body = new ExampleServiceState();
            body.name = UUID.randomUUID().toString();
            body.documentExpirationTimeMicros = originalExpMicros;
            o.setBody(body);
        };

        long totalExpectedVersionCount = 3;
        // create N documents with expiration set to future time
        Map<URI, ExampleServiceState> services = this.host.doFactoryChildServiceStart(
                null,
                this.serviceCount, ExampleServiceState.class,
                setBody, factoryUri);

        // patch once, each service, to build up some version history
        TestContext ctx = this.host.testCreate(services.size());
        for (ExampleServiceState st : services.values()) {
            st.counter = 1L;
            Operation patch = Operation.createPatch(this.host, st.documentSelfLink)
                    .setBody(st)
                    .setCompletion(ctx.getCompletion());
            this.host.send(patch);
        }
        this.host.testWait(ctx);

        // now delete everything. We should now have 3 versions (0,1,2) per link
        this.host.deleteAllChildServices(factoryUri);

        // get all versions
        Query kindQuery = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();
        QueryTask qt = QueryTask.Builder
                .createDirectTask()
                .addOption(QueryOption.INCLUDE_ALL_VERSIONS)
                .addOption(QueryOption.EXPAND_CONTENT)
                .setQuery(kindQuery)
                .build();
        this.host.createQueryTaskService(qt, false, true, qt, null);
        this.host.log("Results before forced update: %s", Utils.toJsonHtml(qt.results));
        assertEquals(services.size() * totalExpectedVersionCount, (long) qt.results.documentCount);

        // now force a POST, which should create a new version history, per link,
        // and a single new version per link, at zero
        ctx = this.host.testCreate(services.size());
        for (ExampleServiceState st : services.values()) {
            st.counter = totalExpectedVersionCount + 1;
            st.documentVersion = 0L;
            Operation post = Operation.createPost(factoryUri)
                    .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)
                    .setBody(st)
                    .setCompletion(ctx.getCompletion());
            this.host.send(post);
        }
        this.host.testWait(ctx);

        qt.results = null;

        // query again, verify version history has been reset
        this.host.createQueryTaskService(qt, false, true, qt, null);
        this.host.log("Results after forced update: %s", Utils.toJsonHtml(qt.results));
        assertEquals(services.size(), (long) qt.results.documentCount);

        ServiceStat forcedDeleteCountSt = this.getLuceneStat(
                LuceneDocumentIndexService.STAT_NAME_FORCED_UPDATE_DOCUMENT_DELETE_COUNT);
        assertTrue(forcedDeleteCountSt.latestValue >= services.size());
    }

    /**
     * Tests the following edge scenario:
     * User creates document with expiration set in the future (version 0)
     * User deletes document, before expiration (version 1)
     * User uses same self link, recreates the document, sets new expiration,
     * uses PRAGMA_FORCE_INDEX_UPDATE
     *
     * @throws Throwable
     */
    @Test
    public void deleteWithExpirationAndPostWithPragmaForceUpdate() throws Throwable {
        setUpHost(false);
        URI factoryUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        long originalExpMicros = Utils.getSystemNowMicrosUtc() + TimeUnit.MINUTES.toMicros(1);
        Consumer<Operation> setBody = (o) -> {
            ExampleServiceState body = new ExampleServiceState();
            body.name = UUID.randomUUID().toString();
            body.documentExpirationTimeMicros = originalExpMicros;
            o.setBody(body);
        };

        // create N documents with expiration set to future time
        Map<URI, ExampleServiceState> services = this.host.doFactoryChildServiceStart(
                null,
                this.serviceCount, ExampleServiceState.class,
                setBody, factoryUri);

        // delete all documents
        this.host.deleteAllChildServices(factoryUri);

        // get all versions
        QueryTask qt = QueryTask.Builder
                .createDirectTask()
                .addOption(QueryOption.INCLUDE_ALL_VERSIONS)
                .addOption(QueryOption.EXPAND_CONTENT)
                .setQuery(
                        Query.Builder.create().addKindFieldClause(ExampleServiceState.class)
                                .build()).build();
        this.host.createQueryTaskService(qt, false, true, qt, null);
        this.host.log("Results before expiration: %s", Utils.toJsonHtml(qt.results));

        // verify we have a version = 0 per document, with original expiration
        // verify we have a version = 1 per document, with updateAction = DELETE
        Map<Long, Set<String>> linksPerVersion = new HashMap<>();
        for (long l = 0; l < 2; l++) {
            linksPerVersion.put(l, new HashSet<>());
        }
        for (String linkWithVersion : qt.results.documentLinks) {
            URI u = UriUtils.buildUri(this.host, linkWithVersion);
            Map<String, String> params = UriUtils.parseUriQueryParams(u);
            String documentVersion = params.get(ServiceDocument.FIELD_NAME_VERSION);
            long v = Long.parseLong(documentVersion);
            ExampleServiceState stForVersion = Utils.fromJson(
                    qt.results.documents.get(linkWithVersion),
                    ExampleServiceState.class);
            if (v == 0) {
                assertEquals(originalExpMicros, stForVersion.documentExpirationTimeMicros);
            } else if (v == 1) {
                assertEquals(Action.DELETE.toString(), stForVersion.documentUpdateAction);
            }
            Set<String> linksForVersion = linksPerVersion.get(v);
            linksForVersion.add(u.getPath());
        }

        for (Set<String> links : linksPerVersion.values()) {
            assertEquals(this.serviceCount, links.size());
        }

        // recreate the documents, use same self links, set for short expiration in future,
        long recreateExpMicros = Utils.getSystemNowMicrosUtc()
                + this.host.getMaintenanceIntervalMicros();
        Iterator<URI> uris = services.keySet().iterator();
        Consumer<Operation> setBodyReUseLinks = (o) -> {
            o.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE);
            ExampleServiceState body = new ExampleServiceState();
            body.documentSelfLink = uris.next().getPath();
            body.name = UUID.randomUUID().toString();
            body.documentExpirationTimeMicros = recreateExpMicros;
            o.setBody(body);
        };
        services = this.host.doFactoryChildServiceStart(
                null,
                this.serviceCount, ExampleServiceState.class,
                setBodyReUseLinks, factoryUri);
        for (ExampleServiceState st : services.values()) {
            this.host.waitFor("document not expired", () -> {
                ProcessingStage stage = this.host.getServiceStage(st.documentSelfLink);
                if (stage == null) {
                    return true;
                }
                this.host.log("link: %s with stage: %s", st.documentSelfLink, stage.toString());
                return false;
            });
        }
        boolean forceIndexUpdate = false;
        for (ExampleServiceState st : services.values()) {
            st.documentExpirationTimeMicros = 0;
            st.documentVersion = 0L;
            Operation post = Operation.createPost(factoryUri)
                    .setBody(st);
            if (forceIndexUpdate) {
                post.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE);
            }
            this.host.getTestRequestSender().sendAndWait(post);
            //post or force index update in alternative
            forceIndexUpdate = !forceIndexUpdate;
        }
        URI factoryExpandedUri = UriUtils.buildExpandLinksQueryUri(factoryUri);
        Operation get = Operation.createGet(factoryExpandedUri);
        ServiceDocumentQueryResult result =
                this.host.getTestRequestSender().sendAndWait(get, ServiceDocumentQueryResult.class);
        assertEquals(services.size(), (long) result.documentCount);
        for (Object d : result.documents.values()) {
            ExampleServiceState state = Utils.fromJson(d, ExampleServiceState.class);
            assertEquals(0, state.documentVersion);
        }
    }

    @Test
    public void serviceCreationAndDocumentExpirationLongRunning() throws Throwable {
        setUpHost(false);
        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);

        long threshold = this.host.isLongDurationTest() ? this.serviceCount : 2;
        LuceneDocumentIndexService.setExpiredDocumentSearchThreshold((int) threshold);

        Date expiration = this.host.getTestExpiration();

        long opTimeoutMicros = this.host.testDurationSeconds != 0 ? this.host
                .getOperationTimeoutMicros() * 4
                : this.host.getOperationTimeoutMicros();

        this.host.setTimeoutSeconds((int) TimeUnit.MICROSECONDS.toSeconds(opTimeoutMicros));

        String minimalSelfLinkPrefix = "minimal";
        Service minimalFactory = this.host.startServiceAndWait(
                new MinimalFactoryTestService(), minimalSelfLinkPrefix, new ServiceDocument());

        LuceneDocumentIndexService.setIndexFileCountThresholdForWriterRefresh(100);
        do {
            this.host.log("Expiration: %s, now: %s", expiration, new Date());
            File f = new File(this.host.getStorageSandbox());
            this.host.log("Disk: free %d, usable: %d, total: %d", f.getFreeSpace(),
                    f.getUsableSpace(),
                    f.getTotalSpace());
            this.host.log("Memory: free %d, total: %d, max: %d", Runtime.getRuntime()
                            .freeMemory(),
                    Runtime.getRuntime().totalMemory(),
                    Runtime.getRuntime().maxMemory());
            verifyDocumentExpiration(minimalFactory);
        } while (this.testDurationSeconds > 0 && new Date().before(expiration));
    }

    private void verifyDocumentExpiration(Service minimalFactory)
            throws Throwable, InterruptedException {

        URI factoryUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        Consumer<Operation> setBody = (o) -> {
            ExampleServiceState body = new ExampleServiceState();
            body.name = Utils.getSystemNowMicrosUtc() + "";
            o.setBody(body);
        };
        Consumer<Operation> setBodyMinimal = (o) -> {
            MinimalTestServiceState body = (MinimalTestServiceState) this.host
                    .buildMinimalTestState();
            o.setBody(body);
        };

        Map<URI, ExampleServiceState> services = this.host.doFactoryChildServiceStart(
                null,
                this.serviceCount, ExampleServiceState.class,
                setBody, factoryUri);

        Set<String> names = new HashSet<>();
        TestContext ctx = this.host.testCreate(services.size());
        // patch services to a new version so we verify expiration across multiple versions
        for (URI u : services.keySet()) {
            ExampleServiceState s = new ExampleServiceState();
            s.name = UUID.randomUUID().toString();
            // set a very long expiration
            s.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(
                    TimeUnit.DAYS.toMicros(1));
            names.add(s.name);
            this.host.send(Operation.createPatch(u).setBody(s)
                    .setCompletion(ctx.getCompletion()));
        }
        this.host.testWait(ctx);

        // verify state was updated
        Map<URI, ExampleServiceState> states = this.host.getServiceState(null,
                ExampleServiceState.class, services.keySet());
        for (ExampleServiceState st : states.values()) {
            assertTrue(names.contains(st.name));
            // verify expiration is properly set
            assertTrue(st.documentExpirationTimeMicros > Utils.fromNowMicrosUtc(
                    TimeUnit.DAYS.toMicros(1) / 2));
        }

        Map<String, ServiceStat> stats = this.host.getServiceStats(
                this.host.getDocumentIndexServiceUri());
        ServiceStat deletedCountBeforeExpiration = stats.get(
                LuceneDocumentIndexService.STAT_NAME_SERVICE_DELETE_COUNT
                        + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
        if (deletedCountBeforeExpiration == null) {
            deletedCountBeforeExpiration = new ServiceStat();
        }

        ServiceStat expiredCountBeforeExpiration = stats.get(
                LuceneDocumentIndexService.STAT_NAME_DOCUMENT_EXPIRATION_COUNT
                        + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
        if (expiredCountBeforeExpiration == null) {
            expiredCountBeforeExpiration = new ServiceStat();
        }

        ServiceStat forcedMaintenanceCountBeforeExpiration = stats.get(
                LuceneDocumentIndexService.STAT_NAME_DOCUMENT_EXPIRATION_FORCED_MAINTENANCE_COUNT
                        + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
        if (forcedMaintenanceCountBeforeExpiration == null) {
            forcedMaintenanceCountBeforeExpiration = new ServiceStat();
        }

        long expTime = 0;
        int expectedCount = services.size();

        // first time, patch to zero, which means ignore expiration, and we should not
        // observe any expired documents
        patchOrDeleteWithExpiration(factoryUri, services, expTime, expectedCount);

        // now set expiration to 1, which is definitely in the past, observe all documents expired
        expTime = 1;
        expectedCount = 0;
        patchOrDeleteWithExpiration(factoryUri, services, expTime, expectedCount);
        this.host.log("All example services expired");

        // In the long-running test case, sleep for a couple of maintenance intervals in order to
        // avoid generating unnecessary log spam.
        if (this.host.isLongDurationTest()) {
            Thread.sleep(2 * TimeUnit.MICROSECONDS.toMillis(
                    this.host.getMaintenanceIntervalMicros()));
        }

        ServiceStat deletedCountBaseline = expiredCountBeforeExpiration;
        ServiceStat forcedMaintenanceCountBaseline = forcedMaintenanceCountBeforeExpiration;

        Map<URI, ExampleServiceState> servicesFinal = services;
        this.host.waitFor("expiration stats did not converge", () -> {

            Map<String, ServiceStat> stMap = this.host.getServiceStats(
                    this.host.getDocumentIndexServiceUri());
            ServiceStat deletedCountAfterExpiration = stMap.get(
                    LuceneDocumentIndexService.STAT_NAME_SERVICE_DELETE_COUNT
                            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
            ServiceStat expiredDocumentForcedMaintenanceCount = stMap.get(
                    LuceneDocumentIndexService.STAT_NAME_DOCUMENT_EXPIRATION_FORCED_MAINTENANCE_COUNT
                            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);

            // in batch expiration mode, wait till at least first batch completes
            if (servicesFinal.size() > LuceneDocumentIndexService.getExpiredDocumentSearchThreshold()) {
                if (expiredDocumentForcedMaintenanceCount == null) {
                    this.host.log("Forced maintenance count was null");
                    return false;
                }
                if (expiredDocumentForcedMaintenanceCount.latestValue <
                        forcedMaintenanceCountBaseline.latestValue) {
                    this.host.log("Forced maintenance count was %f",
                            expiredDocumentForcedMaintenanceCount.latestValue);
                    return false;
                }
            }

            if (deletedCountAfterExpiration == null) {
                this.host.log("Deleted count after expiration was null");
                return false;
            }

            if (deletedCountBaseline.latestValue >= deletedCountAfterExpiration.latestValue) {
                this.host.log("No service deletions seen, currently at %f",
                        deletedCountAfterExpiration.latestValue);
                return false;
            }

            stMap = this.host.getServiceStats(
                    this.host.getDocumentIndexServiceUri());
            ServiceStat expCountAfter = stMap.get(
                    LuceneDocumentIndexService.STAT_NAME_DOCUMENT_EXPIRATION_COUNT
                            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);

            if (deletedCountBaseline.latestValue >= expCountAfter.latestValue) {
                this.host.log("No service expirations seen, currently at %f",
                        expCountAfter.latestValue);
                return false;
            }

            // confirm services are stopped
            for (URI u : servicesFinal.keySet()) {
                ProcessingStage s = this.host.getServiceStage(u.getPath());
                if (s != null && s != ProcessingStage.STOPPED) {
                    if (!this.host.isLongDurationTest()) {
                        this.host.log("Found service %s in unexpected state %s", u.getPath(),
                                s.toString());
                    }
                    return false;
                }
            }

            return true;
        });

        // do a more thorough check to ensure the services were removed from the index
        this.host.validatePermanentServiceDocumentDeletion(ExampleService.FACTORY_LINK,
                0, this.host.testDurationSeconds == 0);

        // now create in memory, non indexed services
        Map<URI, MinimalTestServiceState> minimalServices = this.host
                .doFactoryChildServiceStart(null,
                        this.serviceCount, MinimalTestServiceState.class,
                        setBodyMinimal, minimalFactory.getUri());

        this.host.testStart(minimalServices.size());
        for (URI u : minimalServices.keySet()) {
            this.host.send(Operation.createDelete(u)
                    .setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();
        waitForFactoryResults(factoryUri, 0);
        this.host.log("All minimal services deleted");

        ServiceStat stAll = getLuceneStat(
                LuceneDocumentIndexService.STAT_NAME_INDEXED_DOCUMENT_COUNT
                        + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);

        if (stAll != null) {
            this.host.log("total versions: %f", stAll.latestValue);
        }

        // the more services we start, the more we should wait before we expire them
        // to avoid expirations occurring during, or before service start.
        // We give the system a couple of seconds per N services, which is about 100x more
        // than it needs, but CI will sometimes get overloaded and cause false negatives
        long intervalMicros = Math.max(1, this.serviceCount / 2000) * TimeUnit.SECONDS.toMicros(1);
        Consumer<Operation> maintExpSetBody = (o) -> {
            ExampleServiceState body = new ExampleServiceState();
            body.name = UUID.randomUUID().toString();
            body.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(intervalMicros);
            o.setBody(body);
        };

        stats = this.host.getServiceStats(
                this.host.getDocumentIndexServiceUri());
        ServiceStat expCountBaseline = stats.get(
                LuceneDocumentIndexService.STAT_NAME_DOCUMENT_EXPIRATION_COUNT
                        + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);

        // create a new set of services, meant to expire on their own, quickly
        services = this.host.doFactoryChildServiceStart(
                null,
                this.serviceCount, ExampleServiceState.class,
                maintExpSetBody, factoryUri);

        // In the long-running test case, sleep for the expiration period plus one maintenance
        // interval in order to avoid generating unnecessary log spam
        if (this.host.isLongDurationTest()) {
            long sleepMillis = TimeUnit.MICROSECONDS.toMillis(intervalMicros
                    + this.host.getMaintenanceIntervalMicros());
            this.host.log("sleeping to wait for expiration maintenance (%d millis)", sleepMillis);
            Thread.sleep(sleepMillis);
            this.host.log("done sleeping");
        }

        // do not do anything on the services, rely on the maintenance interval to expire them
        this.host.waitFor("Lucene service maintenanance never expired services", () -> {
            Map<String, ServiceStat> statMap = this.host.getServiceStats(
                    this.host.getDocumentIndexServiceUri());
            ServiceStat maintExpiredCount = statMap
                    .get(LuceneDocumentIndexService.STAT_NAME_DOCUMENT_EXPIRATION_COUNT
                            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);

            if (expCountBaseline.latestValue >= maintExpiredCount.latestValue) {
                this.host.log("Documents expired before: %f, now: %f",
                        expCountBaseline.latestValue,
                        maintExpiredCount.latestValue);
                return false;
            }

            ServiceDocumentQueryResult r = this.host.getFactoryState(factoryUri);
            if (r.documentLinks.size() > 0) {
                this.host.log("Documents not expired: %d", r.documentLinks.size());
                return false;
            }
            return true;
        });

        this.host.log("Documents expired through maintenance");

        if (!this.host.isLongDurationTest()) {
            return;
        }

        ServiceDocumentQueryResult r = this.host.getFactoryState(factoryUri);
        ServiceHostState s = this.host.getState();
        this.host.log("number of documents: %d, num services: %s", r.documentLinks.size(),
                s.serviceCount);
        assertEquals(0, r.documentLinks.size());

        validateTimeSeriesStats();
    }

    private void validateTimeSeriesStats() throws Throwable {

        final String[] TIME_SERIES_ENABLED_STATS = new String[] {
                LuceneDocumentIndexService.STAT_NAME_ACTIVE_QUERY_FILTERS,
                LuceneDocumentIndexService.STAT_NAME_ACTIVE_PAGINATED_QUERIES,
                LuceneDocumentIndexService.STAT_NAME_COMMIT_COUNT,
                LuceneDocumentIndexService.STAT_NAME_COMMIT_DURATION_MICROS,
                LuceneDocumentIndexService.STAT_NAME_GROUP_QUERY_COUNT,
                LuceneDocumentIndexService.STAT_NAME_QUERY_DURATION_MICROS,
                LuceneDocumentIndexService.STAT_NAME_GROUP_QUERY_DURATION_MICROS,
                LuceneDocumentIndexService.STAT_NAME_QUERY_SINGLE_DURATION_MICROS,
                LuceneDocumentIndexService.STAT_NAME_QUERY_ALL_VERSIONS_DURATION_MICROS,
                LuceneDocumentIndexService.STAT_NAME_RESULT_PROCESSING_DURATION_MICROS,
                LuceneDocumentIndexService.STAT_NAME_INDEXED_FIELD_COUNT,
                LuceneDocumentIndexService.STAT_NAME_INDEXED_DOCUMENT_COUNT,
                LuceneDocumentIndexService.STAT_NAME_INDEXING_DURATION_MICROS,
                LuceneDocumentIndexService.STAT_NAME_SEARCHER_UPDATE_COUNT,
                LuceneDocumentIndexService.STAT_NAME_SERVICE_DELETE_COUNT,
                LuceneDocumentIndexService.STAT_NAME_DOCUMENT_EXPIRATION_COUNT,
                LuceneDocumentIndexService.STAT_NAME_DOCUMENT_EXPIRATION_FORCED_MAINTENANCE_COUNT,
        };

        Map<String, ServiceStat> indexServiceStats = this.host.getServiceStats(
                this.host.getDocumentIndexServiceUri());
        assertTrue(indexServiceStats.size() > TIME_SERIES_ENABLED_STATS.length);
        for (String name : TIME_SERIES_ENABLED_STATS) {
            validateTimeSeriesStat(indexServiceStats, name);
        }
    }

    private void validateTimeSeriesStat(Map<String, ServiceStat> indexServiceStats, String name) {
        String nameForDayStat = name + ServiceStats.STAT_NAME_SUFFIX_PER_DAY;
        ServiceStat dayStat = indexServiceStats.get(nameForDayStat);
        if (dayStat != null) {
            // ignore entries not updated as part of the current test
            TestUtilityService.validateTimeSeriesStat(dayStat, TimeUnit.HOURS.toMillis(1));
        }
        String nameForHourStat = name + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;
        ServiceStat hourStat = indexServiceStats.get(nameForHourStat);
        if (hourStat != null) {
            TestUtilityService.validateTimeSeriesStat(hourStat, TimeUnit.MINUTES.toMillis(1));
        }
    }

    private void patchOrDeleteWithExpiration(URI factoryUri, Map<URI, ExampleServiceState> services,
            long expTime, int expectedCount) throws Throwable, InterruptedException {
        // now patch again, this time setting expiration to 1 (so definitely in the past)
        this.host.testStart(services.size());
        int i = 0;
        for (URI u : services.keySet()) {
            ExampleServiceState s = new ExampleServiceState();
            s.name = UUID.randomUUID().toString();
            s.documentExpirationTimeMicros = expTime;
            Operation op = Operation.createPatch(u).setBody(s)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        // verify response body matches request
                        ExampleServiceState st = o.getBody(ExampleServiceState.class);
                        if (!s.name.equals(st.name)
                                || s.documentExpirationTimeMicros != st.documentExpirationTimeMicros) {
                            this.host.failIteration(new IllegalStateException(
                                    "Response not expected:" + Utils.toJson(st)));
                            return;
                        }
                        this.host.completeIteration();
                    });
            if (expTime == 1 && (++i) % 2 == 0) {
                // Send a DELETE for every other request. We are verifying that
                // updating expiration with either a PATCH or a DELETE, works.
                op.setAction(Action.DELETE);
            }
            this.host.send(op);
        }
        this.host.testWait();

        if (expTime == 0) {
            // we are disabling expiration, so to verify expiration does NOT happen, wait at least
            // a couple of maintenance intervals
            Thread.sleep(TimeUnit.MICROSECONDS.toMillis(this.host.getMaintenanceIntervalMicros()) * 2);
        }

        // send a GET immediately and expect either failure or success, we are doing it
        // to ensure it actually completes
        boolean sendDelete = expTime != 0 && expTime < Utils.getSystemNowMicrosUtc();
        int count = services.size();
        if (sendDelete) {
            count *= 2;
        }
        this.host.testStart(count);
        for (URI u : services.keySet()) {
            this.host.send(Operation.createGet(u).setCompletion((o, e) -> {
                this.host.completeIteration();
            }));

            if (!sendDelete) {
                continue;
            }
            // if expiration is in the past also send a DELETE, to once again make sure its completed
            this.host.send(Operation.createDelete(u).setBody(new ServiceDocument())
                    .setCompletion((o, e) -> {
                        this.host.completeIteration();
                    }));
        }
        this.host.testWait();

        // verify services expired
        waitForFactoryResults(factoryUri, expectedCount);
    }

    private void waitForFactoryResults(URI factoryUri, int expectedCount)
            throws Throwable, InterruptedException {

        ServiceDocumentQueryResult[] rsp = new ServiceDocumentQueryResult[1];
        this.host.waitFor("services never expired", () -> {
            int actualCount = 0;
            TestContext ctx = this.host.testCreate(1);
            Operation op = Operation.createGet(factoryUri).setCompletion((o, e) -> {
                if (e != null) {
                    ctx.fail(e);
                    return;
                }
                rsp[0] = o.getBody(ServiceDocumentQueryResult.class);
                ctx.complete();
            });
            this.host.queryServiceUris(factoryUri.getPath() + "/*", op);
            this.host.testWait(ctx);
            for (String link : rsp[0].documentLinks) {
                ProcessingStage ps = this.host.getServiceStage(link);
                if (ps != ProcessingStage.AVAILABLE) {
                    continue;
                }
                actualCount++;
            }

            if (actualCount == expectedCount && rsp[0].documentLinks.size() == expectedCount) {
                return true;
            }

            if (!this.host.isLongDurationTest()) {
                this.host.log("Expected example service count: %d, current: %d", expectedCount,
                        actualCount);
            }

            return false;
        });
    }

    @Test
    public void testBackupAndRestoreFromZipFile() throws Throwable {
        setUpHost(false);

        LuceneDocumentIndexService.BackupRequest b = new LuceneDocumentIndexService.BackupRequest();
        b.documentKind = LuceneDocumentIndexService.BackupRequest.KIND;

        int count = 1000;
        URI factoryUri = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK);

        Map<URI, ExampleServiceState> exampleStates = this.host.doFactoryChildServiceStart(null,
                count,
                ExampleServiceState.class,
                (o) -> {
                    ExampleServiceState s = new ExampleServiceState();
                    s.name = UUID.randomUUID().toString();
                    o.setBody(s);
                }, factoryUri);

        Operation backupOp = Operation.createPatch(UriUtils.buildUri(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX))
                .setBody(b);

        TestRequestSender sender = new TestRequestSender(this.host);
        BackupResponse backupResponse = sender.sendAndWait(backupOp, BackupResponse.class);

        // destroy and spin up new host
        this.host.tearDown();
        this.host = null;
        setUpHost(false);
        sender = this.host.getTestRequestSender();

        LuceneDocumentIndexService.RestoreRequest r = new LuceneDocumentIndexService.RestoreRequest();
        r.documentKind = LuceneDocumentIndexService.RestoreRequest.KIND;
        r.backupFile = backupResponse.backupFile;

        Operation restoreOp = Operation.createPatch(UriUtils.buildUri(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX))
                .setBody(r);
        sender.sendAndWait(restoreOp);

        // Check our documents are still there
        ServiceDocumentQueryResult queryResult = this.host
                .getFactoryState(UriUtils.buildExpandLinksQueryUri(UriUtils.buildUri(this.host,
                        ExampleService.FACTORY_LINK)));
        assertNotNull(queryResult);
        assertNotNull(queryResult.documents);
        assertEquals(queryResult.documents.size(), exampleStates.keySet().size());

        HashMap<String, ExampleServiceState> out = queryResultToExampleState(queryResult);

        // now test the reference bodies match the query results
        for (Entry<URI, ExampleServiceState> exampleDoc : exampleStates.entrySet()) {
            ExampleServiceState in = exampleDoc.getValue();
            ExampleServiceState testState = out.get(in.documentSelfLink);
            assertNotNull(testState);
            assertEquals(in.name, testState.name);
            assertEquals(in.counter, testState.counter);
        }
    }

    @Test
    public void serviceVersionRetentionAndGrooming() throws Throwable {
        try {
            Utils.setTimeDriftThreshold(TimeUnit.HOURS.toMicros(1));
            MinimalTestService.setVersionRetentionLimit(1);
            for (int i = 0; i < this.iterationCount; i++) {
                EnumSet<ServiceOption> caps = EnumSet.of(ServiceOption.PERSISTENCE);
                doServiceVersionGroomingValidation(caps);
                tearDown();
            }
        } finally {
            Utils.setTimeDriftThreshold(Utils.DEFAULT_TIME_DRIFT_THRESHOLD_MICROS);
            MinimalTestService.setVersionRetentionLimit(
                    MinimalTestService.DEFAULT_VERSION_RETENTION_LIMIT);
        }
    }

    private void doServiceVersionGroomingValidation(EnumSet<ServiceOption> caps) throws Throwable {
        setUpHost(false);
        URI documentIndexUri = UriUtils.buildUri(this.host, LuceneDocumentIndexService.SELF_LINK);

        // Start some services which will live for the lifetime of the test
        Map<URI, ExampleServiceState> exampleStates = this.host.doFactoryChildServiceStart(
                null,
                this.serviceCount,
                ExampleServiceState.class,
                (o) -> {
                    ExampleServiceState s1 = new ExampleServiceState();
                    s1.name = UUID.randomUUID().toString();
                    o.setBody(s1);
                },
                UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));

        Collection<URI> serviceUrisWithStandardRetention = exampleStates.keySet();

        long end = Utils.getSystemNowMicrosUtc()
                + TimeUnit.SECONDS.toMicros(this.testDurationSeconds);
        final long offset = 10;
        long updateCount = ExampleServiceState.VERSION_RETENTION_LIMIT + offset;

        do {
            // Start some services which will live only for the current test iteration
            List<Service> services = this.host.doThroughputServiceStart(
                    this.serviceCount, MinimalTestService.class,
                    this.host.buildMinimalTestState(), caps,
                    null);

            Collection<URI> serviceUrisWithCustomRetention = new ArrayList<>();
            for (Service s : services) {
                serviceUrisWithCustomRetention.add(s.getUri());
            }

            // Verify that interleaved updates to multiple services are honored
            this.host.testStart(this.serviceCount * updateCount);
            for (int i = 0; i < updateCount; i++) {
                for (URI u : serviceUrisWithStandardRetention) {
                    ExampleServiceState st = new ExampleServiceState();
                    st.name = i + "";
                    this.host.send(Operation.createPut(u)
                            .setBody(st)
                            .setCompletion(this.host.getCompletion()));
                }
            }
            this.host.testWait();
            long floor = ExampleServiceState.VERSION_RETENTION_FLOOR;
            verifyVersionRetention(serviceUrisWithStandardRetention, floor, floor + updateCount);

            // Set the update count for the next iteration so that we'll stay within the retention
            // window of the persisted documents
            updateCount = ExampleServiceState.VERSION_RETENTION_LIMIT
                    - ExampleServiceState.VERSION_RETENTION_FLOOR;

            // Verify that services with a low version limit of 1 are honored.
            int lowLimitUpdateCount = 10;
            this.host.testStart(lowLimitUpdateCount * this.serviceCount);
            for (int i = 0; i < lowLimitUpdateCount; i++) {
                for (URI u : serviceUrisWithCustomRetention) {
                    this.host.send(Operation.createPut(u)
                            .setBody(this.host.buildMinimalTestState())
                            .setCompletion(this.host.getCompletion()));
                }
            }
            this.host.testWait();
            verifyVersionRetention(serviceUrisWithCustomRetention, 1,
                    lowLimitUpdateCount / 2);

            // Delete the services which are specific to this iteration.
            this.host.testStart(this.serviceCount);
            for (URI u : serviceUrisWithCustomRetention) {
                this.host.send(Operation.createDelete(u)
                        .setCompletion(this.host.getCompletion()));
            }
            this.host.testWait();

            // This can be removed once we're confident these tests are stable
            this.host.logServiceStats(documentIndexUri, this.testResults);
        } while (Utils.getSystemNowMicrosUtc() < end);
    }

    private void verifyVersionRetention(Collection<URI> serviceUris, long floor, long limit)
            throws Throwable {

        long maintIntervalMillis = TimeUnit.MICROSECONDS
                .toMillis(this.host.getMaintenanceIntervalMicros());

        // let at least one maintenance interval pass. not essential, since we loop below
        // but lets more documents get deleted at once
        Thread.sleep(maintIntervalMillis);

        QueryTask.Query.Builder b = QueryTask.Query.Builder.create();
        serviceUris.forEach((u) -> b.addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK,
                u.getPath(), Occurance.SHOULD_OCCUR));

        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        q.query = b.build();
        q.options = EnumSet.of(QueryOption.COUNT, QueryOption.INCLUDE_ALL_VERSIONS);

        try {
            this.host.waitFor("Version retention failed to remove some documents", () -> {
                QueryTask qt = QueryTask.create(q).setDirect(true);
                this.host.createQueryTaskService(qt, false, true, qt, null);
                return qt.results.documentCount >= serviceUris.size() * floor
                        && qt.results.documentCount <= serviceUris.size() * limit;
            });
        } catch (Throwable t) {
            long upperBound = serviceUris.size() * limit;
            // Get the set of document links which caused the failure
            q.options = EnumSet.of(QueryOption.INCLUDE_ALL_VERSIONS);
            QueryTask qt = QueryTask.create(q).setDirect(true);
            this.host.createQueryTaskService(qt, false, true, qt, null);
            HashMap<String, TreeSet<Integer>> aggregated = new HashMap<>();
            for (String link : qt.results.documentLinks) {
                String documentSelfLink = link.split("\\?")[0];
                TreeSet<Integer> versions = aggregated.get(documentSelfLink);
                if (versions == null) {
                    versions = new TreeSet<>();
                }
                versions.add(Integer.parseInt(link.split("=")[1]));
                aggregated.put(documentSelfLink, versions);
            }
            aggregated.entrySet().stream()
                    .filter((entry) -> entry.getValue().size() < floor
                            || entry.getValue().size() > limit)
                    .forEach((entry) -> {
                        String documentSelfLink = entry.getKey();
                        TreeSet<Integer> versions = entry.getValue();
                        this.host.log("Failing documentSelfLink:%s, "
                                + "lowestVersion:%d, highestVersion:%d, count:%d, expected count (ceil): %d",
                                documentSelfLink, versions.first(), versions.last(),
                                versions.size(), upperBound);
                    });
            throw t;
        }
    }

    @Test
    public void throughputPutSingleVersionRetention() throws Throwable {
        long limit = this.retentionLimit;
        long floor = this.retentionFloor;
        try {
            this.retentionFloor = 1L;
            this.retentionLimit = 1L;
            throughputPut();
            this.host.waitFor("stat did not update", () -> {
                ServiceStat indexDocSingleVersionCountSt = getLuceneStat(
                        LuceneDocumentIndexService.STAT_NAME_VERSION_RETENTION_SERVICE_COUNT
                                + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);

                return indexDocSingleVersionCountSt.latestValue >= this.serviceCount;
            });
        } finally {
            this.retentionFloor = floor;
            this.retentionLimit = limit;
        }
    }

    public static class MinimalTestServiceWithIndexedMetadata extends MinimalTestService {

        @Override
        public ServiceDocument getDocumentTemplate() {
            ServiceDocument template = super.getDocumentTemplate();
            template.documentDescription.documentIndexingOptions =
                    EnumSet.of(ServiceDocumentDescription.DocumentIndexingOption.INDEX_METADATA);
            return template;
        }
    }

    @Test
    public void throughputPut() throws Throwable {
        int serviceThreshold = LuceneDocumentIndexService.getVersionRetentionServiceThreshold();
        long limit = MinimalTestService.getVersionRetentionLimit();
        long floor = MinimalTestService.getVersionRetentionFloor();
        try {
            setUpHost(false);
            LuceneDocumentIndexService.setVersionRetentionServiceThreshold((int) this.serviceCount);
            MinimalTestService.setVersionRetentionLimit(this.retentionLimit);
            MinimalTestService.setVersionRetentionFloor(this.retentionFloor);
            if (this.host.isStressTest()) {
                Utils.setTimeDriftThreshold(TimeUnit.HOURS.toMicros(1));
            }
            // Do throughput testing for basic, mutable services
            doDurableServiceUpdate(Action.PUT, MinimalTestService.class,
                    this.serviceCount, this.updateCount, null);
            // Now do the same test for a similar service with indexed metadata
            doDurableServiceUpdate(Action.PUT, MinimalTestServiceWithIndexedMetadata.class,
                    this.serviceCount, this.updateCount, null);
        } finally {
            Utils.setTimeDriftThreshold(Utils.DEFAULT_TIME_DRIFT_THRESHOLD_MICROS);
            MinimalTestService.setVersionRetentionLimit(limit);
            MinimalTestService.setVersionRetentionFloor(floor);
            LuceneDocumentIndexService.setVersionRetentionServiceThreshold(serviceThreshold);
        }
    }

    private void doDurableServiceUpdate(Action action, Class<? extends Service> type,
            long serviceCount, Integer putCount, EnumSet<ServiceOption> caps) throws Throwable {
        EnumSet<TestProperty> props = EnumSet.noneOf(TestProperty.class);

        if (caps == null) {
            caps = EnumSet.of(ServiceOption.PERSISTENCE);
            props.add(TestProperty.PERSISTED);
        }
        if (putCount != null && putCount == 1) {
            props.add(TestProperty.SINGLE_ITERATION);
        }

        List<Service> services = this.host.doThroughputServiceStart(serviceCount, type,
                this.host.buildMinimalTestState(), caps, null);

        long count = this.host.computeIterationsFromMemory(props, (int) serviceCount);
        if (caps.contains(Service.ServiceOption.PERSISTENCE)) {
            // reduce iteration count for durable services
            count = Math.max(1, count / 10);
        }

        if (putCount != null) {
            count = putCount;
        }

        // increase queue limit so each service instance does not apply back pressure
        this.host.testStart(services.size());
        for (Service s : services) {
            ServiceConfigUpdateRequest body = ServiceConfigUpdateRequest.create();
            body.operationQueueLimit = (int) count;
            URI configUri = UriUtils.buildConfigUri(s.getUri());
            this.host.send(Operation.createPatch(configUri).setBody(body)
                    .setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();

        for (int i = 0; i < this.iterationCount; i++) {
            double throughput = this.host.doServiceUpdates(action, count, props, services);
            this.testResults.getReport().all(TestResults.KEY_THROUGHPUT, throughput);
            logQuerySingleStat();
        }

        // decrease maintenance, which will trigger cache clears
        this.host.setMaintenanceIntervalMicros(250000);
        Thread.sleep(500);

        Map<URI, MinimalTestServiceState> statesBeforeRestart = this.host.getServiceState(null,
                MinimalTestServiceState.class, services);
        int mismatchCount = 0;
        for (MinimalTestServiceState st : statesBeforeRestart.values()) {
            if (st.documentVersion != count * this.iterationCount) {
                this.host.log("Version mismatch for %s. Expected %d, got %d", st.documentSelfLink,
                        count * this.iterationCount, st.documentVersion);
                mismatchCount++;
            }
        }
        assertTrue(mismatchCount == 0);
    }

    @Test
    public void testImplicitQueryPageSize() throws Throwable {
        setUpHost(false);
        this.indexService.toggleOption(ServiceOption.INSTRUMENTATION, true);
        URI factoryUri = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK);
        Map<URI, ExampleServiceState> exampleStates = this.host.doFactoryChildServiceStart(
                null, this.serviceCount, ExampleServiceState.class,
                (o) -> {
                    ExampleServiceState state = new ExampleServiceState();
                    state.name = UUID.randomUUID().toString();
                    o.setBody(state);
                }, factoryUri);

        Collection<URI> exampleUris = exampleStates.keySet();

        this.host.testStart(this.serviceCount * this.updateCount);
        for (int i = 0; i < this.updateCount; i++) {
            for (URI u : exampleUris) {
                ExampleServiceState state = new ExampleServiceState();
                state.name = i + "";
                this.host.send(Operation.createPut(u)
                        .setBody(state)
                        .setCompletion(this.host.getCompletion()));
            }
        }
        this.host.testWait();

        Query query = QueryTask.Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();

        QueryTask.QuerySpecification querySpec = new QueryTask.QuerySpecification();
        querySpec.query = query;
        querySpec.options = EnumSet.of(QueryOption.TOP_RESULTS);
        querySpec.resultLimit = (int) this.serviceCount;

        // Sort by document version in order to ensure that the document index service sees all
        // non-current document versions first.
        querySpec.sortTerm = new QueryTask.QueryTerm();
        querySpec.sortTerm.propertyName = "documentVersion";
        querySpec.sortTerm.propertyType = ServiceDocumentDescription.TypeName.LONG;

        int pageSize = LuceneDocumentIndexService.getImplicitQueryProcessingPageSize();
        try {
            // Without an implicit processing limit, we should see n passes per query
            verifyImplicitQueryPageSize(querySpec, 1, (1 + this.updateCount));
            // With an implicit processing limit, we should see one pass per query
            int size = (int) (1 + this.serviceCount) * this.updateCount;
            verifyImplicitQueryPageSize(querySpec, size, 1.0);
        } finally {
            LuceneDocumentIndexService.setImplicitQueryProcessingPageSize(pageSize);
        }
    }

    private void verifyImplicitQueryPageSize(QueryTask.QuerySpecification querySpec, int pageSize,
            double expectedIterationCount) {

        LuceneDocumentIndexService.setImplicitQueryProcessingPageSize(pageSize);

        QueryTask qt = QueryTask.create(querySpec).setDirect(true);
        this.host.createQueryTaskService(qt, false, true, qt, null);
        assertEquals(qt.results.documentLinks.size(), this.serviceCount);

        ServiceStat st = getLuceneStat(LuceneDocumentIndexService.STAT_NAME_ITERATIONS_PER_QUERY);
        assertEquals(st.latestValue, expectedIterationCount, 0.01);
    }

    /**
     * Test Lucene index upgrade to Version.CURRENT.  On host start, the index should
     * be upgraded in place.  We've embedded an old index with a example service documents.
     * Verify the fields are still valid.
     */
    @Test
    public void indexUpgrade() throws Throwable {
        setUpHost(false);
        // Stop the host, without cleaning up storage.
        this.host.stop();

        File curLuceneDir = new File(new File(this.host.getStorageSandbox()),
                LuceneDocumentIndexService.FILE_PATH_LUCENE);

        // Copy the old lucene index to the current sandbox
        replaceWithOldIndex(this.INDEX_DIR_NAME, curLuceneDir.toPath());
        // ask OS to gives us an available port, old one might be taken
        this.host.setPort(0);
        // Restart host with the same storage sandbox. If host does not throw, we are good.
        this.host.start();

        HashMap<String, ExampleServiceState> reference = loadState(this.getClass().getResource(
                this.EXAMPLES_BODIES_FILE));

        // do GET on all child URIs
        ServiceDocumentQueryResult queryResult = this.host
                .getFactoryState(UriUtils.buildExpandLinksQueryUri(UriUtils.buildFactoryUri(
                        this.host,
                        ExampleService.class)));
        assertNotNull(queryResult);
        assertNotNull(queryResult.documents);
        assertEquals(queryResult.documents.size(), reference.size());

        HashMap<String, ExampleServiceState> out = queryResultToExampleState(queryResult);

        // now test the reference bodies match the query results
        for (String selfLink : reference.keySet()) {
            ExampleServiceState r = reference.get(selfLink);
            ExampleServiceState testState = out.get(selfLink);
            assertNotNull(testState);
            assertEquals(r.name, testState.name);
            assertEquals(r.counter, testState.counter);
        }
    }

    @Test
    public void offset() throws Throwable {
        setUpHost(false);

        TestRequestSender sender = this.host.getTestRequestSender();
        List<Operation> ops = new ArrayList<>();
        int count = 5;
        for (int i = 0; i < count; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = "foo-" + i;
            state.documentSelfLink = state.name;
            ops.add(Operation.createPost(this.host, ExampleService.FACTORY_LINK).setBody(state));
        }
        sender.sendAndWait(ops);

        QueryTask.Query query = QueryTask.Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();

        QueryTask task = QueryTask.Builder.createDirectTask()
                .setOffset(3)
                .orderAscending(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING)
                .setQuery(query)
                .build();

        Operation post = Operation.createPost(this.host, LocalQueryTaskFactoryService.SELF_LINK).setBody(task);
        QueryTask result = sender.sendAndWait(post, QueryTask.class);

        assertNotNull(result.results.documentLinks);
        assertEquals(Long.valueOf(2), result.results.documentCount);
        assertEquals(2, result.results.documentLinks.size());
        assertEquals("/core/examples/foo-3", result.results.documentLinks.get(0));
        assertEquals("/core/examples/foo-4", result.results.documentLinks.get(1));

        // with offset=5 (same as number of documents)
        task = QueryTask.Builder.createDirectTask()
                .setOffset(5)
                .orderAscending(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING)
                .setQuery(query)
                .build();

        post = Operation.createPost(this.host, LocalQueryTaskFactoryService.SELF_LINK).setBody(task);
        result = sender.sendAndWait(post, QueryTask.class);

        assertEquals(Long.valueOf(0), result.results.documentCount);
        assertNotNull(result.results.documentLinks);
        assertEquals(result.results.documentLinks.size(), 0);

        // with offset=7 (greater than actual document)
        task = QueryTask.Builder.createDirectTask()
                .setOffset(7)
                .orderAscending(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING)
                .setQuery(query)
                .build();

        post = Operation.createPost(this.host, LocalQueryTaskFactoryService.SELF_LINK).setBody(task);
        result = sender.sendAndWait(post, QueryTask.class);

        assertEquals(Long.valueOf(0), result.results.documentCount);
        assertNotNull(result.results.documentLinks);
        assertEquals(result.results.documentLinks.size(), 0);

        // with TOP_RESULT
        task = QueryTask.Builder.createDirectTask()
                .setOffset(3)
                .setResultLimit(10)
                .addOption(QueryOption.TOP_RESULTS)
                .orderAscending(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING)
                .setQuery(query)
                .build();

        post = Operation.createPost(this.host, LocalQueryTaskFactoryService.SELF_LINK).setBody(task);
        result = sender.sendAndWait(post, QueryTask.class);

        assertEquals(Long.valueOf(2), result.results.documentCount);
        assertEquals(2, result.results.documentLinks.size());
        assertEquals("/core/examples/foo-3", result.results.documentLinks.get(0));
        assertEquals("/core/examples/foo-4", result.results.documentLinks.get(1));


        // with EXPAND_CONTENT
        task = QueryTask.Builder.createDirectTask()
                .setOffset(3)
                .addOption(QueryOption.EXPAND_CONTENT)
                .orderAscending(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING)
                .setQuery(query)
                .build();

        post = Operation.createPost(this.host, LocalQueryTaskFactoryService.SELF_LINK).setBody(task);
        result = sender.sendAndWait(post, QueryTask.class);

        assertEquals(Long.valueOf(2), result.results.documentCount);
        assertEquals(2, result.results.documentLinks.size());
        assertEquals("/core/examples/foo-3", result.results.documentLinks.get(0));
        assertEquals("/core/examples/foo-4", result.results.documentLinks.get(1));

        assertNotNull(result.results.documents);
        assertEquals(2, result.results.documents.size());
        assertTrue(result.results.documents.containsKey("/core/examples/foo-3"));
        assertTrue(result.results.documents.containsKey("/core/examples/foo-4"));
    }

    @Test
    public void offsetWithResultLimit() throws Throwable {
        setUpHost(false);

        TestRequestSender sender = this.host.getTestRequestSender();
        List<Operation> ops = new ArrayList<>();
        int count = 5;
        for (int i = 0; i < count; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = "foo-" + i;
            state.documentSelfLink = state.name;
            ops.add(Operation.createPost(this.host, ExampleService.FACTORY_LINK).setBody(state));
        }
        sender.sendAndWait(ops);

        QueryTask.Query query = QueryTask.Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();

        // with resultLimit
        QueryTask task = QueryTask.Builder.createDirectTask()
                .setOffset(1)
                .setResultLimit(2)
                .orderAscending(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING)
                .setQuery(query)
                .build();

        Operation post = Operation.createPost(this.host, LocalQueryTaskFactoryService.SELF_LINK).setBody(task);
        QueryTask result = sender.sendAndWait(post, QueryTask.class);

        assertNotNull(result.results.nextPageLink);
        QueryTask p1 = sender.sendAndWait(Operation.createGet(this.host, result.results.nextPageLink), QueryTask.class);

        assertEquals(Long.valueOf(2), p1.results.documentCount);
        assertEquals(2, p1.results.documentLinks.size());
        assertEquals("/core/examples/foo-1", p1.results.documentLinks.get(0));
        assertEquals("/core/examples/foo-2", p1.results.documentLinks.get(1));

        assertNotNull(p1.results.nextPageLink);
        QueryTask p2 = sender.sendAndWait(Operation.createGet(this.host, p1.results.nextPageLink), QueryTask.class);

        assertEquals(Long.valueOf(2), p2.results.documentCount);
        assertEquals(2, p2.results.documentLinks.size());
        assertEquals("/core/examples/foo-3", p2.results.documentLinks.get(0));
        assertEquals("/core/examples/foo-4", p2.results.documentLinks.get(1));

        assertNull(p2.results.nextPageLink);

        // offset is greater than the num of result
        task = QueryTask.Builder.createDirectTask()
                .setOffset(10)
                .setResultLimit(2)
                .orderAscending(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING)
                .setQuery(query)
                .build();

        post = Operation.createPost(this.host, LocalQueryTaskFactoryService.SELF_LINK).setBody(task);
        result = sender.sendAndWait(post, QueryTask.class);

        assertNotNull(result.results.nextPageLink);
        p1 = sender.sendAndWait(Operation.createGet(this.host, result.results.nextPageLink), QueryTask.class);

        assertEquals(Long.valueOf(0), p1.results.documentCount);
        assertNotNull(p1.results.documentLinks);
        assertEquals(0, p1.results.documentLinks.size());

        // first page docs are less than resultLimit
        task = QueryTask.Builder.createDirectTask()
                .setOffset(4)
                .setResultLimit(2)
                .orderAscending(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING)
                .setQuery(query)
                .build();

        post = Operation.createPost(this.host, LocalQueryTaskFactoryService.SELF_LINK).setBody(task);
        result = sender.sendAndWait(post, QueryTask.class);

        assertNotNull(result.results.nextPageLink);
        p1 = sender.sendAndWait(Operation.createGet(this.host, result.results.nextPageLink), QueryTask.class);

        assertEquals(Long.valueOf(1), p1.results.documentCount);
        assertEquals(1, p1.results.documentLinks.size());
        assertEquals("/core/examples/foo-4", p1.results.documentLinks.get(0));
        assertNull(p1.results.nextPageLink);

        // second page docs are less than resultLimit
        task = QueryTask.Builder.createDirectTask()
                .setOffset(2)
                .setResultLimit(2)
                .orderAscending(ExampleServiceState.FIELD_NAME_NAME, TypeName.STRING)
                .setQuery(query)
                .build();

        post = Operation.createPost(this.host, LocalQueryTaskFactoryService.SELF_LINK).setBody(task);
        result = sender.sendAndWait(post, QueryTask.class);

        assertNotNull(result.results.nextPageLink);
        p1 = sender.sendAndWait(Operation.createGet(this.host, result.results.nextPageLink), QueryTask.class);

        assertEquals(Long.valueOf(2), p1.results.documentCount);
        assertEquals(2, p1.results.documentLinks.size());
        assertEquals("/core/examples/foo-2", p1.results.documentLinks.get(0));
        assertEquals("/core/examples/foo-3", p1.results.documentLinks.get(1));

        assertNotNull(p1.results.nextPageLink);
        p2 = sender.sendAndWait(Operation.createGet(this.host, p1.results.nextPageLink), QueryTask.class);

        assertEquals(Long.valueOf(1), p2.results.documentCount);
        assertEquals(1, p2.results.documentLinks.size());
        assertEquals("/core/examples/foo-4", p2.results.documentLinks.get(0));

        assertNull(p2.results.nextPageLink);
    }


    private HashMap<String, ExampleServiceState> loadState(URL exampleBodies) throws Throwable {
        File exampleServiceBodiesFile = new File(exampleBodies.toURI());

        final HashMap<String, ExampleServiceState> state = new HashMap<>();

        if (exampleServiceBodiesFile.exists()) {
            this.host.testStart(1);
            FileUtils.readFileAndComplete(
                    Operation.createGet(null).setCompletion(
                            (o, e) -> {
                                if (e != null) {
                                    this.host.log(Level.WARNING,
                                            "Failure loading state from %s: %s",
                                            exampleServiceBodiesFile, Utils.toString(e));
                                    this.host.completeIteration();
                                    return;
                                }

                                try {
                                    ServiceDocumentQueryResult r = o
                                            .getBody(ServiceDocumentQueryResult.class);
                                    if (r.documents == null || r.documents.isEmpty()) {
                                        this.host.log(Level.WARNING, "Invalid state from %s: %s",
                                                exampleServiceBodiesFile,
                                                Utils.toJsonHtml(r));
                                        this.host.completeIteration();
                                        return;
                                    }

                                    state.putAll(queryResultToExampleState(r));
                                    this.host.completeIteration();

                                } catch (Throwable ex) {
                                    this.host.log(Level.WARNING, "Invalid state from %s: %s",
                                            exampleServiceBodiesFile,
                                            Utils.toJsonHtml(o.getBodyRaw()));
                                    this.host.completeIteration();
                                }
                            }), exampleServiceBodiesFile);
            this.host.testWait();
        }

        return state;
    }

    private void replaceWithOldIndex(String oldLuceneDirName, Path curLuceneIndexPath)
            throws Throwable {

        // clean the current sandbox.
        Files.list(curLuceneIndexPath).forEach((Path fileP) -> {
            String name = fileP.toString();
            if (name.equals("write.lock")) {
                return;
            }
            try {
                Files.delete(fileP);
            } catch (Throwable e) {

            }
        });

        URL pathToOldLuceneDir = this.getClass().getResource(oldLuceneDirName);

        FileUtils.copyFiles(new File(pathToOldLuceneDir.toURI()), curLuceneIndexPath.toFile());
    }

    @Test
    public void commitNotification() throws Throwable {

        // To test commit callback, first, start up the host. This will generate some initial data in index.
        setUpHost(false);

        // Then, restart the host with same port to reuse the sandbox.
        // Since it reuses previous sandbox, there will be nothing to commit in lucene.
        // Therefore, until test populates data, commit callback will NOT be called.
        this.host.stop();
        this.host.setPort(0);
        this.host.start();
        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);

        AtomicBoolean isNotified = new AtomicBoolean(false);
        AtomicLong sequenceNumber = new AtomicLong();

        Operation subscribe = Operation.createPost(UriUtils.buildUri(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX))
                .setReferer(this.host.getUri());

        this.host.startSubscriptionService(subscribe, notifyOp -> {
            isNotified.set(true);
            sequenceNumber.set(notifyOp.getBody(CommitInfo.class).sequenceNumber);
            notifyOp.complete();
        });

        assertFalse("callback should not be called before populating data.(nothing to commit)", isNotified.get());

        // populate data
        TestRequestSender sender = this.host.getTestRequestSender();
        List<Operation> posts = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = "foo-" + i;
            posts.add(Operation.createPost(this.host, ExampleService.FACTORY_LINK).setBody(state));
        }
        sender.sendAndWait(posts, ExampleServiceState.class);


        // verify commit has performed
        this.host.waitFor("commit callback should be called", isNotified::get);
        assertTrue("Something should be committed", sequenceNumber.get() > -1);
    }


    private void logQuerySingleStat() {
        Map<String, ServiceStat> stats = this.host
                .getServiceStats(this.host.getDocumentIndexServiceUri());
        ServiceStat querySingleDurationStat = stats
                .get(LuceneDocumentIndexService.STAT_NAME_QUERY_SINGLE_DURATION_MICROS);
        if (querySingleDurationStat == null) {
            return;
        }
        this.host.log("%s", Utils.toJsonHtml(querySingleDurationStat));
    }

    public static HashMap<String, ExampleServiceState> queryResultToExampleState(
            ServiceDocumentQueryResult r) {
        HashMap<String, ExampleServiceState> state = new HashMap<>();
        for (String k : r.documents.keySet()) {
            state.put(k, Utils.fromJson(r.documents.get(k), ExampleServiceState.class));
        }
        return state;
    }

    @Test
    public void testDocumentMetadataIndexing() throws Throwable {
        LuceneDocumentIndexService.setMetadataUpdateMaxQueueDepth((int) this.serviceCount / 2);
        try {
            doDocumentMetadataIndexing();
        } finally {
            LuceneDocumentIndexService.setMetadataUpdateMaxQueueDepth(
                    LuceneDocumentIndexService.DEFAULT_METADATA_UPDATE_MAX_QUEUE_DEPTH);
        }
    }

    private void doDocumentMetadataIndexing() throws Throwable {
        setUpHost(false);
        URI indexedMetadataFactoryUri = createIndexedMetadataFactoryService(this.host);

        ExampleServiceState serviceState = new ExampleServiceState();
        serviceState.name = "name";

        Map<URI, ExampleServiceState> services = this.host.doFactoryChildServiceStart(null,
                this.serviceCount, ExampleServiceState.class,
                (o) -> o.setBody(serviceState),
                indexedMetadataFactoryUri);

        for (int i = 0; i < this.updateCount; i++) {
            serviceState.counter = (long) i;
            for (URI serviceUri : services.keySet()) {
                Operation patch = Operation.createPatch(serviceUri).setBody(serviceState);
                this.host.send(patch);
            }
        }

        this.host.waitFor("Metadata indexing failed to occur", () -> {
            Map<String, ServiceStat> indexStats = this.host.getServiceStats(
                    this.host.getDocumentIndexServiceUri());
            ServiceStat stat = indexStats.get(
                    LuceneDocumentIndexService.STAT_NAME_METADATA_INDEXING_UPDATE_COUNT
                            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
            if (stat == null) {
                return false;
            }

            return (stat.accumulatedValue == this.updateCount * this.serviceCount);
        });

        QueryTask queryTask = QueryTask.Builder.create()
                .setQuery(QueryTask.Query.Builder.create()
                        .addKindFieldClause(ExampleServiceState.class)
                        .build())
                .addOption(QueryOption.INDEXED_METADATA)
                .setResultLimit((int) this.serviceCount)
                .build();

        this.host.createQueryTaskService(queryTask, false, true, queryTask, null);
        assertNotNull(queryTask.results.nextPageLink);

        QueryTask result = this.host.getServiceState(null, QueryTask.class,
                UriUtils.buildUri(this.host, queryTask.results.nextPageLink));
        assertEquals(this.serviceCount, (long) result.results.documentCount);
        assertNull(result.results.nextPageLink);

        Map<String, ServiceStat> indexStats = this.host.getServiceStats(
                this.host.getDocumentIndexServiceUri());
        ServiceStat iterationCountStat = indexStats.get(
                LuceneDocumentIndexService.STAT_NAME_ITERATIONS_PER_QUERY);
        assertEquals(1.0, iterationCountStat.latestValue, 0.01);

        for (URI serviceUri : services.keySet()) {
            Operation delete = Operation.createDelete(serviceUri);
            this.host.send(delete);
        }

        this.host.waitFor("Metadata deletion failed to occur", () -> {
            Map<String, ServiceStat> stats = this.host.getServiceStats(
                    this.host.getDocumentIndexServiceUri());
            ServiceStat stat = stats.get(
                    LuceneDocumentIndexService.STAT_NAME_METADATA_INDEXING_UPDATE_COUNT
                            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
            if (stat == null) {
                return false;
            }

            // Account for the initial POST and the final DELETE.
            return (stat.accumulatedValue == (2 + this.updateCount) * this.serviceCount);
        });
    }

    @Test
    public void testMetadataIndexingWithForcedIndexUpdate() throws Throwable {
        for (int i = 0; i < this.iterationCount; i++) {
            tearDown();
            doMetadataIndexingWithForcedIndexUpdate();
        }
    }

    private void doMetadataIndexingWithForcedIndexUpdate() throws Throwable {
        setUpHost(false);
        URI indexedMetadataFactoryUri = createIndexedMetadataFactoryService(this.host);

        ExampleServiceState serviceState = new ExampleServiceState();
        serviceState.name = "name";

        Map<URI, ExampleServiceState> services = this.host.doFactoryChildServiceStart(null,
                this.serviceCount, ExampleServiceState.class,
                (o) -> o.setBody(serviceState),
                indexedMetadataFactoryUri);

        TestContext ctx = this.host.testCreate(services.size() * this.updateCount);
        for (int i = 0; i < this.updateCount; i++) {
            serviceState.counter = (long) i;
            for (URI serviceUri : services.keySet()) {
                Operation patch = Operation.createPatch(serviceUri).setBody(serviceState)
                        .setCompletion(ctx.getCompletion());
                this.host.send(patch);
            }
        }
        this.host.testWait(ctx);

        ctx = this.host.testCreate(services.size());
        for (URI serviceUri : services.keySet()) {
            Operation delete = Operation.createDelete(serviceUri)
                    .setCompletion(ctx.getCompletion());
            this.host.send(delete);
        }
        this.host.testWait(ctx);

        serviceState.name = "nameWithForcedIndexUpdate";

        ctx = this.host.testCreate(services.size());
        for (ExampleServiceState state : services.values()) {
            serviceState.documentSelfLink = state.documentSelfLink;
            Operation post = Operation.createPost(indexedMetadataFactoryUri)
                    .setBody(serviceState)
                    .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)
                    .setCompletion(ctx.getCompletion());
            this.host.send(post);
        }
        this.host.testWait(ctx);

        QueryTask queryTask = QueryTask.Builder.createDirectTask()
                .setQuery(Query.Builder.create()
                        .addKindFieldClause(ExampleServiceState.class)
                        .build())
                .addOption(QueryOption.EXPAND_CONTENT)
                .build();

        this.host.createQueryTaskService(queryTask, false, true, queryTask, null);
        assertEquals(services.size(), queryTask.results.documentLinks.size());

        for (Entry<String, Object> entry : queryTask.results.documents.entrySet()) {
            URI selfUri = UriUtils.buildUri(this.host, entry.getKey());
            assertTrue(services.containsKey(selfUri));
            ExampleServiceState state = Utils.fromJson(entry.getValue(),
                    ExampleServiceState.class);
            assertEquals("nameWithForcedIndexUpdate", state.name);
        }
    }
}
