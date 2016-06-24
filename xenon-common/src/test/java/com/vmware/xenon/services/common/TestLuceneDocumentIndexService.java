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

import static javax.xml.bind.DatatypeConverter.printBase64Binary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;
import javax.xml.bind.DatatypeConverter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.BasicReportTestCase;
import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.FileUtils;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ProcessingStage;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceConfigUpdateRequest;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestProperty;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask.Query.Occurance;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;

class FaultInjectionLuceneDocumentIndexService extends LuceneDocumentIndexService {
    /*
     * Called by test code to abruptly close the index writer simulating spurious
     * self close of the index writer, in production environments, due to out of memory
     * or other recoverable failures
     */
    public void closeWriter() {
        try {
            this.logWarning("Closing writer abruptly to induce failure");
            int permits = QUERY_THREAD_COUNT + UPDATE_THREAD_COUNT;
            this.writerAvailable.acquire(permits);
            super.writer.commit();
            super.writer.close();
            this.writerAvailable.release(permits);
        } catch (Throwable e) {
        }
    }

}

public class TestLuceneDocumentIndexService extends BasicReportTestCase {

    public static class OnDemandLoadService extends StatefulService {

        public static final int MAX_STATE_SIZE = 1024 * 1024;

        public OnDemandLoadService() {
            super(ExampleServiceState.class);
            super.toggleOption(ServiceOption.ON_DEMAND_LOAD, true);
        }

        @Override
        public void handlePatch(Operation op) {
            ExampleServiceState state = getState(op);
            ExampleServiceState body = getBody(op);
            Utils.mergeWithState(getStateDescription(), state, body);
            state.keyValues = body.keyValues;
            op.complete();
        }

        @Override
        public ServiceDocument getDocumentTemplate() {
            ServiceDocument template = super.getDocumentTemplate();
            template.documentDescription.serializedStateSizeLimit = MAX_STATE_SIZE;
            return template;
        }
    }

    public static class OnDemandLoadFactoryService extends FactoryService {
        public static final String SELF_LINK = "test/on-demand-load-services";

        public OnDemandLoadFactoryService() {
            super(ExampleServiceState.class);
            super.toggleOption(ServiceOption.REPLICATION, true);
            super.toggleOption(ServiceOption.ON_DEMAND_LOAD, true);
        }

        private EnumSet<ServiceOption> childServiceCaps;

        /**
         * Test use only.
         */
        public void setChildServiceCaps(EnumSet<ServiceOption> caps) {
            this.childServiceCaps = caps;
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            Service s = new OnDemandLoadService();
            if (this.childServiceCaps != null) {
                for (ServiceOption c : this.childServiceCaps) {
                    s.toggleOption(c, true);
                }
            }

            return s;
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
     * Parameter that specifies long running test duration in seconds
     */
    public long testDurationSeconds;

    private final String EXAMPLES_BODIES_FILE = "example_bodies.json";
    private final String INDEX_DIR_NAME = "lucene510";

    private FaultInjectionLuceneDocumentIndexService indexService;

    private int expiredDocumentSearchThreshold;

    @Before
    public void setup() {
        this.expiredDocumentSearchThreshold = LuceneDocumentIndexService
                .getExpiredDocumentSearchThreshold();
    }

    @After
    public void tearDown() throws Throwable {
        LuceneDocumentIndexService
                .setExpiredDocumentSearchThreshold(this.expiredDocumentSearchThreshold);
    }

    @Override
    public void beforeHostStart(VerificationHost host) {
        host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(100));
        this.indexService = new FaultInjectionLuceneDocumentIndexService();
        this.indexService.toggleOption(ServiceOption.INSTRUMENTATION, true);
        host.setDocumentIndexingService(this.indexService);
        host.setPeerSynchronizationEnabled(false);
    }

    @Test
    public void corruptedIndexRecovery() throws Throwable {
        this.doDurableServiceUpdate(Action.PUT, 100, 2, null);
        Thread.sleep(this.host.getMaintenanceIntervalMicros() / 1000);

        // Stop the host, without cleaning up storage.
        this.host.stop();
        this.host.setPort(0);

        corruptLuceneIndexFiles();

        try {
            // Restart host with the same storage sandbox. If host does not throw, we are good.
            this.indexService.toggleOption(ServiceOption.INSTRUMENTATION, true);
            this.host.start();
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

        final int expectedDirectoryPathsWithLuceneInName = 4;
        assertEquals(expectedDirectoryPathsWithLuceneInName, total);
    }

    @Test
    public void corruptIndexWhileRunning() throws Throwable {
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
    public void serviceHostRestartWithDurableServices() throws Throwable {
        ExampleServiceHost h = new ExampleServiceHost();
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

            // create on demand load services
            String factoryLink = createOnDemandLoadFactoryService(h);
            createOnDemandLoadServices(h, factoryLink);

            this.host.testStart(1);
            h.registerForServiceAvailability((o, e) -> {
                this.host.completeIteration();
            } , ExampleService.FACTORY_LINK);
            this.host.testWait();

            this.host.toggleServiceOptions(UriUtils.buildUri(h, ExampleService.FACTORY_LINK),
                    EnumSet.of(ServiceOption.IDEMPOTENT_POST), null);

            ServiceHostState initialState = h.getState();

            ExampleServiceState body = new ExampleServiceState();
            body.name = UUID.randomUUID().toString();
            List<URI> exampleURIs = new ArrayList<>();

            // create example services
            this.host.createExampleServices(h, this.serviceCount, exampleURIs, null);

            verifyCreateStatCount(exampleURIs, 1.0);

            int vc = 2;
            this.host.testStart(exampleURIs.size() * vc);
            for (int i = 0; i < vc; i++) {
                for (URI u : exampleURIs) {
                    this.host.send(Operation.createPut(u).setBody(body)
                            .setCompletion(this.host.getCompletion()));
                }
            }
            this.host.testWait();

            verifyDeleteRePost(h, exampleURIs);

            Map<URI, ExampleServiceState> beforeState = this.host.getServiceState(null,
                    ExampleServiceState.class, exampleURIs);

            verifyChildServiceCountByOptionQuery(h, beforeState);

            // stop the host, create new one
            h.stop();

            h = new ExampleServiceHost();
            args.port = 0;
            h.initialize(args);

            if (!this.host.isStressTest()) {
                h.setServiceStateCaching(false);
                // set the index service memory use to be very low to cause pruning of any cached entries
                h.setServiceMemoryLimit(ServiceUriPaths.CORE_DOCUMENT_INDEX, 0.0001);
                h.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(100));
            }

            long start = Utils.getNowMicrosUtc();

            if (!VerificationHost.restartStatefulHost(h)) {
                this.host.log("Failed restart of host, aborting");
                return;
            }

            this.host.toggleServiceOptions(h.getDocumentIndexServiceUri(),
                    EnumSet.of(ServiceOption.INSTRUMENTATION),
                    null);

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
                // if we had re-indexed all state on restart, the field update count would be approximately
                // the number of example services times their field count. We require less than that to catch
                // re-indexing that might occur before instrumentation is enabled in the index service
                assertTrue(
                        afterRestartIndexedFieldCountStat.latestValue < (this.serviceCount
                                * fieldCountPerService) / 2);
            }

            beforeState = updateUriMapWithNewPort(h.getPort(), beforeState);
            List<URI> updatedExampleUris = new ArrayList<>();
            for (URI u : exampleURIs) {
                updatedExampleUris.add(UriUtils.updateUriPort(u, h.getPort()));
            }
            exampleURIs = updatedExampleUris;

            ServiceHostState stateAfterRestart = h.getState();

            assertTrue(initialState.id.equals(stateAfterRestart.id));

            String onDemandFactoryLink = createOnDemandLoadFactoryService(h);

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

            long end = Utils.getNowMicrosUtc();

            this.host.log("Example Factory available %d micros after host start", end - start);

            verifyCreateStatCount(exampleURIs, 0.0);

            verifyOnDemandLoad(h, onDemandFactoryLink);

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

            this.host.testStart(beforeState.size());
            // issue some updates to force creation of link update time entries
            for (URI u : beforeState.keySet()) {
                Operation put = Operation.createPut(u)
                        .setCompletion(this.host.getCompletion())
                        .setBody(body);
                this.host.send(put);
            }
            this.host.testWait();

            verifyChildServiceCountByOptionQuery(h, afterState);

            // issue some additional updates, per service, to verify that having clear self link info entries is OK
            this.host.testStart(exampleURIs.size() * vc);
            for (int i = 0; i < vc; i++) {
                for (URI u : exampleURIs) {
                    this.host.send(Operation.createPut(u).setBody(body)
                            .setCompletion(this.host.getCompletion()));
                }
            }
            this.host.testWait();

            verifyFactoryStartedAndSynchronizedAfterNodeSynch(h, statName);

        } finally {
            h.stop();
            tmpFolder.delete();
        }
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

    private void verifyOnDemandLoad(ServiceHost h, String onDemandFactoryLink) throws Throwable {
        this.host.log("******************************* entered *******************************");
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

        // delete some of the services, not using a body, emulation DELETE through expiration
        URI serviceToDelete = childUris.remove(0);
        Operation delete = Operation.createDelete(serviceToDelete)
                .setCompletion(this.host.getCompletion());
        this.host.sendAndWait(delete);

        // attempt to use service we just deleted, we should get failure
        ExampleServiceState st = new ExampleServiceState();
        st.name = Utils.getNowMicrosUtc() + "";

        // do a PATCH, expect 404
        Operation patch = Operation.createPatch(serviceToDelete)
                .setBody(st)
                .setCompletion(
                        this.host.getExpectedFailureCompletion(Operation.STATUS_CODE_NOT_FOUND));
        this.host.sendAndWait(patch);

        // do a GET, expect 404
        Operation get = Operation.createGet(serviceToDelete)
                .setCompletion(
                        this.host.getExpectedFailureCompletion(Operation.STATUS_CODE_NOT_FOUND));
        this.host.sendAndWait(get);

        // do a PUT, expect 404
        Operation put = Operation.createGet(serviceToDelete)
                .setBody(st)
                .setCompletion(
                        this.host.getExpectedFailureCompletion(Operation.STATUS_CODE_NOT_FOUND));
        this.host.sendAndWait(put);

        // do a POST, expect 409
        Operation post = Operation.createPost(serviceToDelete)
                .setCompletion(
                        this.host.getExpectedFailureCompletion(Operation.STATUS_CODE_CONFLICT));
        this.host.sendAndWait(post);

        // do a DELETE again, expect no failure
        delete = Operation.createDelete(serviceToDelete)
                .setCompletion(this.host.getCompletion());
        this.host.sendAndWait(delete);

        // do a DELETE for a completely unknown service, expect 200
        delete = Operation.createDelete(new URI(factoryUri.toString() + "/unknown"))
                .setCompletion(this.host.getCompletion());
        this.host.sendAndWait(delete);

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
            post = Operation.createPost(factoryUri)
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
        body.documentExpirationTimeMicros = Utils.getNowMicrosUtc() + TimeUnit.SECONDS.toMicros(2);
        patch = Operation.createPatch(serviceToDelete)
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
                this.host.log("%s %s", u.getPath(), stg);
                if (stg != null) {
                    return false;
                }
            }
            return true;
        });


        this.host.log("******************************* finished *******************************");
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

    private String createOnDemandLoadFactoryService(ServiceHost h) throws Throwable {
        // create an on demand load factory and services
        this.host.testStart(1);
        OnDemandLoadFactoryService s = new OnDemandLoadFactoryService();
        s.setChildServiceCaps(EnumSet.of(ServiceOption.PERSISTENCE,
                ServiceOption.REPLICATION, ServiceOption.OWNER_SELECTION,
                ServiceOption.INSTRUMENTATION));
        Operation factoryPost = Operation.createPost(
                UriUtils.buildUri(h, s.getClass()))
                .setCompletion(this.host.getCompletion());
        h.startService(factoryPost, s);
        this.host.testWait();
        String factoryLink = s.getSelfLink();
        this.host.scheduleNodeGroupChangeMaintenance(ServiceUriPaths.DEFAULT_NODE_SELECTOR);
        this.host.log("Started on demand load factory at %s", factoryLink);
        return factoryLink;
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
                        if (u.contains(ExampleService.FACTORY_LINK)) {
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
    public void interleavedUpdatesWithQueries() throws Throwable {
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
        long endTime = Utils.getNowMicrosUtc() + TimeUnit.SECONDS.toMicros(1);
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

            for (URI u : services.keySet()) {
                ExampleServiceState s = new ExampleServiceState();
                s.name = initialServiceNameValue;
                s.counter = Utils.getNowMicrosUtc();
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
        } while (endTime > Utils.getNowMicrosUtc());

        Date exp = this.host.getTestExpiration();
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
        this.host.doExampleServiceUpdateAndQueryByVersion(this.host.getUri(),
                (int) this.serviceCount);
    }

    @Test
    public void patchLargeServiceState() throws Throwable {
        // create on demand load services
        String factoryLink = createOnDemandLoadFactoryService(this.host);
        createOnDemandLoadServices(this.host, factoryLink);
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

        TestContext ctx = testCreate(res.documentLinks.size());
        for (String link : res.documentLinks) {
            Operation patch = Operation.createPatch(this.host, link)
                    .setBody(patchBody)
                    .setCompletion(ctx.getCompletion());
            this.host.send(patch);
        }
        testWait(ctx);
    }


    @Test
    public void throughputPut() throws Throwable {
        doDurableServiceUpdate(Action.PUT, this.serviceCount, this.updateCount, null);
    }

    @Test
    public void putWithFailureAndCacheValidation() throws Throwable {
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
                            ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
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
    public void serviceCreationAndDocumentExpirationLongRunning() throws Throwable {
        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);

        LuceneDocumentIndexService.setExpiredDocumentSearchThreshold(2);

        Date expiration = this.host.getTestExpiration();

        long opTimeoutMicros = this.host.testDurationSeconds != 0 ? this.host
                .getOperationTimeoutMicros() * 4
                : this.host.getOperationTimeoutMicros();

        this.host.setTimeoutSeconds((int) TimeUnit.MICROSECONDS.toSeconds(opTimeoutMicros));

        URI factoryUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        Consumer<Operation> setBody = (o) -> {
            ExampleServiceState body = new ExampleServiceState();
            body.name = UUID.randomUUID().toString();
            o.setBody(body);
        };
        Consumer<Operation> setBodyMinimal = (o) -> {
            MinimalTestServiceState body = (MinimalTestServiceState) this.host
                    .buildMinimalTestState();
            o.setBody(body);
        };

        String minimalSelfLinkPrefix = "minimal";
        Service minimalFactory = this.host.startServiceAndWait(
                new MinimalFactoryTestService(), minimalSelfLinkPrefix, new ServiceDocument());

        while (new Date().before(expiration)) {

            this.host.log("Expiration: %s, now: %s", expiration, new Date());

            Map<URI, ExampleServiceState> services = this.host.doFactoryChildServiceStart(
                    null,
                    this.serviceCount, ExampleServiceState.class,
                    setBody, factoryUri);

            Set<String> names = new HashSet<>();
            this.host.testStart(services.size());
            // patch services to a new version so we verify expiration across multiple versions
            for (URI u : services.keySet()) {
                ExampleServiceState s = new ExampleServiceState();
                s.name = UUID.randomUUID().toString();
                // set a very long expiration
                s.documentExpirationTimeMicros = Utils.getNowMicrosUtc()
                        + TimeUnit.DAYS.toMicros(1);
                names.add(s.name);
                this.host.send(Operation.createPatch(u).setBody(s)
                        .setCompletion(this.host.getCompletion()));
            }
            this.host.testWait();

            // verify state was saved by issuing a factory GET which goes to the index
            Map<URI, ExampleServiceState> states = this.host.getServiceState(null,
                    ExampleServiceState.class, services.keySet());
            for (ExampleServiceState st : states.values()) {
                assertTrue(names.contains(st.name));
            }

            URI luceneStatsUri = UriUtils.buildStatsUri(this.host.getDocumentIndexServiceUri());
            ServiceStats stats = this.host.getServiceState(null, ServiceStats.class,
                    luceneStatsUri);
            ServiceStat deletedCountBeforeExpiration = stats.entries
                    .get(LuceneDocumentIndexService.STAT_NAME_SERVICE_DELETE_COUNT);
            if (deletedCountBeforeExpiration == null) {
                deletedCountBeforeExpiration = new ServiceStat();
            }

            stats = this.host.getServiceState(null, ServiceStats.class, luceneStatsUri);
            ServiceStat expiredCountBeforeExpiration = stats.entries
                    .get(LuceneDocumentIndexService.STAT_NAME_DOCUMENT_EXPIRATION_COUNT);

            if (expiredCountBeforeExpiration == null) {
                expiredCountBeforeExpiration = new ServiceStat();
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

            ServiceStat expiredCountAfterExpiration = null;
            Date exp = this.host.getTestExpiration();
            while (exp.after(new Date())) {
                boolean isConverged = true;
                // confirm services are stopped
                for (URI u : services.keySet()) {
                    ProcessingStage s = this.host.getServiceStage(u.getPath());
                    if (s != null && s != ProcessingStage.STOPPED) {
                        isConverged = false;
                    }
                }

                if (!isConverged) {
                    Thread.sleep(250);
                    continue;
                }

                stats = this.host.getServiceState(null, ServiceStats.class,
                        luceneStatsUri);
                ServiceStat deletedCountAfterExpiration = stats.entries
                        .get(LuceneDocumentIndexService.STAT_NAME_SERVICE_DELETE_COUNT);

                ServiceStat expiredDocumentForcedMaintenanceCount = stats.entries
                        .get(LuceneDocumentIndexService.STAT_NAME_DOCUMENT_EXPIRATION_FORCED_MAINTENANCE_COUNT);

                // in batch expiry mode wait till at least first batch completes
                if (services.size() > LuceneDocumentIndexService.getExpiredDocumentSearchThreshold()
                        && (expiredDocumentForcedMaintenanceCount == null
                        || expiredDocumentForcedMaintenanceCount.latestValue < 2)) {
                    Thread.sleep(250);
                    continue;
                }

                if (deletedCountAfterExpiration == null) {
                    Thread.sleep(250);
                    continue;
                }

                if (deletedCountBeforeExpiration.latestValue >= deletedCountAfterExpiration.latestValue) {
                    this.host.log("No service deletions seen, currently at %f",
                            deletedCountAfterExpiration.latestValue);
                    Thread.sleep(250);
                    continue;
                }

                stats = this.host.getServiceState(null, ServiceStats.class, luceneStatsUri);
                expiredCountAfterExpiration = stats.entries
                        .get(LuceneDocumentIndexService.STAT_NAME_DOCUMENT_EXPIRATION_COUNT);

                if (expiredCountBeforeExpiration.latestValue >= expiredCountAfterExpiration.latestValue) {
                    this.host.log("No service expirations seen, currently at %f",
                            expiredCountAfterExpiration.latestValue);
                    Thread.sleep(250);
                    continue;
                }

                break;
            }

            if (exp.before(new Date())) {
                throw new TimeoutException();
            }

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

            File f = new File(this.host.getStorageSandbox());
            this.host.log("Disk: free %d, usable: %d, total: %d", f.getFreeSpace(),
                    f.getUsableSpace(),
                    f.getTotalSpace());

            this.host.log("Memory: free %d, total: %d, max: %d", Runtime.getRuntime()
                    .freeMemory(),
                    Runtime.getRuntime().totalMemory(),
                    Runtime.getRuntime().maxMemory());

            stats = this.host.getServiceState(null, ServiceStats.class,
                    luceneStatsUri);
            ServiceStat stAll = stats.entries
                    .get(LuceneDocumentIndexService.STAT_NAME_INDEXED_DOCUMENT_COUNT);
            if (stAll != null) {
                this.host.log("total versions: %f", stAll.latestValue);
            }

            Consumer<Operation> maintExpSetBody = (o) -> {
                ExampleServiceState body = new ExampleServiceState();
                body.name = UUID.randomUUID().toString();
                body.documentExpirationTimeMicros = Utils.getNowMicrosUtc()
                        + this.host.getMaintenanceIntervalMicros();
                o.setBody(body);
            };

            // create a new set of services, meant to expire on their own, quickly
            services = this.host.doFactoryChildServiceStart(
                    null,
                    this.serviceCount, ExampleServiceState.class,
                    maintExpSetBody, factoryUri);

            // do not do anything on the services, rely on the maintenance interval to expire them
            exp = this.host.getTestExpiration();
            while (exp.after(new Date())) {
                stats = this.host.getServiceState(null, ServiceStats.class, luceneStatsUri);
                ServiceStat maintExpiredCount = stats.entries
                        .get(LuceneDocumentIndexService.STAT_NAME_DOCUMENT_EXPIRATION_COUNT);

                if (expiredCountAfterExpiration.latestValue >= maintExpiredCount.latestValue) {
                    Thread.sleep(this.host.getMaintenanceIntervalMicros() / 1000);
                    continue;
                }

                ServiceDocumentQueryResult r = this.host.getFactoryState(factoryUri);
                if (r.documentLinks.size() > 0) {
                    this.host.log("Documents not expired: %d", r.documentLinks.size());
                    Thread.sleep(this.host.getMaintenanceIntervalMicros() / 1000);
                    continue;
                }

                break;
            }

            this.host.log("Documents expired through maintenance");

            if (new Date().after(exp)) {
                throw new IllegalStateException(
                        "Lucene service maintenanance never expired services");
            }

            if (this.host.isLongDurationTest()) {
                Thread.sleep(1000);
            } else {
                break;
            }

            ServiceDocumentQueryResult r = this.host.getFactoryState(factoryUri);
            ServiceHostState s = this.host.getState();
            this.host.log("number of documents: %d, host state: %s", r.documentLinks.size(),
                    Utils.toJsonHtml(s));

            assertEquals(0, r.documentLinks.size());
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
        boolean sendDelete = expTime != 0 && expTime < Utils.getNowMicrosUtc();
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
        ServiceDocumentQueryResult rsp = null;

        long start = Utils.getNowMicrosUtc();
        while (Utils.getNowMicrosUtc() - start < this.host.getOperationTimeoutMicros()) {
            int actualCount = 0;
            rsp = this.host.getFactoryState(factoryUri);
            for (String link : rsp.documentLinks) {
                ProcessingStage ps = this.host.getServiceStage(link);
                if (ps != ProcessingStage.AVAILABLE) {
                    continue;
                }
                actualCount++;
            }

            this.host.log("Expected example service count: %d, current: %d", expectedCount,
                    actualCount);

            if (actualCount == expectedCount && rsp.documentLinks.size() == expectedCount) {
                break;
            }

            Thread.sleep(100);
        }

        if (rsp.documentLinks.size() == expectedCount) {
            return;
        }

        throw new IllegalArgumentException("Services not expired:" + Utils.toJsonHtml(rsp));
    }

    @Test
    public void serviceVersionRetentionAndGrooming() throws Throwable {
        EnumSet<ServiceOption> caps = EnumSet.of(ServiceOption.PERSISTENCE);
        doServiceVersionGroomingValidation(caps);
    }

    @Test
    public void testBackupAndRestoreFromZipFile() throws Throwable {
        LuceneDocumentIndexService.BackupRequest b = new LuceneDocumentIndexService.BackupRequest();
        b.documentKind = LuceneDocumentIndexService.BackupRequest.KIND;

        int count = 1000;
        URI factoryUri = UriUtils.buildUri(this.host,
                ExampleService.FACTORY_LINK);

        Map<URI, ExampleServiceState> exampleStates = this.host.doFactoryChildServiceStart(null,
                count,
                ExampleServiceState.class,
                (o) -> {
                    ExampleServiceState s = new ExampleServiceState();
                    s.name = UUID.randomUUID().toString();
                    o.setBody(s);
                }, factoryUri);

        final URI[] backupFile = { null };
        this.host.testStart(1);
        this.host
                .send(Operation
                        .createPatch(
                                UriUtils.buildUri(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX))
                        .setBody(b)
                        .setCompletion(
                                (o, e) -> {
                                    if (e != null) {
                                        this.host.failIteration(e);
                                        return;
                                    }

                                    LuceneDocumentIndexService.BackupRequest rsp = o
                                            .getBody(LuceneDocumentIndexService.BackupRequest
                                            .class);
                                    backupFile[0] = rsp.backupFile;
                                    if (rsp.backupFile == null) {
                                        this.host.failIteration(new IllegalStateException(
                                                "no backup file"));
                                    }
                                    File f = new File(rsp.backupFile);

                                    if (!f.isFile()) {
                                        this.host.failIteration(new IllegalArgumentException(
                                                "not file"));
                                    }
                                    this.host.completeIteration();
                                }));
        this.host.testWait();

        LuceneDocumentIndexService.RestoreRequest r = new LuceneDocumentIndexService.RestoreRequest();
        r.documentKind = LuceneDocumentIndexService.RestoreRequest.KIND;
        r.backupFile = backupFile[0];

        this.host.testStart(1);
        this.host.send(Operation
                .createPatch(UriUtils.buildUri(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX))
                .setBody(r)
                .setCompletion(this.host.getCompletion()));
        this.host.testWait();

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

    public static class MinimalTestServiceWithDefaultRetention extends StatefulService {
        public MinimalTestServiceWithDefaultRetention() {
            super(MinimalTestServiceState.class);
        }
    }

    private void doServiceVersionGroomingValidation(EnumSet<ServiceOption> caps) throws Throwable {
        long end = Utils.getNowMicrosUtc() + TimeUnit.SECONDS.toMicros(this.testDurationSeconds);
        final long offset = 10;

        do {
            List<Service> services = this.host.doThroughputServiceStart(
                    this.serviceCount, MinimalTestServiceWithDefaultRetention.class,
                    this.host.buildMinimalTestState(), caps,
                    null);

            Collection<URI> serviceUrisWithDefaultRetention = new ArrayList<>();
            for (Service s : services) {
                serviceUrisWithDefaultRetention.add(s.getUri());
            }

            URI factoryUri = UriUtils.buildUri(this.host,
                    ExampleService.FACTORY_LINK);
            Map<URI, ExampleServiceState> exampleStates = this.host.doFactoryChildServiceStart(
                    null,
                    this.serviceCount,
                    ExampleServiceState.class,
                    (o) -> {
                        ExampleServiceState s = new ExampleServiceState();
                        s.name = UUID.randomUUID().toString();
                        o.setBody(s);
                    }, factoryUri);

            Collection<URI> serviceUrisWithCustomRetention = exampleStates.keySet();
            long count = ServiceDocumentDescription.DEFAULT_VERSION_RETENTION_LIMIT + offset;
            this.host.testStart(this.serviceCount * count);
            for (int i = 0; i < count; i++) {
                for (URI u : serviceUrisWithDefaultRetention) {
                    this.host.send(Operation.createPut(u)
                            .setBody(this.host.buildMinimalTestState())
                            .setCompletion(this.host.getCompletion()));
                }
            }
            this.host.testWait();
            count = ExampleServiceState.VERSION_RETENTION_LIMIT + offset;
            this.host.testStart(serviceUrisWithCustomRetention.size() * count);
            for (int i = 0; i < count; i++) {
                for (URI u : serviceUrisWithCustomRetention) {
                    ExampleServiceState st = new ExampleServiceState();
                    st.name = Utils.getNowMicrosUtc() + "";
                    this.host.send(Operation.createPut(u)
                            .setBody(st)
                            .setCompletion(this.host.getCompletion()));
                }
            }
            this.host.testWait();

            Collection<URI> serviceUris = serviceUrisWithDefaultRetention;
            verifyVersionRetention(serviceUris, ServiceDocumentDescription.DEFAULT_VERSION_RETENTION_LIMIT);

            serviceUris = serviceUrisWithCustomRetention;
            verifyVersionRetention(serviceUris, ExampleServiceState.VERSION_RETENTION_LIMIT);

            this.host.testStart(this.serviceCount);
            for (URI u : serviceUrisWithDefaultRetention) {
                this.host.send(Operation.createDelete(u)
                        .setCompletion(this.host.getCompletion()));
            }
            this.host.testWait();

            this.host.testStart(this.serviceCount);
            for (URI u : serviceUrisWithCustomRetention) {
                this.host.send(Operation.createDelete(u)
                        .setCompletion(this.host.getCompletion()));
            }
            this.host.testWait();
        } while (Utils.getNowMicrosUtc() < end);
    }

    private void verifyVersionRetention(
            Collection<URI> serviceUris, long limit) throws Throwable {

        long maintIntervalMillis = TimeUnit.MICROSECONDS
                .toMillis(this.host.getMaintenanceIntervalMicros());

        // let a couple of maintenance intervals pass. not essential, since we loop below
        // but lets more documents get deleted at once
        Thread.sleep(maintIntervalMillis);

        QueryTask finishedTaskWithLinksState = null;
        // issue a query that verifies we have *less* than the count versions
        Date exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {
            QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
            q.options = EnumSet.of(QueryOption.COUNT, QueryOption.INCLUDE_ALL_VERSIONS);
            for (URI u : serviceUris) {
                QueryTask.Query linkClause = new QueryTask.Query();
                linkClause.setTermPropertyName(ServiceDocument.FIELD_NAME_SELF_LINK)
                        .setTermMatchValue(u.getPath());
                linkClause.occurance = Occurance.SHOULD_OCCUR;
                q.query.addBooleanClause(linkClause);
            }
            URI u = this.host.createQueryTaskService(QueryTask.create(q), false);
            QueryTask finishedTaskState = this.host.waitForQueryTaskCompletion(q,
                    serviceUris.size(), (int) limit, u, false, true);
            // also do a query that returns the actual links
            q.options = EnumSet.of(QueryOption.INCLUDE_ALL_VERSIONS);
            u = this.host.createQueryTaskService(QueryTask.create(q), false);
            finishedTaskWithLinksState = this.host.waitForQueryTaskCompletion(q,
                    serviceUris.size(), (int) limit, u, false, true);

            long expectedCount = serviceUris.size() * limit;
            this.host.log("Documents found through count:%d, links:%d expectedCount:%d",
                    finishedTaskState.results.documentCount,
                    finishedTaskWithLinksState.results.documentLinks.size(),
                    expectedCount);

            if (finishedTaskState.results.documentCount != finishedTaskWithLinksState.results.documentLinks
                    .size()) {
                Thread.sleep(maintIntervalMillis);
                continue;
            }
            if (finishedTaskState.results.documentCount != expectedCount) {
                Thread.sleep(maintIntervalMillis);
                continue;
            }
            return;
        }

        // Verification failed. Logging all self-links that returned
        // more document versions than expected
        if (finishedTaskWithLinksState != null) {
            HashMap<String, TreeSet<Integer>> aggregated = new HashMap<>();
            for (String link : finishedTaskWithLinksState.results.documentLinks) {
                String documentSelfLink = link.split("\\?")[0];
                TreeSet<Integer> versions = aggregated.get(documentSelfLink);
                if (versions == null) {
                    versions = new TreeSet<>();
                }
                versions.add(Integer.parseInt(link.split("=")[1]));
                aggregated.put(documentSelfLink, versions);
            }
            aggregated.entrySet().stream().filter(aggregate -> aggregate.getValue().size() > limit)
                    .forEach(aggregate -> {
                        String documentSelfLink = aggregate.getKey();
                        Integer lowerVersion = aggregate.getValue().first();
                        Integer upperVersion = aggregate.getValue().last();
                        this.host.log("Failed documentSelfLink:%s. lowerVersion:%d, upperVersion:%d, count:%d",
                                documentSelfLink, lowerVersion, upperVersion, aggregate.getValue().size());
                    });
        }

        throw new TimeoutException();
    }

    private void doDurableServiceUpdate(Action action, long serviceCount,
            Integer putCount,
            EnumSet<ServiceOption> caps) throws Throwable {
        EnumSet<TestProperty> props = EnumSet.noneOf(TestProperty.class);

        this.indexService.toggleOption(ServiceOption.INSTRUMENTATION, false);

        if (caps == null) {
            caps = EnumSet.of(ServiceOption.PERSISTENCE);
            props.add(TestProperty.PERSISTED);
        }
        if (putCount != null && putCount == 1) {
            props.add(TestProperty.SINGLE_ITERATION);
        }

        List<Service> services = this.host.doThroughputServiceStart(
                serviceCount, MinimalTestService.class, this.host.buildMinimalTestState(), caps,
                null);

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

        int repeat = 5;
        for (int i = 0; i < repeat; i++) {
            this.host.doServiceUpdates(action, count, props, services);
        }

        // decrease maintenance, which will trigger cache clears
        this.host.setMaintenanceIntervalMicros(250000);
        Thread.sleep(500);

        Map<URI, MinimalTestServiceState> statesBeforeRestart = this.host.getServiceState(null,
                MinimalTestServiceState.class, services);
        int mismatchCount = 0;
        for (MinimalTestServiceState st : statesBeforeRestart.values()) {
            if (st.documentVersion != count * repeat) {
                this.host.log("Version mismatch for %s. Expected %d, got %d", st.documentSelfLink,
                        count * repeat, st.documentVersion);
                mismatchCount++;
            }
        }
        assertTrue(mismatchCount == 0);
    }

    /**
     * Test Lucene index upgrade to Version.CURRENT.  On host start, the index should
     * be upgraded in place.  We've embedded an old index with a example service documents.
     * Verify the fields are still valid.
     */
    @Test
    public void indexUpgrade() throws Throwable {
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

    public static HashMap<String, ExampleServiceState> queryResultToExampleState(
            ServiceDocumentQueryResult r) {
        HashMap<String, ExampleServiceState> state = new HashMap<>();
        for (String k : r.documents.keySet()) {
            state.put(k, Utils.fromJson(r.documents.get(k), ExampleServiceState.class));
        }
        return state;
    }
}
