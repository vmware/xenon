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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import static com.vmware.xenon.services.common.ServiceHostManagementService.STAT_NAME_THREAD_COUNT;

import java.io.File;
import java.net.URI;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.FileUtils;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestProperty;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.TestRequestSender.FailureResponse;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.LocalFileService.LocalFileServiceState;
import com.vmware.xenon.services.common.ServiceHostManagementService.BackupType;

public class TestServiceHostManagementService extends BasicTestCase {

    public int serviceCount = 100;

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void getStateAndDelete() throws Throwable {
        URI u = UriUtils
                .buildUri(this.host, ServiceHostManagementService.class);
        ServiceHostState rsp = this.host.getServiceState(EnumSet.of(TestProperty.FORCE_REMOTE),
                ServiceHostState.class, u);
        this.host.updateSystemInfo(false);
        ServiceHostState localRsp = this.host.getState();
        Runtime r = Runtime.getRuntime();

        this.host.log("%s", Utils.toJsonHtml(localRsp));

        // we can't do equality checks for free and usable memory/disk counts since they change
        // between calls
        assertTrue(localRsp.systemInfo.freeMemoryByteCount > 10000);
        assertTrue(localRsp.systemInfo.maxMemoryByteCount > 10000);
        assertTrue(localRsp.systemInfo.totalMemoryByteCount > r.totalMemory() / 2);

        assertTrue(localRsp.systemInfo.totalDiskByteCount > 10000);
        assertTrue(localRsp.systemInfo.usableDiskByteCount > 10000);
        assertTrue(localRsp.systemInfo.freeDiskByteCount > 10000);

        assertTrue(rsp.httpPort == localRsp.httpPort);
        assertTrue(rsp.systemInfo.availableProcessorCount == localRsp.systemInfo.availableProcessorCount);
        // we have seen, once, the maxMemoryByteCount change, during test execution, so here simply
        // check the values are reasonable
        assertTrue(rsp.systemInfo.maxMemoryByteCount > localRsp.systemInfo.maxMemoryByteCount / 2);
        assertTrue(rsp.codeProperties != null);
        assertTrue(localRsp.codeProperties != null);
        assertTrue(rsp.codeProperties.size() > 4);
        String gitCommitId = null;
        String gitCommitTime = null;
        for (Entry<Object, Object> p : rsp.codeProperties.entrySet()) {
            String propKey = (String) p.getKey();
            String propValue = (String) p.getValue();
            assertTrue(propKey.startsWith(ServiceHost.GIT_COMMIT_SOURCE_PROPERTY_PREFIX));
            assertTrue(!propValue.isEmpty());
            if (propKey.equals(ServiceHost.GIT_COMMIT_SOURCE_PROPERTY_COMMIT_ID)) {
                gitCommitId = propValue;
            }
            if (propKey.equals(ServiceHost.GIT_COMMIT_SOURCE_PROPERTY_COMMIT_TIME)) {
                gitCommitTime = propValue;
            }
            this.host.log("Git prop %s:%s", propKey, propValue);
        }

        assertTrue(gitCommitId != null);
        assertTrue(gitCommitTime != null);

        // now issue a DELETE and verify host shutdown
        this.host.testStart(1);
        this.host.send(Operation.createDelete(u).setCompletion(this.host.getCompletion()));
        this.host.testWait();

        // the DELETE is completed with 201 BEFORE the host is shut down, to
        // avoid deadlocks
        // insert a small to make it extremely un likely the host is not marked
        // as shut down

        int retry = 0;
        while (this.host.isStarted() != false) {
            Thread.sleep(500);
            retry++;
            if (retry > this.host.getTimeoutSeconds() * 2) {
                throw new TimeoutException();
            }
        }
    }

    @Test
    public void testBackupAndRestoreFromRemoteHost() throws Throwable {
        testBackupAndRestore(100, 100);
        // the expected count includes the number of services created in the previous iteration.
        testBackupAndRestore(1001, 1101);
    }

    private void testBackupAndRestore(int serviceCount, int expectedCount) throws Throwable {

        File tmpFile = File.createTempFile("intermediate-file", ".zip", null);
        tmpFile.deleteOnExit();

        String backupServiceLink = LocalFileService.SERVICE_PREFIX + "/backup";

        createBackupFileService(tmpFile.toURI(), backupServiceLink);
        URI backupFileServiceUri = UriUtils.buildUri(this.host, backupServiceLink);

        // Post some documents to populate the index.
        Map<URI, ExampleServiceState> exampleStates = populateExampleServices(serviceCount);

        ServiceHostManagementService.BackupRequest backupRequest = new ServiceHostManagementService.BackupRequest();
        backupRequest.destination = backupFileServiceUri;
        backupRequest.kind = ServiceHostManagementService.BackupRequest.KIND;

        // trigger backup
        URI backupOpUri = UriUtils.buildUri(this.host, ServiceHostManagementService.SELF_LINK);
        Operation backupOp = Operation.createPatch(backupOpUri).setBody(backupRequest);
        this.host.getTestRequestSender().sendAndWait(backupOp);

        this.host.tearDown();

        this.host.log("backup file %s (bytes:%d md5:%s)", tmpFile.toString(), tmpFile.length(),
                FileUtils.md5sum(tmpFile));

        this.host = VerificationHost.create(0);
        this.host.start();

        String restoreServiceLink = LocalFileService.SERVICE_PREFIX + "/restore";
        createRestoreFileService(tmpFile.toURI(), restoreServiceLink);

        URI restoreFileServiceUri = UriUtils.buildUri(this.host, restoreServiceLink);

        ServiceHostManagementService.RestoreRequest restoreRequest = new ServiceHostManagementService.RestoreRequest();
        restoreRequest.destination = restoreFileServiceUri;
        restoreRequest.kind = ServiceHostManagementService.RestoreRequest.KIND;

        // perform restore
        URI restoreOpUri = UriUtils.buildUri(this.host, ServiceHostManagementService.SELF_LINK);
        Operation restoreOp = Operation.createPatch(restoreOpUri).setBody(restoreRequest);
        this.host.getTestRequestSender().sendAndWait(restoreOp);

        // Check our documents are still there
        ServiceDocumentQueryResult queryResult = this.host
                .getFactoryState(UriUtils.buildExpandLinksQueryUri(UriUtils.buildUri(this.host,
                        ExampleService.FACTORY_LINK)));
        assertNotNull(queryResult);
        assertNotNull(queryResult.documents);
        assertEquals(queryResult.documents.size(), expectedCount);

        HashMap<String, ExampleServiceState> out = TestLuceneDocumentIndexService
                .queryResultToExampleState(queryResult);

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
    public void testBackupAndRestoreWithLocalFile() throws Throwable {

        File tmpFile = this.tempDir.newFile("backup.zip");
        URI localFileUri = tmpFile.toURI();

        // Post some documents to populate the index.
        Map<URI, ExampleServiceState> exampleStates = populateExampleServices(this.serviceCount);

        // specify local file uri to the destination
        ServiceHostManagementService.BackupRequest backupRequest = new ServiceHostManagementService.BackupRequest();
        backupRequest.destination = localFileUri;
        backupRequest.kind = ServiceHostManagementService.BackupRequest.KIND;

        // trigger backup
        URI backupOpUri = UriUtils.buildUri(this.host, ServiceHostManagementService.SELF_LINK);
        Operation backupOp = Operation.createPatch(backupOpUri).setBody(backupRequest);
        this.host.getTestRequestSender().sendAndWait(backupOp);

        this.host.tearDown();

        this.host.log("backup file %s (bytes:%d md5:%s)",
                tmpFile.toString(), tmpFile.length(), FileUtils.md5sum(tmpFile));

        this.host = VerificationHost.create(0);
        this.host.start();

        // specify local file URI
        ServiceHostManagementService.RestoreRequest restoreRequest = new ServiceHostManagementService.RestoreRequest();
        restoreRequest.destination = localFileUri;
        restoreRequest.kind = ServiceHostManagementService.RestoreRequest.KIND;

        // perform restore
        URI restoreOpUri = UriUtils.buildUri(this.host, ServiceHostManagementService.SELF_LINK);
        Operation restoreOp = Operation.createPatch(restoreOpUri).setBody(restoreRequest);
        this.host.getTestRequestSender().sendAndWait(restoreOp);

        // Check our documents are still there
        ServiceDocumentQueryResult queryResult = this.host
                .getFactoryState(UriUtils.buildExpandLinksQueryUri(UriUtils.buildUri(this.host,
                        ExampleService.FACTORY_LINK)));
        assertNotNull(queryResult);
        assertNotNull(queryResult.documents);
        assertEquals(this.serviceCount, queryResult.documents.size());

        HashMap<String, ExampleServiceState> out = TestLuceneDocumentIndexService
                .queryResultToExampleState(queryResult);

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
    public void testBackupAndRestoreWithLocalDirectoryIncremental() throws Throwable {

        TestRequestSender sender = this.host.getTestRequestSender();

        File tmpDir = this.tempDir.newFolder("backup");
        URI localDirUri = tmpDir.toURI();

        // Post some documents to populate the index.
        List<Operation> ops = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = "foo-" + i;
            state.documentSelfLink = state.name;
            Operation post = Operation.createPost(this.host, ExampleService.FACTORY_LINK).setBody(state);
            ops.add(post);
        }
        List<ExampleServiceState> initialStates = sender.sendAndWait(ops, ExampleServiceState.class);


        // specify local dir to the destination
        ServiceHostManagementService.BackupRequest backupRequest = new ServiceHostManagementService.BackupRequest();
        backupRequest.destination = localDirUri;
        backupRequest.kind = ServiceHostManagementService.BackupRequest.KIND;
        backupRequest.backupType = BackupType.DIRECTORY;

        // trigger backup
        Operation backupOp = Operation.createPatch(this.host, ServiceHostManagementService.SELF_LINK).setBody(backupRequest);
        sender.sendAndWait(backupOp);

        // verify backup directory has populated
        String[] backupFiles = tmpDir.list();
        assertNotNull("backup directory must be populated.", backupFiles);
        assertTrue("backup directory must be populated.", backupFiles.length != 0);

        this.host.log("backup directory: %s (%d)", tmpDir.toString(), backupFiles.length);

        // destroy current host and spin up new host
        this.host.tearDown();
        this.host = VerificationHost.create(0);
        sender = this.host.getTestRequestSender();
        this.host.start();

        // restore request with directory
        ServiceHostManagementService.RestoreRequest restoreRequest = new ServiceHostManagementService.RestoreRequest();
        restoreRequest.destination = localDirUri;
        restoreRequest.kind = ServiceHostManagementService.RestoreRequest.KIND;

        // perform restore
        Operation restoreOp = Operation.createPatch(this.host, ServiceHostManagementService.SELF_LINK).setBody(restoreRequest);
        sender.sendAndWait(restoreOp);

        // restart
        restartHostAndWaitAvailable();
        sender = this.host.getTestRequestSender();


        // verify existence of initial data
        ops = initialStates.stream().map(state -> Operation.createGet(this.host, state.documentSelfLink)).collect(toList());
        sender.sendAndWait(ops);


        // delete first half of initial data
        ops = initialStates.subList(0, 10).stream().map(state -> Operation.createDelete(this.host, state.documentSelfLink)).collect(toList());
        sender.sendAndWait(ops);

        // update doc 10-15
        ops = initialStates.subList(10, 15).stream().map(state -> {
            ExampleServiceState newState = new ExampleServiceState();
            newState.name = state.name + "-patched";
            return Operation.createPatch(this.host, state.documentSelfLink).setBody(newState);
        }).collect(toList());
        sender.sendAndWait(ops);

        // create new set of data
        ops = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = "foo-new-" + i;
            state.documentSelfLink = state.name;
            Operation post = Operation.createPost(this.host, ExampleService.FACTORY_LINK).setBody(state);
            ops.add(post);
        }
        List<ExampleServiceState> newData = sender.sendAndWait(ops, ExampleServiceState.class);


        // trigger backup (incremental)
        backupOp = Operation.createPatch(this.host, ServiceHostManagementService.SELF_LINK).setBody(backupRequest);
        sender.sendAndWait(backupOp);

        // destroy current node and spin up new one
        this.host.tearDown();
        this.host = VerificationHost.create(0);
        sender = this.host.getTestRequestSender();
        this.host.start();

        // perform restore
        restoreOp = Operation.createPatch(this.host, ServiceHostManagementService.SELF_LINK).setBody(restoreRequest);
        sender.sendAndWait(restoreOp);

        restartHostAndWaitAvailable();
        sender = this.host.getTestRequestSender();

        // verify initial data: doc 0-9 deleted, 10-14 updated, 15-20 exists
        ops = initialStates.subList(0, 10).stream().map(state -> Operation.createGet(this.host, state.documentSelfLink)).collect(toList());
        for (Operation op : ops) {
            FailureResponse failureResponse = sender.sendAndWaitFailure(op);
            assertEquals(Operation.STATUS_CODE_NOT_FOUND, failureResponse.op.getStatusCode());
        }

        ops = initialStates.subList(10, 20).stream().map(state -> Operation.createGet(this.host, state.documentSelfLink)).collect(toList());
        List<ExampleServiceState> states = sender.sendAndWait(ops, ExampleServiceState.class);
        for (int i = 0; i < 5; i++) {
            ExampleServiceState state = states.get(i);
            assertTrue("doc should be updated: " + state.documentSelfLink, state.name.endsWith("-patched"));
        }


        // verify new data exists
        ops = newData.stream().map(state -> Operation.createGet(this.host, state.documentSelfLink)).collect(toList());
        sender.sendAndWait(ops);

    }

    private void restartHostAndWaitAvailable() throws Throwable {
        this.host.stop();
        this.host.setPort(0);
        this.host.start();
        this.host.waitForReplicatedFactoryServiceAvailable(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
    }


    private Map<URI, ExampleServiceState> populateExampleServices(int serviceCount) {
        // Post some documents to populate the index.
        URI factoryUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        Map<URI, ExampleServiceState> exampleStates = this.host.doFactoryChildServiceStart(null,
                serviceCount,
                ExampleServiceState.class,
                (o) -> {
                    ExampleServiceState s = new ExampleServiceState();
                    s.name = UUID.randomUUID().toString();
                    o.setBody(s);
                }, factoryUri);

        return exampleStates;
    }

    @Test
    public void threadCountStats() {
        this.host.waitFor("Waiting thread count to be populated in stats", () -> {
            Map<String, ServiceStat> stats = this.host.getServiceStats(this.host.getManagementServiceUri());
            return stats.get(STAT_NAME_THREAD_COUNT) != null;
        });
        Map<String, ServiceStat> stats = this.host.getServiceStats(this.host.getManagementServiceUri());
        double threadCountValue = stats.get(STAT_NAME_THREAD_COUNT).latestValue;
        assertEquals("threadCount in management/stats", Utils.DEFAULT_THREAD_COUNT, threadCountValue, 0);
    }

    @Test
    public void timeSnapshotRecovery() throws Throwable {
        int serviceVersion = 10;
        int snapshotServiceVersion = serviceVersion / 2;

        File backupFile = this.tempDir.newFile();
        TestRequestSender sender = this.host.getTestRequestSender();

        // create local file service
        String backupServiceLink = LocalFileService.SERVICE_PREFIX + "/backup";
        createBackupFileService(backupFile.toURI(), backupServiceLink);
        URI backupFileServiceUri = UriUtils.buildUri(this.host, backupServiceLink);

        // create and update a document
        String selfLink = UriUtils.buildUriPath(ExampleService.FACTORY_LINK, "/foo");
        ExampleServiceState doc = new ExampleServiceState();
        doc.name = "init";
        doc.documentSelfLink = selfLink;
        doc = sender.sendAndWait(Operation.createPost(this.host, ExampleService.FACTORY_LINK).setBody(doc), ExampleServiceState.class);

        long snapshotTime = 0;
        for (int i = 1; i <= serviceVersion; i++) {
            doc.name = "updated-v" + i;
            sender.sendAndWait(Operation.createPatch(this.host, doc.documentSelfLink).setBody(doc));
            if (i == snapshotServiceVersion) {
                snapshotTime = Utils.getNowMicrosUtc();
            }
        }

        // perform backup
        ServiceHostManagementService.BackupRequest backupRequest = new ServiceHostManagementService.BackupRequest();
        backupRequest.destination = backupFileServiceUri;
        backupRequest.kind = ServiceHostManagementService.BackupRequest.KIND;

        sender.sendAndWait(Operation.createPatch(this.host, ServiceHostManagementService.SELF_LINK).setBody(backupRequest));

        this.host.tearDown();

        // create new host
        this.host = VerificationHost.create(0);
        this.host.start();
        sender = this.host.getTestRequestSender();

        String restoreServiceLink = LocalFileService.SERVICE_PREFIX + "/restore";

        createRestoreFileService(backupFile.toURI(), restoreServiceLink);

        // perform restore with time snapshot boundary
        ServiceHostManagementService.RestoreRequest restoreRequest = new ServiceHostManagementService.RestoreRequest();
        restoreRequest.destination = UriUtils.buildUri(this.host, restoreServiceLink);
        restoreRequest.kind = ServiceHostManagementService.RestoreRequest.KIND;
        restoreRequest.timeSnapshotBoundaryMicros = snapshotTime;
        sender.sendAndWait(Operation.createPatch(this.host, ServiceHostManagementService.SELF_LINK).setBody(restoreRequest));

        // verify document version is the one specified as snapshotTime
        this.host.waitForReplicatedFactoryServiceAvailable(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
        ExampleServiceState result = sender.sendAndWait(Operation.createGet(this.host, selfLink), ExampleServiceState.class);
        assertEquals("Point-in-time version", snapshotServiceVersion, result.documentVersion);
    }

    private void createBackupFileService(URI localFileUri, String serviceLink) {
        LocalFileServiceState initialState = new LocalFileServiceState();
        initialState.fileOptions = EnumSet.of(StandardOpenOption.WRITE);
        initialState.localFileUri = localFileUri;

        Operation post = Operation.createPost(this.host, serviceLink).setBody(initialState);
        this.host.startService(post, new LocalFileService());
        this.host.waitForServiceAvailable(serviceLink);
    }

    private void createRestoreFileService(URI localFileUri, String serviceLink) {
        LocalFileServiceState initialState = new LocalFileServiceState();
        initialState.localFileUri = localFileUri;

        Operation post = Operation.createPost(this.host, serviceLink).setBody(initialState);
        this.host.startService(post, new LocalFileService());
        this.host.waitForServiceAvailable(serviceLink);
    }
}
