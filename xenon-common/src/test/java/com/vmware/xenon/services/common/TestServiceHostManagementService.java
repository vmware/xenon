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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URI;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.FileUtils;
import com.vmware.xenon.common.MinimalFileStore;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestProperty;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestServiceHostManagementService extends BasicTestCase {

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

        // start our blob store
        MinimalFileStore mfs = new MinimalFileStore();
        MinimalFileStore.MinimalFileState mfsState = new MinimalFileStore.MinimalFileState();
        File tmpFile = File.createTempFile("intermediate-file", ".zip", null);
        tmpFile.deleteOnExit();
        mfsState.fileUri = tmpFile.toURI();

        mfs = (MinimalFileStore) this.host.startServiceAndWait(mfs, UUID.randomUUID().toString(),
                mfsState);

        // Post some documents to populate the index.
        URI factoryUri = UriUtils.buildUri(this.host, ExampleFactoryService.SELF_LINK);
        Map<URI, ExampleServiceState> exampleStates = this.host.doFactoryChildServiceStart(null,
                serviceCount,
                ExampleServiceState.class,
                (o) -> {
                    ExampleServiceState s = new ExampleServiceState();
                    s.name = UUID.randomUUID().toString();
                    o.setBody(s);
                }, factoryUri);

        ServiceHostManagementService.BackupRequest backupRequest = new ServiceHostManagementService.BackupRequest();
        backupRequest.destination = mfs.getUri();
        backupRequest.kind = ServiceHostManagementService.BackupRequest.KIND;

        this.host.testStart(1);
        this.host.sendRequest(Operation
                .createPatch(UriUtils.buildUri(this.host, ServiceHostManagementService.SELF_LINK))
                .setReferer(this.host.getUri()).setBody(backupRequest)
                .setCompletion(this.host.getCompletion()));
        this.host.testWait();

        this.host.tearDown();

        this.host.log("MFS file %s (bytes:%d md5:%s)", tmpFile.toString(), tmpFile.length(),
                FileUtils.md5sum(tmpFile));

        this.host = VerificationHost.create(0);
        this.host.start();

        mfs = new MinimalFileStore();
        mfsState = new MinimalFileStore.MinimalFileState();
        mfsState.fileUri = tmpFile.toURI();
        mfsState.fileComplete = true;

        mfs = (MinimalFileStore) this.host.startServiceAndWait(mfs, UUID.randomUUID().toString(),
                mfsState);

        ServiceHostManagementService.RestoreRequest restoreRequest = new ServiceHostManagementService.RestoreRequest();
        restoreRequest.destination = mfs.getUri();
        restoreRequest.kind = ServiceHostManagementService.RestoreRequest.KIND;

        this.host.testStart(1);
        this.host.sendRequest(Operation
                .createPatch(UriUtils.buildUri(this.host, ServiceHostManagementService.SELF_LINK))
                .setReferer(this.host.getUri()).setBody(restoreRequest)
                .setCompletion(this.host.getCompletion()));
        this.host.testWait();

        // Check our documents are still there
        ServiceDocumentQueryResult queryResult = this.host
                .getFactoryState(UriUtils.buildExpandLinksQueryUri(UriUtils.buildUri(this.host,
                        ExampleFactoryService.SELF_LINK)));
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
}
