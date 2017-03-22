/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.LuceneDocumentIndexService.BackupResponse;
import com.vmware.xenon.services.common.LuceneDocumentIndexService.BackupType;

public class TestLuceneDocumentIndexBackupService extends BasicReusableHostTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public int count = 1000;

    private VerificationHost host;

    @Before
    public void setup() throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        this.host = VerificationHost.create(0);
        this.host.start();
    }

    @After
    public void tearDown() {
        if (this.host != null) {
            this.host.stop();
            this.host = null;
        }
    }

    @Test
    public void testBackupAndRestoreWithDirectory() throws Throwable {
        URI backupDirUri = this.temporaryFolder.newFolder("test-backup-dir").toURI();

        List<ExampleServiceState> createdData = populateData();

        LuceneDocumentIndexService.BackupRequest b = new LuceneDocumentIndexService.BackupRequest();
        b.destination = backupDirUri;
        b.documentKind = LuceneDocumentIndexService.BackupRequest.KIND;
        b.type = BackupType.INCREMENTAL;

        Operation backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX).setBody(b);
        BackupResponse backupResponse = this.host.getTestRequestSender().sendAndWait(backupOp, BackupResponse.class);
        assertEquals(backupDirUri, backupResponse.backupFile);

        // destroy and spin up new host
        this.host.tearDown();
        this.host = VerificationHost.create(0);
        this.host.start();

        // restore
        LuceneDocumentIndexService.RestoreRequest r = new LuceneDocumentIndexService.RestoreRequest();
        r.documentKind = LuceneDocumentIndexService.RestoreRequest.KIND;
        r.backupFile = backupDirUri;

        Operation restoreOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX).setBody(r);
        this.host.getTestRequestSender().sendAndWait(restoreOp);

        // restart
        this.host.stop();
        this.host.setPort(0);
        this.host.start();
        this.host.waitForReplicatedFactoryServiceAvailable(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));

        // verify restored data exists
        List<Operation> ops = createdData.stream().map(state -> Operation.createGet(this.host, state.documentSelfLink)).collect(toList());
        this.host.getTestRequestSender().sendAndWait(ops);
    }

    @Test
    public void testBackupAndRestoreWithZipFile() throws Throwable {
        URI backupUri = this.temporaryFolder.newFile("test-backup.zip").toURI();

        List<ExampleServiceState> createdData = populateData();

        // default backup type is zip
        LuceneDocumentIndexService.BackupRequest b = new LuceneDocumentIndexService.BackupRequest();
        b.destination = backupUri;
        b.documentKind = LuceneDocumentIndexService.BackupRequest.KIND;

        // backup with zip
        Operation backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX).setBody(b);
        this.host.getTestRequestSender().sendAndWait(backupOp);

        // destroy and spin up new host
        this.host.tearDown();
        this.host = VerificationHost.create(0);
        this.host.start();

        LuceneDocumentIndexService.RestoreRequest r = new LuceneDocumentIndexService.RestoreRequest();
        r.documentKind = LuceneDocumentIndexService.RestoreRequest.KIND;
        r.backupFile = backupUri;

        Operation restoreOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX).setBody(r);
        this.host.getTestRequestSender().sendAndWait(restoreOp);

        // restart
        this.host.stop();
        this.host.setPort(0);
        this.host.start();
        this.host.waitForReplicatedFactoryServiceAvailable(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));

        // verify restored data exists
        List<Operation> ops = createdData.stream().map(state -> Operation.createGet(this.host, state.documentSelfLink)).collect(toList());
        this.host.getTestRequestSender().sendAndWait(ops);
    }

    private List<ExampleServiceState> populateData() {
        List<Operation> ops = new ArrayList<>();
        for (int i = 0; i < this.count; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = "foo-" + i;
            state.documentSelfLink = state.name;
            Operation post = Operation.createPost(this.host, ExampleService.FACTORY_LINK).setBody(state);
            ops.add(post);
        }
        return this.host.getTestRequestSender().sendAndWait(ops, ExampleServiceState.class);
    }

    @Test
    public void backupRequestParameters() throws IOException {
        // test combination of backup request parameters.

        TestRequestSender sender = this.host.getTestRequestSender();

        LuceneDocumentIndexService.BackupRequest backupRequest;
        Operation backupOp;
        BackupResponse backupResponse;
        URI backupFileUri;

        // type=ZIP, destination=empty => generate a zip (existing behavior)
        backupRequest = new LuceneDocumentIndexService.BackupRequest();
        backupRequest.documentKind = LuceneDocumentIndexService.BackupRequest.KIND;

        backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX).setBody(backupRequest);
        backupResponse = sender.sendAndWait(backupOp, BackupResponse.class);

        backupFileUri = backupResponse.backupFile;
        assertTrue("backup zip file must be generated", Files.isRegularFile(Paths.get(backupFileUri)));


        // type=ZIP, destination=existing file => should override
        File existingFile = this.temporaryFolder.newFile();

        backupRequest = new LuceneDocumentIndexService.BackupRequest();
        backupRequest.documentKind = LuceneDocumentIndexService.BackupRequest.KIND;
        backupRequest.destination = existingFile.toURI();

        backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX).setBody(backupRequest);
        backupResponse = sender.sendAndWait(backupOp, BackupResponse.class);

        URI newBackupFileUri = backupResponse.backupFile;
        assertTrue("backup zip file should exist", Files.isRegularFile(Paths.get(newBackupFileUri)));

        long newBackupFileSize = Files.size(Paths.get(newBackupFileUri));
        assertTrue("existing empty file should be overridden", newBackupFileSize > 0);


        // type=ZIP, destination=existing directory => fail
        File existingDir = this.temporaryFolder.newFolder();

        backupRequest = new LuceneDocumentIndexService.BackupRequest();
        backupRequest.documentKind = LuceneDocumentIndexService.BackupRequest.KIND;
        backupRequest.type = BackupType.ZIP;
        backupRequest.destination = existingDir.toURI();

        backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX).setBody(backupRequest);
        sender.sendAndWaitFailure(backupOp);


        // type=INCREMENTAL, destination=empty => fail
        backupRequest = new LuceneDocumentIndexService.BackupRequest();
        backupRequest.documentKind = LuceneDocumentIndexService.BackupRequest.KIND;
        backupRequest.type = BackupType.INCREMENTAL;

        backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX).setBody(backupRequest);
        sender.sendAndWaitFailure(backupOp);


        // type=INCREMENTAL, destination=existing file => fail
        existingFile = this.temporaryFolder.newFile();

        backupRequest = new LuceneDocumentIndexService.BackupRequest();
        backupRequest.documentKind = LuceneDocumentIndexService.BackupRequest.KIND;
        backupRequest.type = BackupType.INCREMENTAL;
        backupRequest.destination = existingFile.toURI();

        backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX).setBody(backupRequest);
        sender.sendAndWaitFailure(backupOp);

        // type=INCREMENTAL, destination=existing dir => success
        existingDir = this.temporaryFolder.newFolder();

        backupRequest = new LuceneDocumentIndexService.BackupRequest();
        backupRequest.documentKind = LuceneDocumentIndexService.BackupRequest.KIND;
        backupRequest.type = BackupType.INCREMENTAL;
        backupRequest.destination = existingDir.toURI();

        backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX).setBody(backupRequest);
        backupResponse = sender.sendAndWait(backupOp, BackupResponse.class);

        backupFileUri = backupResponse.backupFile;
        assertTrue("backup directory must be generated", Files.isDirectory(Paths.get(backupFileUri)));
        assertTrue("backup directory must not be empty", Files.list(Paths.get(backupFileUri)).count() > 0);
    }
}
