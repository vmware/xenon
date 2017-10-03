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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import static com.vmware.xenon.common.test.VerificationHost.buildDefaultServiceHostArguments;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.index.IndexWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.LuceneDocumentIndexBackupService.LuceneDocumentIndexBackupStrategy;
import com.vmware.xenon.services.common.LuceneDocumentIndexService.BackupResponse;
import com.vmware.xenon.services.common.ServiceHostManagementService.BackupRequest;
import com.vmware.xenon.services.common.ServiceHostManagementService.BackupType;
import com.vmware.xenon.services.common.ServiceHostManagementService.RestoreRequest;
import com.vmware.xenon.services.common.TestLuceneDocumentIndexService.InMemoryExampleService;

public class TestLuceneDocumentIndexBackupService {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public int count = 1000;

    private VerificationHost host;

    @Before
    public void setup() throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        this.host = createVerificationHost();
    }

    @After
    public void tearDown() {
        if (this.host != null) {
            this.host.stop();
            this.host = null;
        }
    }

    private VerificationHost createVerificationHost() throws Throwable {
        VerificationHost host = VerificationHost.create(0);
        host.start();

        // in-memory index related services
        host.addPrivilegedService(InMemoryLuceneDocumentIndexService.class);

        InMemoryLuceneDocumentIndexService inMemoryIndexService = new InMemoryLuceneDocumentIndexService();
        LuceneDocumentIndexBackupService inMemoryIndexBackupService = new LuceneDocumentIndexBackupService(inMemoryIndexService);
        Service inMemoryExampleFactory = InMemoryExampleService.createFactory();

        host.startService(Operation.createPost(host, ServiceUriPaths.CORE_IN_MEMORY_DOCUMENT_INDEX), inMemoryIndexService);
        host.startService(Operation.createPost(host, ServiceUriPaths.CORE_IN_MEMORY_DOCUMENT_INDEX_BACKUP), inMemoryIndexBackupService);
        host.startService(Operation.createPost(host, InMemoryExampleService.FACTORY_LINK), inMemoryExampleFactory);

        host.waitForServiceAvailable(ServiceUriPaths.CORE_IN_MEMORY_DOCUMENT_INDEX,
                ServiceUriPaths.CORE_IN_MEMORY_DOCUMENT_INDEX_BACKUP, InMemoryExampleService.FACTORY_LINK);

        return host;
    }

    @Test
    public void testBackupAndRestoreFromDirectory() throws Throwable {
        URI backupDirUri = this.temporaryFolder.newFolder("test-backup-dir").toURI();

        List<ExampleServiceState> createdData = populateData(ExampleService.FACTORY_LINK);

        BackupRequest b = new BackupRequest();
        b.kind = BackupRequest.KIND;
        b.destination = backupDirUri;
        b.backupType = BackupType.DIRECTORY;

        Operation backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP).setBody(b);
        this.host.getTestRequestSender().sendAndWait(backupOp, BackupResponse.class);

        // destroy and spin up new host
        this.host.tearDown();
        this.host = createVerificationHost();

        // restore
        RestoreRequest r = new RestoreRequest();
        r.kind = RestoreRequest.KIND;
        r.destination = backupDirUri;

        Operation restoreOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP).setBody(r);
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
    public void testBackupAndRestoreFromDirectoryToInMemoryIndex() throws Throwable {
        URI backupDirUri = this.temporaryFolder.newFolder("test-backup-dir").toURI();

        List<ExampleServiceState> createdData = populateData(InMemoryExampleService.FACTORY_LINK);

        BackupRequest b = new BackupRequest();
        b.kind = BackupRequest.KIND;
        b.destination = backupDirUri;
        b.backupType = BackupType.DIRECTORY;
        b.backupServiceLink = ServiceUriPaths.CORE_IN_MEMORY_DOCUMENT_INDEX_BACKUP;

        Operation backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_IN_MEMORY_DOCUMENT_INDEX_BACKUP).setBody(b);
        this.host.getTestRequestSender().sendAndWait(backupOp, BackupResponse.class);

        // destroy and spin up new host
        this.host.tearDown();
        this.host = createVerificationHost();

        // restore
        RestoreRequest r = new RestoreRequest();
        r.kind = RestoreRequest.KIND;
        r.destination = backupDirUri;

        Operation restoreOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_IN_MEMORY_DOCUMENT_INDEX_BACKUP).setBody(r);
        this.host.getTestRequestSender().sendAndWait(restoreOp);
        this.host.waitForReplicatedFactoryServiceAvailable(UriUtils.buildUri(this.host, InMemoryExampleService.FACTORY_LINK));

        // verify restored data exists
        List<Operation> ops = createdData.stream().map(state -> Operation.createGet(this.host, state.documentSelfLink)).collect(toList());
        this.host.getTestRequestSender().sendAndWait(ops);
    }

    @Test
    public void testBackupAndRestoreFromZipFile() throws Throwable {
        URI backupUri = this.temporaryFolder.newFile("test-backup.zip").toURI();

        List<ExampleServiceState> createdData = populateData(ExampleService.FACTORY_LINK);

        // default backup type is zip
        BackupRequest b = new BackupRequest();
        b.kind = BackupRequest.KIND;
        b.destination = backupUri;

        // backup with zip
        Operation backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP).setBody(b);
        this.host.getTestRequestSender().sendAndWait(backupOp);

        // destroy and spin up new host
        this.host.tearDown();
        this.host = createVerificationHost();

        RestoreRequest r = new RestoreRequest();
        r.kind = RestoreRequest.KIND;
        r.destination = backupUri;

        Operation restoreOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP).setBody(r);
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
    public void testBackupAndRestoreFromZipFileToInMemoryIndex() throws Throwable {
        URI backupUri = this.temporaryFolder.newFile("test-backup.zip").toURI();

        List<ExampleServiceState> createdData = populateData(InMemoryExampleService.FACTORY_LINK);

        // default backup type is zip
        BackupRequest b = new BackupRequest();
        b.kind = BackupRequest.KIND;
        b.destination = backupUri;
        b.backupServiceLink = ServiceUriPaths.CORE_IN_MEMORY_DOCUMENT_INDEX_BACKUP;

        // backup with zip
        Operation backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_IN_MEMORY_DOCUMENT_INDEX_BACKUP).setBody(b);
        this.host.getTestRequestSender().sendAndWait(backupOp);

        // destroy and spin up new host
        this.host.tearDown();
        this.host = createVerificationHost();

        RestoreRequest r = new RestoreRequest();
        r.kind = RestoreRequest.KIND;
        r.destination = backupUri;

        // restore
        Operation restoreOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_IN_MEMORY_DOCUMENT_INDEX_BACKUP).setBody(r);
        this.host.getTestRequestSender().sendAndWait(restoreOp);
        this.host.waitForReplicatedFactoryServiceAvailable(UriUtils.buildUri(this.host, InMemoryExampleService.FACTORY_LINK));

        // verify restored data exists
        List<Operation> ops = createdData.stream().map(state -> Operation.createGet(this.host, state.documentSelfLink)).collect(toList());
        this.host.getTestRequestSender().sendAndWait(ops);
    }

    private List<ExampleServiceState> populateData(String factoryLink) {
        List<Operation> ops = new ArrayList<>();
        for (int i = 0; i < this.count; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = "foo-" + i;
            state.documentSelfLink = state.name;
            Operation post = Operation.createPost(this.host, factoryLink).setBody(state);
            ops.add(post);
        }
        return this.host.getTestRequestSender().sendAndWait(ops, ExampleServiceState.class);
    }

    @Test
    public void backupRequestParameters() throws IOException {
        // test combination of backup request parameters.

        TestRequestSender sender = this.host.getTestRequestSender();

        BackupRequest backupRequest;
        Operation backupOp;

        // type=ZIP, destination=empty => fail
        backupRequest = new BackupRequest();
        backupRequest.kind = BackupRequest.KIND;

        backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP).setBody(backupRequest);
        sender.sendAndWaitFailure(backupOp);

        // type=ZIP, destination=existing file => should override
        File existingFile = this.temporaryFolder.newFile();

        backupRequest = new BackupRequest();
        backupRequest.kind = BackupRequest.KIND;
        backupRequest.destination = existingFile.toURI();

        backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP).setBody(backupRequest);
        sender.sendAndWait(backupOp);

        assertTrue("backup zip file should exist", Files.isRegularFile(existingFile.toPath()));

        long newBackupFileSize = Files.size(existingFile.toPath());
        assertTrue("existing empty file should be overridden", newBackupFileSize > 0);


        // type=ZIP, destination=existing directory => fail
        File existingDir = this.temporaryFolder.newFolder();

        backupRequest = new BackupRequest();
        backupRequest.kind = BackupRequest.KIND;
        backupRequest.backupType = BackupType.ZIP;
        backupRequest.destination = existingDir.toURI();

        backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP).setBody(backupRequest);
        sender.sendAndWaitFailure(backupOp);


        // type=DIRECTORY, destination=empty => fail
        backupRequest = new BackupRequest();
        backupRequest.kind = BackupRequest.KIND;
        backupRequest.backupType = BackupType.DIRECTORY;

        backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP).setBody(backupRequest);
        sender.sendAndWaitFailure(backupOp);


        // type=DIRECTORY, destination=existing file => fail
        existingFile = this.temporaryFolder.newFile();

        backupRequest = new BackupRequest();
        backupRequest.kind = BackupRequest.KIND;
        backupRequest.backupType = BackupType.DIRECTORY;
        backupRequest.destination = existingFile.toURI();

        backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP).setBody(backupRequest);
        sender.sendAndWaitFailure(backupOp);

        // type=DIRECTORY, destination=existing dir => success
        existingDir = this.temporaryFolder.newFolder();

        backupRequest = new BackupRequest();
        backupRequest.kind = BackupRequest.KIND;
        backupRequest.backupType = BackupType.DIRECTORY;
        backupRequest.destination = existingDir.toURI();

        backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP).setBody(backupRequest);
        sender.sendAndWait(backupOp);

        assertTrue("backup directory must be generated", Files.isDirectory(existingDir.toPath()));
        assertTrue("backup directory must not be empty", Files.list(existingDir.toPath()).count() > 0);
    }

    @Test
    public void incrementalBackupWithInMemoryIndex() throws Throwable {
        URI backupDirUri = this.temporaryFolder.newFolder("test-backup-dir").toURI();

        List<ExampleServiceState> initialData = populateData(InMemoryExampleService.FACTORY_LINK);

        BackupRequest b = new BackupRequest();
        b.kind = BackupRequest.KIND;
        b.destination = backupDirUri;
        b.backupType = BackupType.DIRECTORY;
        b.backupServiceLink = ServiceUriPaths.CORE_IN_MEMORY_DOCUMENT_INDEX_BACKUP;

        // perform initial backup
        Operation backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_IN_MEMORY_DOCUMENT_INDEX_BACKUP).setBody(b);
        this.host.getTestRequestSender().sendAndWait(backupOp, BackupResponse.class);

        // create more data
        List<Operation> ops = new ArrayList<>();
        for (int i = 0; i < this.count; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = "bar-" + i;
            state.documentSelfLink = state.name;
            Operation post = Operation.createPost(this.host, InMemoryExampleService.FACTORY_LINK).setBody(state);
            ops.add(post);
        }
        List<ExampleServiceState> additionalData = this.host.getTestRequestSender().sendAndWait(ops, ExampleServiceState.class);


        // perform incremental backup
        backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_IN_MEMORY_DOCUMENT_INDEX_BACKUP).setBody(b);
        this.host.getTestRequestSender().sendAndWait(backupOp, BackupResponse.class);


        // destroy and spin up new host
        this.host.tearDown();
        this.host = createVerificationHost();

        // restore
        RestoreRequest r = new RestoreRequest();
        r.kind = RestoreRequest.KIND;
        r.destination = backupDirUri;

        Operation restoreOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_IN_MEMORY_DOCUMENT_INDEX_BACKUP).setBody(r);
        this.host.getTestRequestSender().sendAndWait(restoreOp);
        this.host.waitForReplicatedFactoryServiceAvailable(UriUtils.buildUri(this.host, InMemoryExampleService.FACTORY_LINK));


        List<ExampleServiceState> allData = new ArrayList<>();
        allData.addAll(initialData);
        allData.addAll(additionalData);

        // verify all data exists
        ops = allData.stream().map(state -> Operation.createGet(this.host, state.documentSelfLink)).collect(toList());
        this.host.getTestRequestSender().sendAndWait(ops);

        Operation factoryGet = Operation.createGet(this.host, InMemoryExampleService.FACTORY_LINK);
        ServiceDocumentQueryResult result = this.host.getTestRequestSender().sendAndWait(factoryGet, ServiceDocumentQueryResult.class);
        assertEquals(Long.valueOf(allData.size()), result.documentCount);
    }

    @Test
    public void backupStrategy() throws Throwable {

        // value holders
        AtomicBoolean validateCalled = new AtomicBoolean(false);
        AtomicBoolean performCalled = new AtomicBoolean(false);
        AtomicReference<BackupRequest> backupRequestAtValidateRequest = new AtomicReference<>();
        AtomicReference<Operation> originalOpAtPerform = new AtomicReference<>();
        AtomicReference<BackupRequest> backupRequestAtPerform = new AtomicReference<>();
        AtomicReference<LuceneDocumentIndexService> indexServiceAtPerform = new AtomicReference<>();
        AtomicReference<String> indexDirectoryAtPerform = new AtomicReference<>();
        AtomicReference<IndexWriter> writerAtPerform = new AtomicReference<>();

        LuceneDocumentIndexBackupStrategy strategy = new LuceneDocumentIndexBackupStrategy() {
            @Override
            public Exception validateRequest(BackupRequest backupRequest) {
                validateCalled.set(true);
                backupRequestAtValidateRequest.set(backupRequest);
                return null;
            }

            @Override
            public void perform(Operation originalOp, BackupRequest backupRequest, LuceneDocumentIndexService indexService,
                    String indexDirectory, IndexWriter writer) throws IOException {
                performCalled.set(true);
                originalOpAtPerform.set(originalOp);
                backupRequestAtPerform.set(backupRequest);
                indexServiceAtPerform.set(indexService);
                indexDirectoryAtPerform.set(indexDirectory);
                writerAtPerform.set(writer);
                originalOp.complete();
            }
        };

        this.host.tearDown();
        this.host = new VerificationHost() {
            @Override
            protected LuceneDocumentIndexBackupStrategy getLuceneBackupStrategy() {
                return strategy;
            }
        };
        VerificationHost.initialize(this.host, buildDefaultServiceHostArguments(0));
        this.host.start();

        BackupRequest b = new BackupRequest();
        b.kind = BackupRequest.KIND;
        b.backupType = BackupType.DIRECTORY;

        Operation backupOp = Operation.createPatch(this.host, ServiceUriPaths.CORE_DOCUMENT_INDEX_BACKUP).setBody(b);
        this.host.getTestRequestSender().sendAndWait(backupOp, BackupResponse.class);

        assertTrue("validate method should be called", validateCalled.get());
        assertTrue("perform method should be called", performCalled.get());
        assertNotNull("passed index service should not be null", indexServiceAtPerform.get());
        assertEquals(indexServiceAtPerform.get().indexDirectory, indexDirectoryAtPerform.get());
        assertEquals(indexServiceAtPerform.get().writer, writerAtPerform.get());
        assertSame(backupRequestAtValidateRequest.get(), backupRequestAtPerform.get());
    }

}
