/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.net.URI;
import java.util.HashMap;

import org.junit.Test;

import com.vmware.xenon.services.common.QueryTask;

public class TestQueryResults {

    @Test
    public void fromTask() throws Exception {
        QueryTask task = new QueryTask();
        ResultProcessor results = ResultProcessor.create(task);
        assertSame(task, results.getQueryTask());
    }

    @Test
    public void fromOp() throws Exception {
        QueryTask task = new QueryTask();
        Operation op = Operation.createGet(URI.create("/no/such/uri")).setBody(task);
        ResultProcessor results = ResultProcessor.create(task);
        assertNotNull(results.getQueryTask());
    }

    @Test
    public void selectedDocumentNull() throws Exception {
        assertNull(ResultProcessor.create(new QueryTask())
                .selectedDocument("id", ServiceDocument.class));
    }

    @Test
    public void selectedDocument() throws Exception {
        QueryTask task = new QueryTask();
        task.results = new ServiceDocumentQueryResult();
        task.results.selectedDocuments = new HashMap<>();
        ServiceDocument doc = new ServiceDocument();
        String selfLink = "link";
        doc.documentSelfLink = selfLink;
        task.results.selectedDocuments.put(selfLink, doc);

        assertSame(ResultProcessor.create(task).selectedDocument(selfLink, ServiceDocument.class),
                doc);

        assertEquals(ResultProcessor.create(task)
                .selectedDocument(selfLink, ServiceDocument.class).documentSelfLink, selfLink);

        assertTrue(ResultProcessor.create(task).selectedDocuments(ServiceDocument.class).iterator()
                .hasNext());
    }

    @Test
    public void selectedDocumentJson() throws Exception {
        QueryTask task = new QueryTask();
        task.results = new ServiceDocumentQueryResult();
        task.results.selectedDocuments = new HashMap<>();
        ServiceDocument doc = new ServiceDocument();
        String selfLink = "link";
        doc.documentSelfLink = selfLink;
        task.results.selectedDocuments.put(selfLink, Utils.toJson(doc));

        assertEquals(ResultProcessor.create(task)
                .selectedDocument(selfLink, ServiceDocument.class).documentSelfLink, selfLink);

        assertTrue(ResultProcessor.create(task).selectedDocuments(ServiceDocument.class).iterator()
                .hasNext());
    }

    @Test
    public void documentNull() throws Exception {
        assertNull(ResultProcessor.create(new QueryTask()).document("id", ServiceDocument.class));
    }

    @Test
    public void selectedDocumentsNull() throws Exception {
        assertNotNull(
                ResultProcessor.create(new QueryTask()).selectedDocuments(ServiceDocument.class));
    }

    @Test
    public void documents() throws Exception {
        QueryTask task = new QueryTask();
        task.results = new ServiceDocumentQueryResult();
        task.results.documents = new HashMap<>();
        ServiceDocument doc = new ServiceDocument();
        String selfLink = "link";
        doc.documentSelfLink = selfLink;
        task.results.documents.put(selfLink, doc);

        assertSame(ResultProcessor.create(task).document(selfLink, ServiceDocument.class), doc);

        assertEquals(ResultProcessor.create(task)
                .document(selfLink, ServiceDocument.class).documentSelfLink, selfLink);

        assertTrue(
                ResultProcessor.create(task).documents(ServiceDocument.class).iterator()
                        .hasNext());
    }

    @Test
    public void documentsJson() throws Exception {
        QueryTask task = new QueryTask();
        task.results = new ServiceDocumentQueryResult();
        task.results.documents = new HashMap<>();
        ServiceDocument doc = new ServiceDocument();
        String selfLink = "link";
        doc.documentSelfLink = selfLink;
        task.results.documents.put(selfLink, Utils.toJson(doc));

        assertEquals(ResultProcessor.create(task)
                .document(selfLink, ServiceDocument.class).documentSelfLink, selfLink);

        assertTrue(
                ResultProcessor.create(task).documents(ServiceDocument.class).iterator().hasNext());
    }

    @Test
    public void selectedLinks() throws Exception {
        assertNotNull(ResultProcessor.create(new QueryTask()).selectedLinks());
    }

    @Test
    public void documentLinks() throws Exception {
        assertNotNull(ResultProcessor.create(new QueryTask()).documentLinks());
    }

    @Test
    public void documentsNull() throws Exception {
        assertNotNull(ResultProcessor.create(new QueryTask()).documents(ServiceDocument.class));
    }
}