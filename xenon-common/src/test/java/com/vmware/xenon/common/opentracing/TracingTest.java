/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common.opentracing;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;

import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;


public class TracingTest extends BasicReusableHostTestCase {

    @ClassRule
    public static InjectMockTracer injectMockTracer = new InjectMockTracer();

    @After
    public void tearDown() {
        this.host.tearDownInProcessPeers();
    }

    @Test
    @Ignore("https://www.pivotaltracker.com/story/show/153227731")
    public void testIncomingTraces() throws Throwable {
        MockTracer tracer = TracingTest.injectMockTracer.tracer;
        List<String> uris = new ArrayList<String>();

        this.host.startFactory(new TestStatefulService());
        this.host.startService(new TestStatelessService());
        // Populate the stateful service.
        TestStatefulService.State postBody = new TestStatefulService.State();
        postBody.name = "foo";
        postBody.documentSelfLink = "foo";
        Operation post = Operation.createPost(this.host, "/stateful").setBody(postBody);
        TestStatefulService.State postResult = this.sender.sendAndWait(post, TestStatefulService.State.class);
        assertEquals("foo", postResult.name);
        assertEquals("/stateful/foo", postResult.documentSelfLink);
        // we don't care about traces before this point
        tracer.reset();
        this.host.log(Level.INFO, "otTracer reset");

        // Submit a request to the stateless service, which will allocate a trace id, then make an internal request
        // to the stateless server, which should give us another span under the same traceid if propogation outbound
        // and inbound is working right. The stateless service makes two such requests to be sure that context is
        // preserved after finishing the span from the first callout.
        Operation getOp = this.sender.sendAndWait(Operation.createGet(this.host, "/stateless"));
        // The stateless service returns what it read from the stateful service.
        TestStatefulService.State getResult = getOp.getBody(TestStatefulService.State.class);
        assertEquals("foo", getResult.name);
        assertEquals("/stateful/foo", getResult.documentSelfLink);

        // capture URIs for comparising with the trace spans.
        // stateful
        uris.add(Operation.createGet(this.host, "/stateful/foo").getUri().toString());
        // stateless
        uris.add(getOp.getUri().toString());

        List<MockSpan> finishedSpans = tracer.finishedSpans();
        /* Spans can potentially complete out of order */
        finishedSpans.sort(Comparator.comparingLong(e -> e.context().spanId()));
        // TODO: provide a nice declarative check. e.g. a matcher that takes a yaml expression.
        // We want to check:
        // for each span opname tags, type.
        MockSpan finishedSpan = finishedSpans.get(0);
        long traceId = finishedSpan.context().traceId();
        List<MockSpan> relevantSpans = new ArrayList<>();
        for (MockSpan span : finishedSpans) {
            this.host.log(Level.INFO, "span %s", span.toString());
            String op = span.operationName();
            // Filter out spans that are not part of the test - e.g. gossip maintenance
            if (!op.equals("/stateless") && !op.equals("/stateful/foo") && !op.equals("Queue")) {
                continue;
            }
            assertEquals(String.format("broken trace span %s", span.toString()), traceId, span.context().traceId());
            if (span.operationName().equals("Queue")) {
                /* internal housekeeping of executor/scheduling */
                continue;
            }
            assertEquals(String.format("trace span %s", span.toString()), "GET", span.tags().get(Tags.HTTP_METHOD.getKey()));
            assertEquals(String.format("trace span %s", span.toString()), "200", span.tags().get(Tags.HTTP_STATUS.getKey()));
            relevantSpans.add(span);
        }
        /* Urls: 0 and 1 are the client and server handling of stateless, 2 through 5 stateful/foo. */
        String stateful = uris.get(0);
        String stateless = uris.get(1);
        assertEquals(stateless, relevantSpans.get(0).tags().get(Tags.HTTP_URL.getKey()));
        assertEquals(stateless, relevantSpans.get(1).tags().get(Tags.HTTP_URL.getKey()));
        assertEquals(stateful, relevantSpans.get(2).tags().get(Tags.HTTP_URL.getKey()));
        assertEquals(stateful, relevantSpans.get(3).tags().get(Tags.HTTP_URL.getKey()));
        assertEquals(stateful, relevantSpans.get(4).tags().get(Tags.HTTP_URL.getKey()));
        assertEquals(stateful, relevantSpans.get(5).tags().get(Tags.HTTP_URL.getKey()));
        /* kinds: even should be outbound CLIENT spans, odd inbound SERVER spans. */
        assertEquals(Tags.SPAN_KIND_CLIENT, relevantSpans.get(0).tags().get(Tags.SPAN_KIND.getKey()));
        assertEquals(Tags.SPAN_KIND_SERVER, relevantSpans.get(1).tags().get(Tags.SPAN_KIND.getKey()));
        assertEquals(Tags.SPAN_KIND_CLIENT, relevantSpans.get(2).tags().get(Tags.SPAN_KIND.getKey()));
        assertEquals(Tags.SPAN_KIND_SERVER, relevantSpans.get(3).tags().get(Tags.SPAN_KIND.getKey()));
        assertEquals(Tags.SPAN_KIND_CLIENT, relevantSpans.get(4).tags().get(Tags.SPAN_KIND.getKey()));
        assertEquals(Tags.SPAN_KIND_SERVER, relevantSpans.get(5).tags().get(Tags.SPAN_KIND.getKey()));
        // TODO: test of error paths to ensure capturing of status is robust
        // TODO -, operationName should be the factory
        /* Only one trace expected */
        /* TODO: io.opentracing.tag.Tags#PEER_HOSTNAME, io.opentracing.tag.Tags#PEER_PORT */
        assertEquals(6 , relevantSpans.toArray().length);
    }
}


