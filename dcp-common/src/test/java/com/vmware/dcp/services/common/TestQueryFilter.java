/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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

package com.vmware.dcp.services.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.ServiceDocumentDescription;
import com.vmware.dcp.common.ServiceDocumentDescription.Builder;
import com.vmware.dcp.services.common.QueryFilter.Conjunction;
import com.vmware.dcp.services.common.QueryFilter.QueryFilterException;
import com.vmware.dcp.services.common.QueryFilter.UnsupportedMatchTypeException;
import com.vmware.dcp.services.common.QueryTask.Query;
import com.vmware.dcp.services.common.QueryTask.Query.Occurance;
import com.vmware.dcp.services.common.QueryTask.QueryTerm.MatchType;

public class TestQueryFilter {

    public boolean isStressTest = false;

    public static class QueryFilterDocument extends ServiceDocument {
        public String c1;
        public String c2;
        public String c3;
        public String c4;
        public String c5;
        public String c6;
        public String c7;
        public String c8;

        public Color e1;
        public URI u1;
        public Boolean b1;

        public List<String> l1;
        public List<String> l2;

        public Map<String, String> m1;
        public Map<String, String> m2;
    }

    public enum Color {
        RED,
        BLUE
    }

    final ServiceDocumentDescription description =
            Builder.create().buildDescription(QueryFilterDocument.class);

    Query createTerm(String key, String value) {
        Query query = new Query();
        query.setTermPropertyName(key);
        query.setTermMatchType(MatchType.TERM);
        query.setTermMatchValue(value);
        return query;
    }

    Query createTerm(String key, String value, Occurance occurance) {
        Query query = createTerm(key, value);
        query.occurance = occurance;
        return query;
    }

    /**
     * Create DNF from query. Returns a set with the string representations
     * of the conjunctions in the DNF.
     *
     * Only used to make tests more readable.
     *
     * @param q
     * @return
     */
    Set<String> createDisjunctiveNormalForm(Query q) {
        List<Conjunction> dnf = QueryFilter.createDisjunctiveNormalForm(q);
        Set<String> ret = new HashSet<>();
        for (Conjunction c : dnf) {
            ret.add(c.toString());
        }
        return ret;
    }

    Query createSimpleDisjunctionQuery() {
        Query t1 = createTerm("c1", "v1", Occurance.SHOULD_OCCUR);
        Query t2 = createTerm("c2", "v2", Occurance.SHOULD_OCCUR);

        Query q = new Query();
        q.addBooleanClause(t1);
        q.addBooleanClause(t2);
        return q;
    }

    @Test
    public void simpleDisjunction() {
        Set<String> dnf = createDisjunctiveNormalForm(createSimpleDisjunctionQuery());
        assertEquals(2, dnf.size());
        assertTrue(dnf.contains("c1=v1"));
        assertTrue(dnf.contains("c2=v2"));
    }

    @Test
    public void evaluateSimpleDisjunction() throws QueryFilterException {
        QueryFilter filter = QueryFilter.create(createSimpleDisjunctionQuery());
        QueryFilterDocument document;

        document = new QueryFilterDocument();
        document.c1 = "v1";
        assertTrue(filter.evaluate(document, this.description));

        document = new QueryFilterDocument();
        document.c2 = "v2";
        assertTrue(filter.evaluate(document, this.description));

        document = new QueryFilterDocument();
        document.c3 = "v3";
        assertFalse(filter.evaluate(document, this.description));
    }

    Query createSimpleConjunctionQuery() {
        Query t1 = createTerm("c1", "v1", Occurance.MUST_OCCUR);
        Query t2 = createTerm("c2", "v2", Occurance.MUST_OCCUR);

        Query q = new Query();
        q.addBooleanClause(t1);
        q.addBooleanClause(t2);
        return q;
    }

    @Test
    public void simpleConjunction() {
        Set<String> dnf = createDisjunctiveNormalForm(createSimpleConjunctionQuery());
        assertEquals(1, dnf.size());
        assertTrue(dnf.contains("c1=v1 AND c2=v2"));
    }

    @Test
    public void evaluateSimpleConjunction() throws QueryFilterException {
        QueryFilter filter = QueryFilter.create(createSimpleConjunctionQuery());
        QueryFilterDocument document;

        document = new QueryFilterDocument();
        document.c1 = "v1";
        assertFalse(filter.evaluate(document, this.description));

        document = new QueryFilterDocument();
        document.c2 = "v2";
        assertFalse(filter.evaluate(document, this.description));

        document = new QueryFilterDocument();
        document.c1 = "v1";
        document.c2 = "v2";
        assertTrue(filter.evaluate(document, this.description));
    }

    Query createWithNegationQuery() {
        Query t1 = createTerm("c1", "v1", Occurance.MUST_OCCUR);
        Query t2 = createTerm("c2", "v2", Occurance.MUST_NOT_OCCUR);

        Query q = new Query();
        q.addBooleanClause(t1);
        q.addBooleanClause(t2);
        return q;
    }

    @Test
    public void withNegation() {
        Set<String> dnf = createDisjunctiveNormalForm(createWithNegationQuery());
        assertEquals(1, dnf.size());
        assertTrue(dnf.contains("c1=v1 AND NOT(c2=v2)"));
    }

    @Test
    public void evaluateWithNegation() throws QueryFilterException {
        QueryFilter filter = QueryFilter.create(createWithNegationQuery());
        QueryFilterDocument document;

        document = new QueryFilterDocument();
        document.c1 = "v1";
        document.c2 = "v2";
        assertFalse(filter.evaluate(document, this.description));

        document = new QueryFilterDocument();
        document.c1 = "v1";
        document.c2 = "v3";
        assertTrue(filter.evaluate(document, this.description));
    }

    Query createWithNegationOnUnknownProperty() {
        Query t1 = createTerm("c1", "v1", Occurance.MUST_OCCUR);
        Query t2 = createTerm("unknownProperty", "v2", Occurance.MUST_NOT_OCCUR);

        Query q = new Query();
        q.addBooleanClause(t1);
        q.addBooleanClause(t2);
        return q;
    }

    @Test
    public void evaluateWithNegationOnUnknownProperty() throws QueryFilterException {
        QueryFilter filter = QueryFilter.create(createWithNegationOnUnknownProperty());
        QueryFilterDocument document;

        document = new QueryFilterDocument();
        assertFalse(filter.evaluate(document, this.description));

        document = new QueryFilterDocument();
        document.c1 = "v1";
        assertTrue(filter.evaluate(document, this.description));
    }

    Query createMustOccurMixedWithShouldOccurQuery() {
        Query t1 = createTerm("c1", "v1", Occurance.MUST_OCCUR);
        Query t2 = createTerm("c2", "v2", Occurance.SHOULD_OCCUR);
        Query t3 = createTerm("c3", "v3", Occurance.MUST_OCCUR);

        Query q = new Query();
        q.addBooleanClause(t1);
        q.addBooleanClause(t2);
        q.addBooleanClause(t3);
        return q;
    }

    @Test
    public void mustOccurMixedWithShouldOccur() {
        Set<String> dnf = createDisjunctiveNormalForm(createMustOccurMixedWithShouldOccurQuery());
        assertEquals(1, dnf.size());
        assertTrue(dnf.contains("c1=v1 AND c3=v3"));
    }

    Query createSimpleDisjunctionOfConjunctionsQuery() {
        Query c1 = createTerm("c1", "v1", Occurance.MUST_OCCUR);
        Query c2 = createTerm("c2", "v2", Occurance.MUST_OCCUR);
        Query c3 = createTerm("c3", "v3", Occurance.MUST_OCCUR);
        Query c4 = createTerm("c4", "v4", Occurance.MUST_OCCUR);

        Query d1 = new Query();
        d1.occurance = Occurance.SHOULD_OCCUR;
        d1.addBooleanClause(c1);
        d1.addBooleanClause(c2);

        Query d2 = new Query();
        d2.occurance = Occurance.SHOULD_OCCUR;
        d2.addBooleanClause(c3);
        d2.addBooleanClause(c4);

        Query q = new Query();
        q.addBooleanClause(d1);
        q.addBooleanClause(d2);
        return q;
    }

    @Test
    public void simpleDisjunctionOfConjunctions() {
        Set<String> dnf = createDisjunctiveNormalForm(createSimpleDisjunctionOfConjunctionsQuery());
        assertEquals(2, dnf.size());
        assertTrue(dnf.contains("c1=v1 AND c2=v2"));
        assertTrue(dnf.contains("c3=v3 AND c4=v4"));
    }

    @Test
    public void evaluateSimpleDisjunctionOfConjunctions() throws QueryFilterException {
        QueryFilter filter = QueryFilter.create(createSimpleDisjunctionOfConjunctionsQuery());
        QueryFilterDocument document;

        document = new QueryFilterDocument();
        document.c1 = "v1";
        assertFalse(filter.evaluate(document, this.description));

        document = new QueryFilterDocument();
        document.c2 = "v2";
        assertFalse(filter.evaluate(document, this.description));

        document = new QueryFilterDocument();
        document.c1 = "v1";
        document.c4 = "v4";
        assertFalse(filter.evaluate(document, this.description));

        document = new QueryFilterDocument();
        document.c2 = "v2";
        document.c3 = "v3";
        assertFalse(filter.evaluate(document, this.description));

        document = new QueryFilterDocument();
        document.c1 = "v1";
        document.c2 = "v2";
        assertTrue(filter.evaluate(document, this.description));

        document = new QueryFilterDocument();
        document.c3 = "v3";
        document.c4 = "v4";
        assertTrue(filter.evaluate(document, this.description));
    }

    Query createSimpleConjunctionOfDisjunctionsQuery() {
        Query c1 = createTerm("c1", "v1", Occurance.SHOULD_OCCUR);
        Query c2 = createTerm("c2", "v2", Occurance.SHOULD_OCCUR);
        Query c3 = createTerm("c3", "v3", Occurance.SHOULD_OCCUR);
        Query c4 = createTerm("c4", "v4", Occurance.SHOULD_OCCUR);

        Query d1 = new Query();
        d1.occurance = Occurance.MUST_OCCUR;
        d1.addBooleanClause(c1);
        d1.addBooleanClause(c2);

        Query d2 = new Query();
        d2.occurance = Occurance.MUST_OCCUR;
        d2.addBooleanClause(c3);
        d2.addBooleanClause(c4);

        Query q = new Query();
        q.addBooleanClause(d1);
        q.addBooleanClause(d2);
        return q;
    }

    @Test
    public void simpleConjunctionOfDisjunctions() {
        Set<String> dnf = createDisjunctiveNormalForm(createSimpleConjunctionOfDisjunctionsQuery());
        assertEquals(4, dnf.size());
        assertTrue(dnf.contains("c1=v1 AND c3=v3"));
        assertTrue(dnf.contains("c1=v1 AND c4=v4"));
        assertTrue(dnf.contains("c2=v2 AND c3=v3"));
        assertTrue(dnf.contains("c2=v2 AND c4=v4"));
    }

    @Test
    public void evaluateSimpleConjunctionOfDisjunctions() throws QueryFilterException {
        QueryFilter filter = QueryFilter.create(createSimpleConjunctionOfDisjunctionsQuery());
        QueryFilterDocument document;

        document = new QueryFilterDocument();
        document.c1 = "v1";
        assertFalse(filter.evaluate(document, this.description));

        document = new QueryFilterDocument();
        document.c2 = "v2";
        assertFalse(filter.evaluate(document, this.description));

        document = new QueryFilterDocument();
        document.c1 = "v1";
        document.c3 = "v3";
        assertTrue(filter.evaluate(document, this.description));

        document = new QueryFilterDocument();
        document.c1 = "v1";
        document.c4 = "v4";
        assertTrue(filter.evaluate(document, this.description));

        document = new QueryFilterDocument();
        document.c2 = "v2";
        document.c3 = "v3";
        assertTrue(filter.evaluate(document, this.description));

        document = new QueryFilterDocument();
        document.c2 = "v2";
        document.c4 = "v4";
        assertTrue(filter.evaluate(document, this.description));
    }

    Query createWithListOfStringQuery() {
        String n1 = QueryTask.QuerySpecification.buildCollectionItemName("l1");
        String n2 = QueryTask.QuerySpecification.buildCollectionItemName("l2");
        Query t1 = createTerm(n1, "v2", Occurance.MUST_OCCUR);
        Query t2 = createTerm(n2, "v3", Occurance.MUST_NOT_OCCUR);

        Query q = new Query();
        q.addBooleanClause(t1);
        q.addBooleanClause(t2);
        return q;
    }

    @Test
    public void evaluateWithListOfString() throws QueryFilterException {
        QueryFilter filter = QueryFilter.create(createWithListOfStringQuery());
        QueryFilterDocument document;

        document = new QueryFilterDocument();
        document.l1 = new LinkedList<>();
        document.l1.add("v1");
        assertFalse(filter.evaluate(document, this.description));

        document.l1.add("v2");
        assertTrue(filter.evaluate(document, this.description));

        document.l2 = new LinkedList<>();
        document.l2.add("v2");
        assertTrue(filter.evaluate(document, this.description));

        document.l2.add("v3");
        assertFalse(filter.evaluate(document, this.description));
    }

    Query createWithMapOfStringToString() {
        String n1 = QueryTask.QuerySpecification.buildCompositeFieldName("m1", "k1");
        String n2 = QueryTask.QuerySpecification.buildCompositeFieldName("m2", "k2");
        Query t1 = createTerm(n1, "v2", Occurance.MUST_OCCUR);
        Query t2 = createTerm(n2, "v3", Occurance.MUST_NOT_OCCUR);

        Query q = new Query();
        q.addBooleanClause(t1);
        q.addBooleanClause(t2);
        return q;
    }

    @Test
    public void evaluateWithMapOfStringToString() throws QueryFilterException {
        QueryFilter filter = QueryFilter.create(createWithMapOfStringToString());
        QueryFilterDocument document;

        document = new QueryFilterDocument();
        document.m1 = new HashMap<>();
        document.m1.put("k1", "v1");
        assertFalse(filter.evaluate(document, this.description));

        document.m1.put("k1", "v2");
        assertTrue(filter.evaluate(document, this.description));

        document.m2 = new HashMap<>();
        document.m2.put("k2", "v2");
        assertTrue(filter.evaluate(document, this.description));

        document.m2.put("k2", "v3");
        assertFalse(filter.evaluate(document, this.description));
    }

    @Test
    public void evaluateWithEnum() throws QueryFilterException {
        QueryFilter filter = QueryFilter.create(Query.Builder.create()
                .addFieldClause("e1", Color.RED)
                .build());
        QueryFilterDocument document;

        document = new QueryFilterDocument();
        document.e1 = Color.RED;
        assertTrue(filter.evaluate(document, this.description));

        document.e1 = Color.BLUE;
        assertFalse(filter.evaluate(document, this.description));

        document.e1 = null;
        assertFalse(filter.evaluate(document, this.description));
    }

    @Test
    public void evaluateWithURI() throws QueryFilterException {
        QueryFilter filter = QueryFilter.create(Query.Builder.create()
                .addFieldClause("u1", URI.create("http://www.net.com"))
                .build());
        QueryFilterDocument document;

        document = new QueryFilterDocument();
        document.u1 = URI.create("http://www.net.com");
        assertTrue(filter.evaluate(document, this.description));

        document.u1 = URI.create("http://www.com.net");
        assertFalse(filter.evaluate(document, this.description));

        document.u1 = null;
        assertFalse(filter.evaluate(document, this.description));
    }

    @Test
    public void evaluateWithBoolean() throws QueryFilterException {
        QueryFilter filter = QueryFilter.create(Query.Builder.create()
                .addFieldClause("b1", true)
                .build());
        QueryFilterDocument document;

        document = new QueryFilterDocument();
        document.b1 = true;
        assertTrue(filter.evaluate(document, this.description));

        document.b1 = false;
        assertFalse(filter.evaluate(document, this.description));

        document.b1 = null;
        assertFalse(filter.evaluate(document, this.description));
    }

    @Test(expected = UnsupportedMatchTypeException.class)
    public void unsupportedMatchTypeWildcard() throws QueryFilterException {
        Query q = createTerm("c1", "v1");
        q.term.matchType = MatchType.WILDCARD;
        QueryFilter.create(q);
    }

    @Test(expected = UnsupportedMatchTypeException.class)
    public void unsupportedMatchTypePhrase() throws QueryFilterException {
        Query q = createTerm("c1", "v1");
        q.term.matchType = MatchType.PHRASE;
        QueryFilter.create(q);
    }

    class TimeBoundRunner {
        private final Runnable runnable;

        long count;
        long duration;

        TimeBoundRunner(Runnable runnable) {
            this.runnable = runnable;
        }

        void runImpl(long runForMillis) {
            this.count = 0;
            this.duration = 0;

            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() < (start + runForMillis)) {
                this.runnable.run();
                this.count++;
            }

            this.duration = System.currentTimeMillis() - start;
        }

        void run(long runForMillis) {
            runImpl(TimeUnit.SECONDS.toMillis(1));
            runImpl(runForMillis);
        }

        void print(String prefix) {
            double rate = this.count / (this.duration / 1000.0d);
            System.out.println(String.format("%s: %.0f per second", prefix, rate));
        }
    }

    Query createBenchmarkQuery() {
        Query q = new Query();
        for (int i = 0; i < 100; i++) {
            Query r = new Query();
            r.occurance = Occurance.SHOULD_OCCUR;
            for (int j = 1; j < 8; j++) {
                Query c = createTerm(String.format("c%d", j), String.format("%d", i));
                c.occurance = Occurance.MUST_OCCUR;
                r.addBooleanClause(c);
            }
            q.addBooleanClause(r);
        }

        return q;
    }

    @Test
    public void throughputQueryFilterCreation() throws QueryFilterException {
        if (!this.isStressTest) {
            return;
        }
        Query q = createBenchmarkQuery();
        TimeBoundRunner r = new TimeBoundRunner(() -> {
            try {
                QueryFilter.create(q);
            } catch (QueryFilterException e) {
                throw new Error(e);
            }
        });

        r.run(TimeUnit.SECONDS.toMillis(3));
        r.print("queryFilterCreation");
    }

    @Test
    public void throughputQueryFilterEvaluationPass() throws QueryFilterException {
        if (!this.isStressTest) {
            return;
        }
        QueryFilter filter = QueryFilter.create(createBenchmarkQuery());
        QueryFilterDocument document = new QueryFilterDocument();
        document.c1 = "99";
        document.c2 = "99";
        document.c3 = "99";
        document.c4 = "99";
        document.c5 = "99";
        document.c6 = "99";
        document.c7 = "99";
        document.c8 = "99";

        TimeBoundRunner r = new TimeBoundRunner(() -> {
            assertTrue(filter.evaluate(document, this.description));
        });

        r.run(TimeUnit.SECONDS.toMillis(3));
        r.print("queryFilterEvaluationPass");
    }

    @Test
    public void throughputQueryFilterEvaluationFail() throws QueryFilterException {
        if (!this.isStressTest) {
            return;
        }
        QueryFilter filter = QueryFilter.create(createBenchmarkQuery());
        QueryFilterDocument document = new QueryFilterDocument();
        document.c1 = "199";
        document.c2 = "199";
        document.c3 = "199";
        document.c4 = "199";
        document.c5 = "199";
        document.c6 = "199";
        document.c7 = "199";
        document.c8 = "199";

        TimeBoundRunner r = new TimeBoundRunner(() -> {
            assertFalse(filter.evaluate(document, this.description));
        });

        r.run(TimeUnit.SECONDS.toMillis(3));
        r.print("queryFilterEvaluationFail");
    }
}
