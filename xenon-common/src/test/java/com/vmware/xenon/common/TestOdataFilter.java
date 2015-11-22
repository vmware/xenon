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

package com.vmware.xenon.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.vmware.xenon.common.ODataQueryVisitor;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.NumericRange;
import com.vmware.xenon.services.common.QueryTask.Query;

public class TestOdataFilter {

    @Test
    public void testSimpleQuery() throws Throwable {

        Query expected = new Query().setTermPropertyName("name").setTermMatchValue("foo");
        expected.occurance = Query.Occurance.MUST_OCCUR;

        String odataFilter = String.format("%s eq %s", expected.term.propertyName,
                expected.term.matchValue);
        Query actual = toQuery(odataFilter);

        assertQueriesEqual(actual, expected);
    }

    @Test
    public void testSimpleBooleanQuery() throws Throwable {
        Query expected = new Query();

        // The test $filter is (name eq 'faiyaz') OR (foo eq 'bar')

        // OR is SHOULD_OCCUR in our query verbiage.
        expected.occurance = Query.Occurance.SHOULD_OCCUR;

        Query term1 = new Query().setTermPropertyName("name").setTermMatchValue("faiyaz");
        expected.addBooleanClause(term1);

        Query term2 = new Query().setTermPropertyName("foo").setTermMatchValue("bar");
        expected.addBooleanClause(term2);

        String odataFilter = String.format("(%s eq %s) or (%s eq %s)", term1.term.propertyName,
                term1.term.matchValue, term2.term.propertyName, term2.term.matchValue);
        Query actual = toQuery(odataFilter);

        assertQueriesEqual(actual, expected);
    }

    @Test
    public void testSimpleWildcardQuery() throws Throwable {

        Query expected = new Query().setTermPropertyName("name").setTermMatchValue("foo*");
        expected.occurance = Query.Occurance.MUST_OCCUR;
        expected.setTermMatchType(QueryTask.QueryTerm.MatchType.WILDCARD);

        String odataFilter = String.format("%s eq %s", expected.term.propertyName,
                expected.term.matchValue);
        Query actual = toQuery(odataFilter);

        assertQueriesEqual(actual, expected);
    }

    @Test
    public void testSimpleLongLessThanQuery() throws Throwable {
        Query expected = new Query();

        // The test $filter is (age lt 50).  This corresponds to a range query.
        NumericRange<Long> r = QueryTask.NumericRange.createLongRange(Long.MIN_VALUE,
                (long) 50, true, false);
        expected.setNumericRange(r);
        expected.setTermPropertyName("age");

        String odataFilter = String.format("%s lt 50", expected.term.propertyName);
        Query actual = toQuery(odataFilter);

        assertQueriesEqual(actual, expected);
    }

    @Test
    public void testSimpleDoubleLessThanQuery() throws Throwable {
        Query expected = new Query();

        // The test $filter is (age lt 50.0).  This corresponds to a range query.
        NumericRange<Double> r = QueryTask.NumericRange.createDoubleRange(Double.MIN_VALUE,
                (double) 50, true, false);
        expected.setNumericRange(r);
        expected.setTermPropertyName("age");

        String odataFilter = String.format("%s lt 50.0", expected.term.propertyName);
        Query actual = toQuery(odataFilter);

        assertQueriesEqual(actual, expected);
    }

    @Test
    public void testSimpleLongLessThanOrEqualQuery() throws Throwable {
        Query expected = new Query();

        // The test $filter is (age le 50).  This corresponds to a range query.
        NumericRange<Long> r = QueryTask.NumericRange.createLongRange(Long.MIN_VALUE,
                (long) 50, true, true);
        expected.setNumericRange(r);
        expected.setTermPropertyName("age");

        String odataFilter = String.format("%s le 50", expected.term.propertyName);
        Query actual = toQuery(odataFilter);

        assertQueriesEqual(actual, expected);
    }

    @Test
    public void testSimpleDoubleLessThanOrEqualQuery() throws Throwable {
        Query expected = new Query();

        // The test $filter is (age le 50.0).  This corresponds to a range query.
        NumericRange<Double> r = QueryTask.NumericRange.createDoubleRange(Double.MIN_VALUE,
                (double) 50, true, true);
        expected.setNumericRange(r);
        expected.setTermPropertyName("age");

        String odataFilter = String.format("%s le 50.0", expected.term.propertyName);
        Query actual = toQuery(odataFilter);

        assertQueriesEqual(actual, expected);
    }

    //
    @Test
    public void testSimpleLongGreaterThanQuery() throws Throwable {
        Query expected = new Query();

        // The test $filter is (age gt 50).  This corresponds to a range query.
        NumericRange<Long> r = QueryTask.NumericRange.createLongRange((long) 50,
                Long.MAX_VALUE,
                false, true);
        expected.setNumericRange(r);
        expected.setTermPropertyName("age");

        String odataFilter = String.format("%s gt 50", expected.term.propertyName);
        Query actual = toQuery(odataFilter);

        assertQueriesEqual(actual, expected);
    }

    @Test
    public void testSimpleDoubleGreaterThanQuery() throws Throwable {
        Query expected = new Query();

        // The test $filter is (age lt 50.0).  This corresponds to a range query.
        NumericRange<Double> r = QueryTask.NumericRange.createDoubleRange((double) 50,
                Double.MAX_VALUE,
                false, true);
        expected.setNumericRange(r);
        expected.setTermPropertyName("age");

        String odataFilter = String.format("%s gt 50.0", expected.term.propertyName);
        Query actual = toQuery(odataFilter);

        assertQueriesEqual(actual, expected);
    }

    @Test
    public void testSimpleLongGreaterThanOrEqualQuery() throws Throwable {
        Query expected = new Query();

        // The test $filter is (age ge 50).  This corresponds to a range query.
        NumericRange<Long> r = QueryTask.NumericRange.createLongRange((long) 50,
                Long.MAX_VALUE, true, true);
        expected.setNumericRange(r);
        expected.setTermPropertyName("age");

        String odataFilter = String.format("%s ge 50", expected.term.propertyName);
        Query actual = toQuery(odataFilter);

        assertQueriesEqual(actual, expected);
    }

    @Test
    public void testSimpleDoubleGreaterThanOrEqualQuery() throws Throwable {
        Query expected = new Query();

        // The test $filter is (age ge 50.0).  This corresponds to a range query.
        NumericRange<Double> r = QueryTask.NumericRange.createDoubleRange((double) 50,
                Double.MAX_VALUE, true, true);
        expected.setNumericRange(r);
        expected.setTermPropertyName("age");

        String odataFilter = String.format("%s ge 50.0", expected.term.propertyName);
        Query actual = toQuery(odataFilter);

        assertQueriesEqual(actual, expected);
    }

    @Test
    public void testSimpleLongEqualQuery() throws Throwable {
        Query expected = new Query();

        // The test $filter is (age eq 50).
        NumericRange<Long> r = QueryTask.NumericRange.createLongRange((long) 50,
                (long) 50, true, true);
        expected.setNumericRange(r);
        expected.setTermPropertyName("age");

        String odataFilter = String.format("%s eq 50", expected.term.propertyName);
        Query actual = toQuery(odataFilter);

        assertQueriesEqual(actual, expected);
    }

    @Test
    public void testSimpleDoubleEqualQuery() throws Throwable {
        Query expected = new Query();

        // The test $filter is (age eq 50.0).
        NumericRange<Double> r = QueryTask.NumericRange.createDoubleRange((double) 50,
                (double) 50, true, true);
        expected.setNumericRange(r);
        expected.setTermPropertyName("age");

        String odataFilter = String.format("%s eq 50.0", expected.term.propertyName);
        Query actual = toQuery(odataFilter);

        assertQueriesEqual(actual, expected);
    }

    @Test
    public void testBooleanWithNumericRanges() throws Throwable {
        Query expected = new Query();

        // The test  ($filter name eq 'foo') OR (($filter age le 50) AND ($filter income ge 1000000))

        // Top-level OR
        expected.occurance = Query.Occurance.SHOULD_OCCUR;

        // first term
        Query nameQ = new Query().setTermPropertyName("name").setTermMatchValue("foo");

        // second term
        Query betweenRangesQ = new Query();

        // first range
        NumericRange<?> lessThan50 = QueryTask.NumericRange
                .createLessThanOrEqualRange((long) 50);
        betweenRangesQ.addBooleanClause(new Query().setTermPropertyName("age").setNumericRange(
                lessThan50));

        // second range
        NumericRange<?> greatherThanOrEqual1M = QueryTask.NumericRange
                .createGreaterThanOrEqualRange((long)
                1000000);
        betweenRangesQ.addBooleanClause(new Query().setTermPropertyName("income").setNumericRange
                (greatherThanOrEqual1M));

        expected.addBooleanClause(nameQ);
        expected.addBooleanClause(betweenRangesQ);

        String odataFilter = "(name eq foo) or ((age le 50) and (income ge 1000000))";
        Query actual = toQuery(odataFilter);

        assertQueriesEqual(actual, expected);
    }

    private static Query toQuery(String expression) {
        return new ODataQueryVisitor().toQuery(expression);
    }

    private static void assertQueriesEqual(Query actual, Query expected) {
        assertEquals(actual.occurance, expected.occurance);

        if (expected.term == null) {
            assertNull(actual.term);
        } else {
            assertNotNull(actual.term);

            assertEquals(actual.term.propertyName, expected.term.propertyName);
            assertEquals(actual.term.matchValue, expected.term.matchValue);
            assertEquals(actual.term.matchType, expected.term.matchType);

            if (expected.term.range == null) {
                assertNull(actual.term.range);
            } else {
                assertNotNull(actual.term.range);
                assertEquals(actual.term.range.type, expected.term.range.type);
                assertEquals(actual.term.range.min, expected.term.range.min);
                assertEquals(actual.term.range.max, expected.term.range.max);
                assertEquals(actual.term.range.isMinInclusive, expected.term.range.isMinInclusive);
                assertEquals(actual.term.range.isMaxInclusive, expected.term.range.isMaxInclusive);
            }
        }

        if (expected.booleanClauses == null) {
            assertNull(actual.booleanClauses);
        } else {
            assertNotNull(actual.booleanClauses);
            assertEquals(actual.booleanClauses.size(), expected.booleanClauses.size());

            for (int i = 0; i < expected.booleanClauses.size(); i++) {
                Query a = actual.booleanClauses.get(i);
                Query e = expected.booleanClauses.get(i);

                assertQueriesEqual(a, e);
            }
        }
    }
}
