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

import java.util.Collections;
import java.util.Set;

import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QueryTerm;

public class ODataQueryVisitor {

    public enum BinaryVerb {
        AND("and"), OR("or"),
        EQ("eq"), NE("ne"), LT("lt"), LE("le"), GT("gt"), GE("ge"),
        ALL("all"), ANY("any"),
        // we don't actually support these
        ADD("add"), SUB("sub"),
        MUL("mul"), DIV("div"), MODULO("mod");

        private String operator;

        private BinaryVerb(final String op) {
            this.operator = op;
        }

        public boolean equals(String str) {
            return this.operator.equals(str);
        }
    }

    private static final String DEFAULT_COLLECTION_ITEM_SEPARATOR = ";";
    private static final IllegalArgumentException LeftRightTypeException = new IllegalArgumentException(
            "left and right side type mismatch");

    private Set<String> wildcardUnfoldPropertyNames;

    public ODataQueryVisitor() {
        this(Collections.emptySet());
    }

    /**
     * Constructs ODataQueryVisitor with optional set of property names that will be used when we
     * visit a term property name matching {@link UriUtils#URI_WILDCARD_CHAR}. In this case
     * the query will be expanded with multiple OR sub-queries for each property name of
     * {@link #wildcardUnfoldPropertyNames} instead of the wildcard.
     */
    public ODataQueryVisitor(Set<String> wildcardUnfoldPropertyNames) {
        this.wildcardUnfoldPropertyNames = wildcardUnfoldPropertyNames;
    }

    public Query toQuery(String filterExp) {
        ODataTokenizer tokenizer = new ODataTokenizer(filterExp);
        tokenizer.tokenize();

        if (tokenizer.tokens.hasTokens()) {
            return walkTokens(tokenizer.tokens, null);
        }

        return null;
    }

    /**
     * Start from the left and walk to the right recursively.
     * @param tokens idempotent list of tokens.
     * @param left Query so far
     * @return
     */
    public Query walkTokens(ODataTokenList tokens, Query left) {
        if (!tokens.hasNext()) {
            return null;
        }

        // if paren, create a new query and advance the list
        if (tokens.lookToken().getKind().equals(ODataToken.ODataTokenKind.OPENPAREN)) {
            // advance past the paren
            tokens.skip();

            left = walkTokens(tokens, left);
        }

        if (tokens.hasNext()
                && tokens.lookToken().getKind().equals(ODataToken.ODataTokenKind.SIMPLE_TYPE)) {
            left = parseUnaryTerm(tokens);
        }

        if (tokens.hasNext()
                && tokens.lookToken().getKind().equals(ODataToken.ODataTokenKind.CLOSEPAREN)) {
            tokens.skip();
            return left;
        }

        if (tokens.hasNext()
                && tokens.lookToken().getKind().equals(ODataToken.ODataTokenKind.BINARY_OPERATOR)) {
            left = visitBinaryOperator(left, stringToVerb(tokens.next().getUriLiteral()),
                    walkTokens(tokens, null));
        }

        if (tokens.hasNext()) {
            left = walkTokens(tokens, left);
        }
        return left;
    }

    /**
     * Parse a single term;
     *
     * @param tokens
     * @return Query of single term
     */
    public Query parseUnaryTerm(ODataTokenList tokens) throws IllegalArgumentException {
        ODataToken left;
        ODataToken right;
        ODataToken verb;

        if (tokens.lookToken().getKind().equals(ODataToken.ODataTokenKind.SIMPLE_TYPE)) {
            left = tokens.next();
        } else {
            throw new IllegalArgumentException("Term mismatch");
        }

        if (tokens.lookToken().getKind().equals(ODataToken.ODataTokenKind.BINARY_COMPARISON)) {
            verb = tokens.next();
        } else {
            throw new IllegalArgumentException("Term mismatch");
        }

        if (tokens.lookToken().getKind().equals(ODataToken.ODataTokenKind.SIMPLE_TYPE)) {
            right = tokens.next();
        } else {
            throw new IllegalArgumentException("Term mismatch");
        }

        return visitBinaryComparator(left.getUriLiteral(), stringToVerb(verb.getUriLiteral()),
                right.getUriLiteral());
    }

    // return a unary query term.
    private Query visitBinaryComparator(final Object leftSide,
            final BinaryVerb operator,
            final Object rightSide) {

        Query q = new Query();

        // handle the operator by setting the query occurance.
        q.occurance = convertToLuceneOccur(operator);

        if (rightSide instanceof Query) {
            throw LeftRightTypeException;
        }

        if (isUnfoldQuery(leftSide, operator, rightSide)) {
            for (String name : this.wildcardUnfoldPropertyNames) {
                Query innerQuery = visitBinaryComparator(name, operator, rightSide);
                innerQuery.occurance = Query.Occurance.SHOULD_OCCUR;
                q.addBooleanClause(innerQuery);
            }

            return q;
        }

        if (rightSide instanceof String) {
            if (operator == BinaryVerb.ANY || operator == BinaryVerb.ALL) {
                Query.Occurance itemOccurance = Query.Occurance.MUST_OCCUR;
                if (operator == BinaryVerb.ANY) {
                    itemOccurance = Query.Occurance.SHOULD_OCCUR;
                }
                String[] itemNames = ((String) rightSide).split(DEFAULT_COLLECTION_ITEM_SEPARATOR);
                for (String itemName : itemNames) {
                    if (!itemName.isEmpty()) {
                        Query itemClause = new Query();
                        itemClause.setTermPropertyName((String) leftSide);
                        itemClause.occurance = itemOccurance;
                        itemClause.setTermMatchValue(itemName.replace("\'", "").trim());
                        if (itemName.contains(UriUtils.URI_WILDCARD_CHAR)) {
                            itemClause.setTermMatchType(QueryTerm.MatchType.WILDCARD);
                        }
                        q.addBooleanClause(itemClause);
                    }
                }
            } else {
                q.setTermPropertyName((String) leftSide);

                if (((String) leftSide).contains(UriUtils.URI_WILDCARD_CHAR)) {
                    q.setTermMatchType(QueryTerm.MatchType.WILDCARD);
                }
                // Handle numeric ranges
                if (isNumeric((String) rightSide)) {
                    // create a rangeA
                    QueryTask.NumericRange<?> r = createRange(rightSide.toString(), operator);
                    q.setNumericRange(r);

                } else {
                    q.setTermMatchValue(((String) rightSide).replace("\'", ""));

                    if (((String) rightSide).contains("*")) {
                        q.setTermMatchType(QueryTerm.MatchType.WILDCARD);
                    }
                }
            }
        } else {
            // We don't know what type this is.
            throw LeftRightTypeException;
        }

        return q;
    }

    // Return a query term with 2 boolean queries (which may have other queries within).
    private Query visitBinaryOperator(final Query leftSide,
            final BinaryVerb operator,
            final Query rightSide) {

        Query q = new Query();

        switch (operator) {
        case AND:
            /*
             * AND will set the left, right queries to MUST
             * unless already set MUST_NOT
             */
            if (leftSide.occurance != Query.Occurance.MUST_NOT_OCCUR) {
                leftSide.occurance = Query.Occurance.MUST_OCCUR;
            }

            if (rightSide.occurance != Query.Occurance.MUST_NOT_OCCUR) {
                rightSide.occurance = Query.Occurance.MUST_OCCUR;
            }

            if (leftSide.occurance == Query.Occurance.MUST_NOT_OCCUR &&
                    rightSide.occurance == Query.Occurance.MUST_NOT_OCCUR) {
                q.occurance = Query.Occurance.MUST_NOT_OCCUR;
                leftSide.occurance = Query.Occurance.SHOULD_OCCUR;
                rightSide.occurance = Query.Occurance.SHOULD_OCCUR;
            }
            break;
        case OR:
            /*
             * OR will set the left, right queries to SHOULD
             * unless already set to MUST_NOT
             */
            if (leftSide.occurance != Query.Occurance.MUST_NOT_OCCUR) {
                leftSide.occurance = Query.Occurance.SHOULD_OCCUR;
            }

            if (rightSide.occurance != Query.Occurance.MUST_NOT_OCCUR) {
                rightSide.occurance = Query.Occurance.SHOULD_OCCUR;
            }

            if (leftSide.occurance == Query.Occurance.MUST_NOT_OCCUR &&
                    rightSide.occurance == Query.Occurance.MUST_NOT_OCCUR) {
                q.occurance = Query.Occurance.MUST_NOT_OCCUR;
                leftSide.occurance = Query.Occurance.MUST_OCCUR;
                rightSide.occurance = Query.Occurance.MUST_OCCUR;
            }
            break;
        default:
            break;
        }

        q.addBooleanClause(leftSide);
        q.addBooleanClause(rightSide);

        return q;
    }

    private static Query.Occurance convertToLuceneOccur(BinaryVerb binaryOp) {
        if (binaryOp == null) {
            return Query.Occurance.MUST_OCCUR;
        }

        // We support the following operations on terms.
        // AND("and"), OR("or"), EQ("eq"), NE("ne"),

        // The rest of the operators don't have mappings to Queries.  In those cases,
        // they're MUST OCCUR since they're likely for a range.
        // LT("lt"), LE("le"), GT("gt"), GE("ge"),

        // These are unsupported
        // ADD("add"), SUB("sub"),
        // MUL("mul"), DIV("div"), MODULO("mod"),

        switch (binaryOp) {
        case OR:
            return Query.Occurance.SHOULD_OCCUR;
        case NE:
            return Query.Occurance.MUST_NOT_OCCUR;
        case AND:
        case EQ:
        case GT:
        case GE:
        case LT:
        case LE:
        case ANY:
        case ALL:
            return Query.Occurance.MUST_OCCUR;
        default:
            throw new IllegalArgumentException("unsupported operation");
        }
    }

    private static boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

    private QueryTask.NumericRange<?> createRange(String num, BinaryVerb op) {
        // if there's a decimal, treat as double
        Number d = null;
        if (num.contains(".")) {
            d = Double.parseDouble(num);
        } else {
            d = Long.parseLong(num);
        }

        if (op.equals(BinaryVerb.LT)) {
            return QueryTask.NumericRange.createLessThanRange(d);
        }

        if (op.equals(BinaryVerb.LE)) {
            return QueryTask.NumericRange.createLessThanOrEqualRange(d);
        }

        if (op.equals(BinaryVerb.GT)) {
            return QueryTask.NumericRange.createGreaterThanRange(d);
        }

        if (op.equals(BinaryVerb.GE)) {
            return QueryTask.NumericRange.createGreaterThanOrEqualRange(d);
        }

        if (op.equals(BinaryVerb.EQ)) {
            return QueryTask.NumericRange.createEqualRange(d);
        }

        return null;
    }

    private BinaryVerb stringToVerb(String s) {
        for (BinaryVerb v : BinaryVerb.values()) {
            if (v.equals(s)) {
                return v;
            }
        }

        return null;
    }

    private static boolean isUnfoldQuery(final Object leftSide, final BinaryVerb operator,
            final Object rightSide) {
        return ODataUtils.FILTER_VALUE_ALL_FIELDS.equals(leftSide)
                && BinaryVerb.EQ.equals(operator)
                && (rightSide instanceof String) && !isNumeric((String) rightSide);
    }
}
