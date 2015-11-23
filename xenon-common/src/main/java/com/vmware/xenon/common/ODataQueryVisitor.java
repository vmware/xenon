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

import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QueryTerm;

public class ODataQueryVisitor {

    public enum BinaryVerb {
        AND("and"), OR("or"),
        EQ("eq"), NE("ne"), LT("lt"), LE("le"), GT("gt"), GE("ge"),
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

    private static final IllegalArgumentException LeftRightTypeException = new IllegalArgumentException(
            "left and right side type mismatch");

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
            left = visitBinary(left, stringToVerb(tokens.next().getUriLiteral()),
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

        return visitBinary(left.getUriLiteral(), stringToVerb(verb.getUriLiteral()),
                right.getUriLiteral());
    }

    // Either return a unary query term, or a query term with 2 boolean queries (which may have other queries within).
    private Query visitBinary(final Object leftSide,
            final BinaryVerb operator,
            final Object rightSide) {

        Query q = new Query();

        // handle the operator by setting the query occurance.
        q.occurance = convertToLuceneOccur(operator);

        // Handle a nested query. We return a query with left and right attached as boolean queries.
        if (leftSide instanceof Query) {
            if (!(rightSide instanceof Query)) {
                throw LeftRightTypeException;
            }
            q.addBooleanClause((Query) leftSide);
            q.addBooleanClause((Query) rightSide);
            return q;
        }

        // If the left side is a String, the right side can only be a String or a Range.
        // If the left side is a Query, the right side can only be a Query.
        if (rightSide instanceof Query) {
            throw LeftRightTypeException;
        }
        q.setTermPropertyName((String) leftSide);

        if (((String) leftSide).contains(UriUtils.URI_WILDCARD_CHAR)) {
            q.setTermMatchType(QueryTerm.MatchType.WILDCARD);
        }

        if (rightSide instanceof String) {
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

        } else {
            // We don't know what type this is.
            throw LeftRightTypeException;
        }

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
}
