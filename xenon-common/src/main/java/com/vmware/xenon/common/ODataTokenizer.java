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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.vmware.xenon.common.ODataToken.ODataTokenKind;

public class ODataTokenizer {
    /**
     * The following is adapted from the Apache olingo-odata2 project
     * (https://github.com/apache/olingo-odata2).
     */
    private static final Pattern BIN_OP = Pattern.compile("^(and|or) ");
    private static final Pattern BIN_COMP = Pattern.compile("^(eq|ne|lt|gt|le|ge|any|all) ");
    private static final Pattern OTHER_LIT = Pattern
            .compile("(?:\\p{L}|\\p{Digit}|[-._~%!$&*+;:@])+");

    // Both "/" and "." separators for specifying nested properties are supported.
    private static final String ODATA_NESTED_SEPARATOR = "/";
    private static final String DEFAULT_NESTED_SEPARATOR = ".";

    int curPosition;
    final String expression;
    final int expressionLength;
    ODataTokenList tokens;

    public ODataTokenizer(final String expression) {
        this.expression = expression;
        this.expressionLength = expression.length();
        this.tokens = new ODataTokenList();
    }

    /**
     * Tokenizes an expression as defined per OData specification
     * @return Token list
     */
    public ODataTokenList tokenize() throws IllegalArgumentException {
        this.curPosition = 0;
        int oldPosition;
        char curCharacter;
        String token = "";

        while (this.curPosition < this.expressionLength) {
            oldPosition = this.curPosition;

            curCharacter = this.expression.charAt(this.curPosition);
            switch (curCharacter) {
            case ' ':
                // count whitespace and move pointer to next non-whitespace char
                eatWhiteSpaces(curCharacter);
                break;

            case '(':
                this.tokens.appendODataToken(this.curPosition, ODataTokenKind.OPENPAREN,
                        curCharacter);
                this.curPosition = this.curPosition + 1;

                break;

            case ')':
                this.tokens.appendODataToken(this.curPosition, ODataTokenKind.CLOSEPAREN,
                        curCharacter);
                this.curPosition = this.curPosition + 1;
                break;

            case '\'':
                token = "";
                readLiteral(curCharacter);

                break;

            case ',':
                this.tokens.appendODataToken(oldPosition, ODataTokenKind.COMMA, curCharacter);
                this.curPosition = this.curPosition + 1;
                break;

            case '=':
            case '?':
                // Treat star (*) and periods (.) as literals rather than symbols to support WILD_CARD queries and phrases
                // with dots (e.g. IPs).
                this.curPosition = this.curPosition + 1;
                this.tokens.appendODataToken(oldPosition, ODataTokenKind.SYMBOL, curCharacter);
                break;

            default:
                String rem_expr = this.expression.substring(this.curPosition); // remaining this.expression

                boolean isBinary = checkForBinary(oldPosition, rem_expr);
                if (isBinary) {
                    break;
                }

                boolean isAndOr = checkForAndOrOperator(oldPosition, rem_expr);
                if (isAndOr) {
                    break;
                }

                boolean isBoolean = checkForBoolean(oldPosition, rem_expr);
                if (isBoolean) {
                    break;
                }

                boolean isLiteral = checkForLiteral(oldPosition, curCharacter, rem_expr);
                if (isLiteral) {
                    break;
                }

                token = Character.toString(curCharacter);
                throw new IllegalArgumentException("Unknown character: " + oldPosition + ' '
                        + token
                        + ' ' + this.expression);
            }
        }
        return this.tokens;
    }

    private boolean checkForLiteral(final int oldPosition, final char curCharacter,
            final String rem_expr) {
        String normalizedExpr = rem_expr.replace(ODATA_NESTED_SEPARATOR, DEFAULT_NESTED_SEPARATOR);
        final Matcher matcher = OTHER_LIT.matcher(normalizedExpr);
        boolean isLiteral = false;
        if (matcher.lookingAt()) {
            String token = matcher.group();
            this.curPosition = this.curPosition + token.length();
            // It is a simple type.
            this.tokens.appendODataToken(oldPosition, ODataTokenKind.SIMPLE_TYPE, token);
            isLiteral = true;
        }
        return isLiteral;
    }

    private boolean checkForBoolean(final int oldPosition, final String rem_expr) {
        boolean isBoolean = false;
        if (rem_expr.equals("true") || rem_expr.equals("false")) {
            this.curPosition = this.curPosition + rem_expr.length();
            this.tokens.appendODataToken(oldPosition, ODataTokenKind.SIMPLE_TYPE, rem_expr);
            isBoolean = true;
        }
        return isBoolean;
    }

    private void eatWhiteSpaces(char curCharacter) {
        while ((curCharacter == ' ') && (this.curPosition < this.expressionLength)) {
            this.curPosition = this.curPosition + 1;
            if (this.curPosition < this.expressionLength) {
                curCharacter = this.expression.charAt(this.curPosition);
            }
        }
    }

    private boolean checkForBinary(final int oldPosition, final String rem_expr) {
        boolean isBinary = false;
        Matcher matcher1 = BIN_COMP.matcher(rem_expr);
        if (matcher1.find()) {
            String token = matcher1.group(1);
            this.curPosition = this.curPosition + token.length();
            this.tokens.appendODataToken(oldPosition, ODataTokenKind.BINARY_COMPARISON, token);
            isBinary = true;
        }
        return isBinary;
    }

    private boolean checkForAndOrOperator(final int oldPosition, final String rem_expr) {
        boolean isBinary = false;
        Matcher matcher1 = BIN_OP.matcher(rem_expr);
        if (matcher1.find()) {
            String token = matcher1.group(1);
            this.curPosition = this.curPosition + token.length();
            this.tokens.appendODataToken(oldPosition, ODataTokenKind.BINARY_OPERATOR, token);
            isBinary = true;
        }
        return isBinary;
    }

    private void readLiteral(final char curCharacter) {
        readLiteral(curCharacter, "");
    }

    /**
     * Read up to single ' and move pointer to the following char and tries a type detection
     * @param curCharacter
     * @param token
     */
    private void readLiteral(char curCharacter, String token) throws IllegalArgumentException {
        int offsetPos = -token.length();
        int oldPosition = this.curPosition;

        StringBuilder sb = new StringBuilder();
        sb.append(token).append(Character.toString(curCharacter));
        this.curPosition = this.curPosition + 1;

        boolean wasApostroph = false; // leading ' does not count
        while (this.curPosition < this.expressionLength) {
            curCharacter = this.expression.charAt(this.curPosition);

            if (curCharacter != '\'') {
                if (wasApostroph) {
                    break;
                }
                wasApostroph = false;
            } else {
                // a double ' is a normal character
                wasApostroph = !wasApostroph;
            }
            sb.append(curCharacter);
            this.curPosition = this.curPosition + 1;
        }

        if (!wasApostroph) {
            throw new IllegalArgumentException("undetermined string " + this.expression);
        }

        this.tokens.appendODataToken(oldPosition + offsetPos, ODataTokenKind.SIMPLE_TYPE,
                sb.toString());
    }
}
