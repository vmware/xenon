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

public class ODataToken {
    /**
     * The following is adapted from the Apache olingo-odata2 project
     * (https://github.com/apache/olingo-odata2).
     */
    public enum ODataTokenKind {
        /**
         * Indicates that the token is a whitespace character
         */
        WHITESPACE,

        /**
         * Indicates that the token is a '(' character
         */
        OPENPAREN,

        /**
         * Indicates that the token is a ')' character
         */
        CLOSEPAREN,

        /**
         * Indicates that the token is a ',' character
         */
        COMMA,

        /**
         * Indicates that the token is a typed literal. That may be a
         * String like 'TEST'
         * Double like '1.1D'
         * or any other Simple Type
         */
        SIMPLE_TYPE,

        /**
         * Indicates that the token is a single symbol. That may be a
         * '-', '=', '/', '?', '.' or a '*' character
         */
        SYMBOL,

        /**
         * Indicates that the token is a set of alphanumeric characters starting
         * with a letter
         */
        LITERAL,

        /**
         * Indicates a comparison
         */
        BINARY_COMPARISON,

        /**
         * Indicates a binary operator
         */
        BINARY_OPERATOR,

        UNKNOWN
    }

    private ODataTokenKind kind;
    private int position;
    private String uriLiteral;

    public ODataToken(final ODataTokenKind kind, final int position, final String uriLiteral) {
        this.kind = kind;
        this.position = position;
        this.uriLiteral = uriLiteral;
    }

    public ODataTokenKind getKind() {
        return this.kind;
    }

    public int getPosition() {
        return this.position;
    }

    public String getUriLiteral() {
        return this.uriLiteral;
    }
}
