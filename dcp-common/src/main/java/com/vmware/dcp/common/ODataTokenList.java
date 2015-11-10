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

package com.vmware.dcp.common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ODataTokenList implements Iterator<ODataToken> {
    /**
     * The following is adapted from the Apache olingo-odata2 project
     * (https://github.com/apache/olingo-odata2).
     */
    private ArrayList<ODataToken> tokens = null;
    int currentODataToken = 0;

    public ODataTokenList() {
        this.tokens = new ArrayList<ODataToken>();
    }

    /**
     * Append StringValue ODataToken to tokens parameter
     * @param position Position of parsed token
     * @param kind Kind of parsed token
     * @param uriLiteral String value of parsed token
     */
    public void appendODataToken(final int position, final ODataToken.ODataTokenKind kind,
            final String uriLiteral) {
        ODataToken token = new ODataToken(kind, position, uriLiteral);
        this.tokens.add(token);
        return;
    }

    /**
     * Append CharValue ODataToken to tokens parameter
     * @param position Position of parsed token
     * @param kind Kind of parsed token
     * @param charValue Char value of parsed token
     */
    public void appendODataToken(final int position, final ODataToken.ODataTokenKind kind,
            final char charValue) {
        ODataToken token = new ODataToken(kind, position, Character.toString(charValue));
        this.tokens.add(token);
        return;
    }

    /**
     * Return the current token.
     * @return
     */
    public ODataToken lookToken() {
        if (this.currentODataToken >= this.tokens.size()) {
            throw new NoSuchElementException();
        }

        return this.tokens.get(this.currentODataToken);
    }

    /**
     * Return the current token, advance ptr.
     * @return
     */
    @Override
    public ODataToken next() {
        if (this.currentODataToken >= this.tokens.size()) {
            throw new NoSuchElementException();
        }

        ODataToken ret = this.tokens.get(this.currentODataToken);
        this.currentODataToken++;
        return ret;
    }

    /**
     * Return the previous token.
     * @return
     */
    public ODataToken lookPrevToken() {
        if (this.currentODataToken - 1 < 0) {
            return null;
        }

        return this.tokens.get(this.currentODataToken - 1);
    }

    public void skip() {
        this.currentODataToken++;
    }

    @Override
    public boolean hasNext() {
        return (this.currentODataToken < this.tokens.size());
    }

    @Override
    public void remove() {
        throw new IllegalArgumentException("Method not allowed");
    }

    public ODataToken elementAt(final int index) {

        return this.tokens.get(index);
    }

    public boolean hasTokens() {
        return (this.tokens.size() > 0);
    }

    public int tokenCount() {
        int i = this.tokens.size();

        return i;
    }
}
