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

import java.net.URI;

import org.mozilla.javascript.ScriptableObject;
import org.mozilla.javascript.annotations.JSGetter;
import org.mozilla.javascript.annotations.JSSetter;

/**
 * Rudimentary &#060;a&#062; element implementation.
 */
public class JsA extends ScriptableObject {
    private static final long serialVersionUID = 0L;

    public static final String CLASS_NAME = "A";

    private URI href;

    @Override
    public String getClassName() {
        return CLASS_NAME;
    }

    @JSGetter
    public String getHref() {
        return this.href.toString();
    }

    @JSSetter
    public void setHref(String href) {
        this.href = URI.create(href);
    }

    /**
     * Simulates behavior of &#060;a&#062; element pathname property.
     *
     * @return {@link #href} path portion.
     */
    @JSGetter
    public String getPathname() {
        return this.href.getPath();
    }
}
