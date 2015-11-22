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

package com.vmware.xenon.common.test.websockets;

import org.mozilla.javascript.Context;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;
import org.mozilla.javascript.annotations.JSFunction;

/**
 * Rudimentary document emulation class used for ws-service-lib.js#getLocation() simulation
 */
public class JsDocument extends ScriptableObject {
    private static final long serialVersionUID = 0L;

    public static final String CLASS_NAME = "Document";

    /**
     * Creates emulated &#060;a&#062; element.
     *
     * @param name Element name. This argument is ignored and emulated &#060;a&#062; element is always created.
     * @return Created element.
     */
    @JSFunction
    public Scriptable createElement(String name) {
        return Context.getCurrentContext().newObject(this, JsA.CLASS_NAME);
    }

    @Override
    public String getClassName() {
        return CLASS_NAME;
    }
}
