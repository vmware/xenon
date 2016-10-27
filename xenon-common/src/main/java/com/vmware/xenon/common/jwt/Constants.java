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

package com.vmware.xenon.common.jwt;

import java.nio.charset.Charset;

public final class Constants {
    private Constants() {

    }

    protected static final Algorithm DEFAULT_ALGORITHM = Algorithm.HS256;

    protected static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    protected static final String JWT_TYPE = "JWT";

    protected static final char JWT_SEPARATOR = '.';
}
