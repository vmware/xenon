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

package com.vmware.xenon.services.common;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatelessService;

/**
 * This service represents the guest user.
 *
 * It only exists to occupy a GET-able URI on every service host, such that it is
 * possible to assert the identity of the guest user without having to extend
 * the set of claims with an "isGuestUser" field, or something along those lines.
 *
 * @see {@link SystemUserService}
 */
public class GuestUserService extends StatelessService {
    public static final String SELF_LINK = ServiceUriPaths.CORE_AUTHZ_GUEST_USER;

    public void handleGet(Operation op) {
        ServiceDocument state = new ServiceDocument();
        state.documentSelfLink = SELF_LINK;
        op.setBody(state).complete();
    }
}
