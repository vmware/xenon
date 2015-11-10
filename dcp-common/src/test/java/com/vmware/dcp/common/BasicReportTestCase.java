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
import java.net.URISyntaxException;
import java.util.logging.Level;

import org.junit.Rule;
import org.junit.rules.TestWatcher;

import com.vmware.dcp.common.test.TestResultWatcher;

/**
 * Augment BasicTestCase with remote reporting functionality, using a TestWatcher
 * rule,  invoked after the  parent rules (e.g., temporary  folder creation, host
 * creation), but before any {@link  org.junit.Before} or {@link org.junit.After}
 * in the children classes
 */
public class BasicReportTestCase extends BasicTestCase {

    public static TestWatcher createTestWatcher(BasicReportTestCase reportTestCase) {
        // Should be an address:port pair (e.g., http://10.20.128.46:8000/samples/testResults)
        String rmf = System.getenv("DCP_REMOTE_MONITOR_FACTORY");
        if (rmf == null) {
            return new TestWatcher() {
            };
        }
        try {
            URI remoteUrl = new URI(rmf);
            return new TestResultWatcher(reportTestCase, remoteUrl);
        } catch (URISyntaxException e) {
            reportTestCase.host.log(Level.WARNING, e.toString());
            return new TestWatcher() {
            };
        }

    }

    @Rule
    public TestWatcher remoteWatchman = createTestWatcher(this);
}
