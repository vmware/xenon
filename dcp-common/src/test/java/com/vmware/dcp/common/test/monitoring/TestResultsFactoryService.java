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

package com.vmware.dcp.common.test.monitoring;

import com.vmware.dcp.common.FactoryService;
import com.vmware.dcp.common.Service;
import com.vmware.dcp.common.test.TestResultServiceState;

/**
 * Service gathering test results across time. New test-cases posting to the factory should use
 * the document-id, so that the TestResultsService instance generated tracks globally a single,
 * particular test case.
 */
public class TestResultsFactoryService extends FactoryService {

    // For now, it's in monitoring; we could later move somewhere else
    public static final String SELF_LINK = "/monitoring/testResults";

    public TestResultsFactoryService() {
        super(TestResultServiceState.class);
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return new TestResultService();
    }
}
