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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ServiceHostLogService.LogServiceState;

public class TestServiceHostLogService extends BasicReusableHostTestCase {
    private static final String LOG_MESSAGE = "this is log message number ";

    private URI uri;

    @Before
    public void waitForServices() throws Throwable {
        this.uri = UriUtils.buildUri(this.host, ServiceUriPaths.PROCESS_LOG);
        this.host.testStart(3);
        this.host.registerForServiceAvailability(this.host.getCompletion(),
                ServiceUriPaths.PROCESS_LOG);
        this.host.registerForServiceAvailability(this.host.getCompletion(),
                ServiceUriPaths.SYSTEM_LOG);
        this.host.registerForServiceAvailability(this.host.getCompletion(),
                ServiceUriPaths.GO_PROCESS_LOG);
        this.host.testWait();
    }

    private int publish(int count) {
        int size = 0;

        for (int i = 0; i < count; i++) {
            String msg = LOG_MESSAGE + i;
            host.log("%s", msg);
            size += msg.length() + 1;
        }

        return size;
    }

    @Test
    public void get() throws Throwable {
        int count = 100;
        publish(count);

        this.host.testStart(1);
        Operation get = Operation
                .createGet(this.uri)
                .setCompletion(this.host.getCompletion());
        this.host.send(get);
        this.host.testWait();

        ServiceHostLogService.LogServiceState serviceState =
                this.host.getServiceState(null, ServiceHostLogService.LogServiceState.class,
                        this.uri);

        List<String> items = serviceState.items;
        assertEquals(serviceState.documentSelfLink, this.uri.getPath());
        assertEquals(serviceState.documentKind, Utils.buildKind(LogServiceState.class));

        int last = count - 1;
        boolean foundFirst = false;
        boolean foundLast = false;
        for (String item : items) {
            if (item.contains(LOG_MESSAGE + 0)) {
                foundFirst = true;
            }
            if (item.contains(LOG_MESSAGE + last)) {
                foundLast = true;
            }
        }

        assertTrue(foundFirst);
        assertTrue(foundLast);

        int tailCount = count / 2;
        URI uri = new URI(this.uri.toString() + "?lineCount=" + tailCount);

        this.host.testStart(1);
        get = Operation
                .createGet(uri)
                .setCompletion(this.host.getCompletion());
        this.host.send(get);
        this.host.testWait();

        serviceState =
                this.host.getServiceState(null, ServiceHostLogService.LogServiceState.class, uri);

        items = serviceState.items;
        assertEquals(tailCount, items.size());

        last = count - 1;
        foundLast = false;
        for (String item : items) {
            if (item.contains(LOG_MESSAGE + last)) {
                foundLast = true;
                break;
            }
        }

        assertTrue("found last test log message", foundLast);
    }

    @Test
    public void testSystemLog() throws Throwable {
        URI uri = UriUtils.buildUri(this.host, ServiceUriPaths.SYSTEM_LOG);

        if (!new File(ServiceHostLogService.DEFAULT_SYSTEM_LOG_NAME).canRead()) {
            Logger.getAnonymousLogger().info(ServiceUriPaths.SYSTEM_LOG + " is not readable");
            return;
        }

        this.host.testStart(1);
        Operation get = Operation
                .createGet(uri)
                .setCompletion(this.host.getCompletion());
        this.host.send(get);
        this.host.testWait();

        ServiceHostLogService.LogServiceState serviceState =
                this.host.getServiceState(null, ServiceHostLogService.LogServiceState.class, uri);

        assertNotNull(serviceState.items);
    }
}
