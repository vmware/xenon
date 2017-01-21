/*
 * Copyright (c) 2016 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenonlabs;

import static org.junit.Assert.assertEquals;

import static com.vmware.xenonlabs.UpgradeDemo.createAndStartHost;

import java.net.URI;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestNodeGroupManager;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.services.common.QueryTask;

/**
 * Tests for the UpgradeDemoEmployeeService. Here we demonstrate:
 *      - How to start up Xenon for testing, single node and multi-node.
 *      - Methods for synchronously waiting for async operations in Xenon
 *      - Usage of POST, PUT, PATCH, DELETE
 *      - Usage of Queries (Query Tasks)
 *
 *   @see <a href="https://github.com/vmware/xenon/wiki/Testing-Tutorial">Testing Tutorial</a>
 *   @see <a href="https://github.com/vmware/xenon/wiki/Testing-Guide">Testing Guide</a>
 */
public class UpgradeDemoEmployeeServiceTest {
    private ServiceHost clientHost;
    private TestNodeGroupManager nodeGroupManager;
    private TemporaryFolder folder = new TemporaryFolder();
    private URI employeeUri;

    private class TimeRange {
        long startTime = System.currentTimeMillis();

        long end() {
            return System.currentTimeMillis() - this.startTime;
        }
    }

    /**
     * Start up a multi-node emulated cluster within a single JVM for all tests.
     * @throws Throwable - exception encountered while starting up
     */
    @Before
    public void startHosts() throws Throwable {
        int numNodes = 3;
        this.folder.create();
        ServiceHost[] hostList = new ServiceHost[numNodes];
        for (int i = 0; i < numNodes; i++) {
            String[] args = {
                    "--id=host" + i,
                    "--sandbox=" + Paths.get(this.folder.getRoot().toString() + i),
                    "--port=0",
            };
            hostList[i] = createAndStartHost(args);
        }

        TestNodeGroupManager nodeGroup = new TestNodeGroupManager();
        nodeGroup.addHosts(Arrays.asList(hostList));

        // When you launch a cluster of nodes, they initiate a protocol to synchronize. If you start making changes
        // before the nodes are in sync, then your changes will trigger additional synchronization, which will take
        // longer for startup to complete.
        nodeGroup.waitForConvergence();
        this.clientHost = nodeGroup.getHost();  // grabs a random one of the hosts.
        this.employeeUri = UriUtils.buildFactoryUri(this.clientHost, UpgradeDemoEmployeeService.class);
        this.nodeGroupManager = nodeGroup;
    }

    @After
    public void cleanup() {
        this.nodeGroupManager.getAllHosts().forEach((h) -> h.stop());
        this.folder.delete();
    }

    /**
     * Test creating a bunch of employee objects using POST.
     */
    @Test
    public void testPost() {
        String prefix = "testPost";
        int numEmployees = 100;

        TimeRange timer = new TimeRange();
        createTestEmployees(numEmployees, prefix);

        UpgradeDemoTestUtils.waitForServiceCountEquals(this.clientHost, UpgradeDemoEmployeeService.Employee.class, numEmployees);
        this.clientHost.log(Level.INFO, "Created %d records in %d ms", numEmployees, timer.end());

        deleteTestEmployees(numEmployees, prefix);
        UpgradeDemoTestUtils.waitForServiceCountEquals(this.clientHost, UpgradeDemoEmployeeService.Employee.class, 0);
    }

    /**
     * Test the PUT REST API - which replaces the old version with a new version. Note that PUT only works in Xenon
     * on pre-existing services.
     */
    @Test
    public void testPut()  {
        String prefix = "testPut";
        int numEmployees = 100;

        /*
         * PUT cannot be used to create new services, only replace the contents of existing ones.
         * https://github.com/vmware/xenon/wiki/Programming-Model#verb-semantics-and-associated-actions
         * So to test PUT, we have to create the instances via POST.
         */
        createTestEmployees(numEmployees, prefix);
        UpgradeDemoTestUtils.waitForServiceCountEquals(this.clientHost, UpgradeDemoEmployeeService.Employee.class, numEmployees);

        /*
         * In createTestEmployees, we demonstrate one pattern for synchronous testing that
         * leverages TestRequestSender. Here we demonstrate a different pattern that leverages
         * a TestContext (another pure testing construct) and sendWithDeferredResult, which
         * is the Xenon-optimized version of CompletableFuture
         */
        TestContext testContext = new TestContext(numEmployees, Duration.ofSeconds(10));
        TimeRange timer = new TimeRange();
        UpgradeDemoEmployeeService.Employee employee = new UpgradeDemoEmployeeService.Employee();
        for (int i = 0; i < numEmployees; i++) {
            employee.name = "PUT : " + prefix + ": " + i ;
            employee.location = "Palo Alto, CA";
            employee.documentSelfLink = prefix + i;
            Operation op = Operation.createPut(UriUtils.extendUri(this.employeeUri, employee.documentSelfLink))
                    .setBody(employee)
                    .setReferer(this.clientHost.getUri());
            this.clientHost.sendWithDeferredResult(op, UpgradeDemoEmployeeService.Employee.class)
                    .whenComplete((emp, throwable) -> {
                        if (throwable != null) {
                            testContext.fail(throwable);
                        }
                        testContext.complete();
                    });
        }
        testContext.await();

        // We want to make sure that all the records updated by the PUTs. In the put we updated the employee.name
        // field to include the string '(PUT)', so it should be returned in a query.
        QueryTask.Query query = QueryTask.Query.Builder.create()
                .addKindFieldClause(UpgradeDemoEmployeeService.Employee.class)
                .addFieldClause("name", "PUT", QueryTask.QueryTerm.MatchType.PREFIX)
                .build();

        UpgradeDemoTestUtils.waitUntilQueryResultsCountEquals(this.clientHost, query, numEmployees);
        this.clientHost.log(Level.INFO, "Updated %d records in %d ms", numEmployees, timer.end());

        deleteTestEmployees(numEmployees, prefix);
        UpgradeDemoTestUtils.waitForServiceCountEquals(this.clientHost, UpgradeDemoEmployeeService.Employee.class, 0);
    }

    /**
     * Test the PATCH REST API. PATCH should only update the specified fields.
     */
    @Test
    public void testPatch()  {
        String prefix = "testPatch";
        int numEmployees = 100;
        createTestEmployees(numEmployees, prefix);
        UpgradeDemoTestUtils.waitForServiceCountEquals(this.clientHost, UpgradeDemoEmployeeService.Employee.class, numEmployees);

        TestContext testContext = new TestContext(numEmployees, Duration.ofSeconds(10));
        TimeRange timer = new TimeRange();
        UpgradeDemoEmployeeService.Employee employee = new UpgradeDemoEmployeeService.Employee();
        for (int i = 0; i < numEmployees; i++) {
            employee.name = "PATCH : " + prefix + ": " + i ;
            employee.documentSelfLink = prefix + i;
            Operation op = Operation.createPatch(UriUtils.extendUri(this.employeeUri, employee.documentSelfLink))
                    .setBody(employee)
                    .setReferer(this.clientHost.getUri());
            this.clientHost.sendWithDeferredResult(op, UpgradeDemoEmployeeService.Employee.class)
                    .whenComplete((emp, throwable) -> {
                        if (throwable != null) {
                            testContext.fail(throwable);
                        }
                        testContext.complete();
                    });
        }
        testContext.await();

        // We want to make sure that all the records updated by the PUTs. In the put we updated the employee.name
        // field to include the string 'PATCH', so it should be returned in a query.
        QueryTask.Query query = QueryTask.Query.Builder.create()
                .addKindFieldClause(UpgradeDemoEmployeeService.Employee.class)
                .addFieldClause("name", "PATCH", QueryTask.QueryTerm.MatchType.PREFIX)
                .build();

        UpgradeDemoTestUtils.waitUntilQueryResultsCountEquals(this.clientHost, query, numEmployees);
        this.clientHost.log(Level.INFO, "Updated %d records in %d ms", numEmployees, timer.end());

        deleteTestEmployees(numEmployees, prefix);
        UpgradeDemoTestUtils.waitForServiceCountEquals(this.clientHost, UpgradeDemoEmployeeService.Employee.class, 0);
    }

    /**
     * Creates Employee documents of a predictable format. If prefix = "testPut", then this method will create
     * one Employees like:
     *              "name": "testPut: 0 (CEO)", "documentSelfLink": "testPut0"
     *
     *
     *              "name": "testPut: 1", "documentSelfLink": "testPut1", "managerLink": "testPut0"
     *              "name": "testPut: 2", "documentSelfLink": "testPut2", "managerLink": "testPut0"
     *              ...
     *
     * This function kicks off asynchronous calls, so it will likely return before all the create
     * requests have been requested.  Please see UpgradeDemoTestUtils.checkServiceCountEquals(), which will wait for the index to show
     * the newly created users. Alternatively you could adjust this method to keep of count of completions.
     *
     * Because the creates are asynchronous, if any creates fail, only a message will be output to the logs. Again,
     * recommend using UpgradeDemoTestUtils.checkServiceCountEquals() to validate that all objects were created.
     *
     * @param numEmployees - number of employee records to create
     * @param prefix - will be prepended to the name and documentSelfLink of each document. Usually set to the name of
     *               the calling unit test method.
     */
    private void createTestEmployees(int numEmployees, String prefix) {
        UpgradeDemoEmployeeService.Employee employee = new UpgradeDemoEmployeeService.Employee();

        // A useful xenon test helper class
        TestRequestSender sender = new TestRequestSender(this.clientHost);

        // First create the CEO
        employee.name = prefix + ": " + 0 + " (CEO)";
        employee.location = "Palo Alto, CA";
        employee.documentSelfLink = prefix + 0;
        Operation op = Operation.createPost(this.employeeUri)
                .setBody(employee)
                .setReferer(this.clientHost.getUri());

        // sendAndWait both waits for the callback, but also verifies that there is no
        // error or exception in the call before returning. This method will throw an exception
        // if anything is wrong with the call
        UpgradeDemoEmployeeService.Employee rsp = sender.sendAndWait(op, UpgradeDemoEmployeeService.Employee.class);
        assertEquals(employee.name, rsp.name);
        assertEquals(employee.managerLink, rsp.managerLink);
        assertEquals(employee.location, rsp.location);

        List<Operation> opList = new ArrayList<>(numEmployees);
        // Now create the employees
        for (int i = 1; i < numEmployees; i++) {
            employee.name = prefix + ": " + i;
            employee.documentSelfLink = prefix + i;
            employee.managerLink = UriUtils.buildUriPath(UpgradeDemoEmployeeService.FACTORY_LINK, prefix + 0);
            employee.location = "Seattle, WA";
            opList.add(
                    Operation.createPost(this.employeeUri)
                    .setBody(employee)
                    .setReferer(this.clientHost.getUri())
            );
        }

        // When passed a list of ops, sendAndWait() will wait for all of them and ensure that all
        // of them complete with success. It returns the deserialized responses to the
        // HTTP requests.
        List<UpgradeDemoEmployeeService.Employee> rspList  = sender.sendAndWait(opList, UpgradeDemoEmployeeService.Employee.class);
        for (int i = 1; i < numEmployees; i++) {
            employee.name = prefix + ": " + i;
            employee.documentSelfLink = prefix + i;
            employee.managerLink = UriUtils.buildUriPath(UpgradeDemoEmployeeService.FACTORY_LINK, prefix + 0);
            assertEquals(employee.name, rspList.get(i - 1).name);
            assertEquals(employee.managerLink, rspList.get(i - 1).managerLink);
            assertEquals(employee.location, rspList.get(i - 1).location);
            assertEquals(UriUtils.buildUriPath(UpgradeDemoEmployeeService.FACTORY_LINK, employee.documentSelfLink),
                    rspList.get(i - 1).documentSelfLink);
        }
    }

    /**
     * Deletes employees created by the createTestEmployees() method.
     *
     * @param numEmployees - number of employees to be deleted - should match what was passed to createTestEmployees
     * @param prefix - prefix of employees to be deleted - should match what was passed to deleteTestEmployees.
     */
    private void deleteTestEmployees(int numEmployees, String prefix) {
        TestRequestSender sender = new TestRequestSender(this.clientHost);
        List<Operation> opList = new ArrayList<>(numEmployees);

        for (int i = 0; i < numEmployees; i++) {
            opList.add(
                    Operation.createDelete(UriUtils.extendUri(this.employeeUri, prefix + i))
                    .setReferer(this.clientHost.getUri())
            );
        }
        sender.sendAndWait(opList);
    }
}

