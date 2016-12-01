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

package example.group;

import static example.group.QuickstartHost.startHost;

import java.net.URI;
import java.nio.file.Paths;
import java.util.logging.Level;

import example.group.EmployeeService.Employee;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceHost.Arguments;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestNodeGroupManager;
import com.vmware.xenon.services.common.QueryTask;

/**
 * Tests for the EmployeeService. Here we demonstrate:
 *      - How to start up Xenon for testing, single node and multi-node.
 *      - Methods for synchronously waiting for async operations in Xenon
 *      - Usage of POST, PUT, PATCH, DELETE
 *      - Usage of Queries (Query Tasks)
 *
 *   @see <a href="https://github.com/vmware/xenon/wiki/Testing-Tutorial">Testing Tutorial</a>
 *   @see <a href="https://github.com/vmware/xenon/wiki/Testing-Guide">Testing Guide</a>
 */
public class EmployeeServiceTest extends BasicTestCase {
    private static ServiceHost h;
    private static TemporaryFolder folder = new TemporaryFolder();
    private static URI employeeUri;

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
    @BeforeClass
    public static void startHosts() throws Throwable {
        int numNodes = 3;
        folder.create();
        Arguments args = new Arguments();
        QuickstartHost[] hostList = new QuickstartHost[numNodes];
        for (int i = 0; i < numNodes; i++) {
            args.id = "host" + i;   // human readable name instead of GUID
            args.sandbox = Paths.get(folder.getRoot().toString() + i);
            args.port = 0;
            hostList[i] = startHost(new String[0], args);
        }

        TestNodeGroupManager nodeGroup = new TestNodeGroupManager();
        for (QuickstartHost host : hostList) {
            nodeGroup.addHost(host);
        }

        // When you launch a cluster of nodes, they initiate a protocol to synchronize. If you start making changes
        // before the nodes are in sync, then your changes will trigger additional synchronization, which will take
        // longer for startup to complete.
        nodeGroup.waitForConvergence();
        h = nodeGroup.getHost();  // grabs a random one of the hosts.
        employeeUri = UriUtils.buildFactoryUri(h, EmployeeService.class);
    }

    @AfterClass
    public static void cleanup() {
        folder.delete();
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

        // Since creation of documents is async, even though the POST commands have returned, the documents
        // may not be created yet. This method will keep checking until timeout expires.
        TestUtils.checkServiceCountEquals(h, Employee.class, numEmployees, 10000);
        h.log(Level.INFO, "Created %d records in %d ms", numEmployees, timer.end());

        deleteTestEmployees(numEmployees, prefix);
        TestUtils.checkServiceCountEquals(h, Employee.class, 0, 10000);
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
        TestUtils.checkServiceCountEquals(h, Employee.class, numEmployees, 20000);

        TimeRange timer = new TimeRange();
        Employee employee = new Employee();
        for (int i = 0; i < numEmployees; i++) {
            employee.name = "PUT : " + prefix + ": " + i ;
            employee.documentSelfLink = prefix + i;
            Operation op = Operation.createPut(UriUtils.extendUri(employeeUri, employee.documentSelfLink))
                    .setBody(employee)
                    .setReferer(h.getUri());
            h.sendWithDeferredResult(op, Employee.class)
                    .whenComplete((emp, throwable) -> {
                        if (throwable != null) {
                            h.log(Level.WARNING, throwable.getMessage());
                        }
                    });
        }

        // We want to make sure that all the records updated by the PUTs. In the put we updated the employee.name
        // field to include the string '(PUT)', so it should be returned in a query.
        QueryTask.Query query = QueryTask.Query.Builder.create()
                .addKindFieldClause(Employee.class)
                .addFieldClause("name", "PUT", QueryTask.QueryTerm.MatchType.PREFIX)
                .build();

        TestUtils.waitUntilQueryResultsCountEquals(h, query, numEmployees, 20000);
        h.log(Level.INFO, "Updated %d records in %d ms", numEmployees, timer.end());

        deleteTestEmployees(numEmployees, prefix);
        TestUtils.checkServiceCountEquals(h, Employee.class, 0, 10000);
    }

    /**
     * Test the PATCH REST API. PATCH should only update the specified fields.
     */
    @Test
    public void testPatch()  {
        String prefix = "testPatch";
        int numEmployees = 100;
        createTestEmployees(numEmployees, prefix);
        TestUtils.checkServiceCountEquals(h, Employee.class, numEmployees, 20000);

        TimeRange timer = new TimeRange();
        Employee employee = new Employee();
        for (int i = 0; i < numEmployees; i++) {
            employee.name = "PATCH : " + prefix + ": " + i ;
            employee.documentSelfLink = prefix + i;
            Operation op = Operation.createPatch(UriUtils.extendUri(employeeUri, employee.documentSelfLink))
                    .setBody(employee)
                    .setReferer(h.getUri());
            h.sendWithDeferredResult(op, Employee.class)
                    .whenComplete((emp, throwable) -> {
                        if (throwable != null) {
                            h.log(Level.WARNING, throwable.getMessage());
                        }
                    });
        }

        // We want to make sure that all the records updated by the PUTs. In the put we updated the employee.name
        // field to include the string 'PATCH', so it should be returned in a query.
        QueryTask.Query query = QueryTask.Query.Builder.create()
                .addKindFieldClause(Employee.class)
                .addFieldClause("name", "PATCH", QueryTask.QueryTerm.MatchType.PREFIX)
                .build();

        TestUtils.waitUntilQueryResultsCountEquals(h, query, numEmployees, 20000);
        h.log(Level.INFO, "Updated %d records in %d ms", numEmployees, timer.end());

        deleteTestEmployees(numEmployees, prefix);
        TestUtils.checkServiceCountEquals(h, Employee.class, 0, 10000);
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
     * requests have been requested.  Please see TestUtils.checkServiceCountEquals(), which will wait for the index to show
     * the newly created users. Alternatively you could adjust this method to keep of count of completions.
     *
     * Because the creates are asynchronous, if any creates fail, only a message will be output to the logs. Again,
     * recommend using TestUtils.checkServiceCountEquals() to validate that all objects were created.
     *
     * @param numEmployees - number of employee records to create
     * @param prefix - will be prepended to the name and documentSelfLink of each document. Usually set to the name of
     *               the calling unit test method.
     */
    private void createTestEmployees(int numEmployees, String prefix) {
        Employee employee = new Employee();

        // First create the CEO
        employee.name = prefix + ": " + 0 + " (CEO)";
        employee.documentSelfLink = prefix + 0;
        Operation op = Operation.createPost(employeeUri)
                .setBody(employee)
                .setReferer(h.getUri());
        h.sendWithDeferredResult(op, Employee.class)
                .whenComplete((emp, throwable) -> {
                    if (throwable != null) {
                        h.log(Level.WARNING, throwable.getMessage());
                    }
                });

        // Now create the employees
        for (int i = 1; i < numEmployees; i++) {
            employee.name = prefix + ": " + i;
            employee.documentSelfLink = prefix + i;
            employee.managerLink = EmployeeService.FACTORY_LINK + '/' + prefix + 0;
            op = Operation.createPost(employeeUri)
                    .setBody(employee)
                    .setReferer(h.getUri());
            h.sendWithDeferredResult(op, Employee.class)
                    .whenComplete((emp, throwable) -> {
                        if (throwable != null) {
                            h.log(Level.WARNING, throwable.getMessage());
                        }
                    });
        }
    }

    /**
     * Deletes employees created by the createTestEmployees() method.
     *
     * @param numEmployees - number of employees to be deleted - should match what was passed to createTestEmployees
     * @param prefix - prefix of employees to be deleted - should match what was passed to deleteTestEmployees.
     */
    private void deleteTestEmployees(int numEmployees, String prefix) {
        for (int i = 0; i < numEmployees; i++) {
            Operation op = Operation.createDelete(UriUtils.extendUri(employeeUri, prefix + i))
                    .setReferer(h.getUri());
            h.sendWithDeferredResult(op, Employee.class)
                    .whenComplete((emp, throwable) -> {
                        if (throwable != null) {
                            h.log(Level.WARNING, throwable.getMessage());
                        }
                    });
        }
    }
}

