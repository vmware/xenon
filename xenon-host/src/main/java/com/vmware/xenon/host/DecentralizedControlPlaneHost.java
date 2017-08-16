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

package com.vmware.xenon.host;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;

import com.vmware.xenon.common.AuthorizationSetupHelper;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.RootNamespaceService;
import com.vmware.xenon.ui.UiService;

/**
 * Stand alone process entry point
 */
public class DecentralizedControlPlaneHost extends ServiceHost {

    public static class ServiceHostArguments extends Arguments {
        /**
         * The email address of a user that should be granted "admin" privileges to all services
         */
        public String adminUser = "admin@localhost";

        /**
         * (Required) The password of the adminUser
         */
        public String adminPassword;
    }

    public static class TimeStat {
        public String timestamp;
        public int count = 0;
    }

    private ServiceHostArguments args;

    public static void main(String[] args) throws Throwable {
        BufferedReader reader = null;
        FileInputStream in = null;

        List<TimeStat> stats = new ArrayList<>();

        List<String> livenessProbes = new ArrayList<>();

        try {
            in = new FileInputStream("/Users/dars/httpCalls.log");
            reader = new BufferedReader(new InputStreamReader(in));

            TimeStat currentStat = null;
            String line = reader.readLine();

            do {
//                String timeStamp = extractTimeStamp(line);
//                if (currentStat == null || !currentStat.timestamp.equals(timeStamp)) {
//                    if (currentStat != null) {
//                        stats.add(currentStat);
//                    }
//                    currentStat = new TimeStat();
//                    currentStat.timestamp = timeStamp;
//                    currentStat.count++;
//                } else {
//                    currentStat.count++;
//                }

                if (line.contains("/mgmt/about")) {
                    int start = line.lastIndexOf('[');
                    int end = line.lastIndexOf(']');
                    String[] tokens = line.substring(start + 1 , end - 1).split(" ");
                    if (tokens.length > 0) {
                        String time = tokens[4];
                        String value = time.substring(0, time.length() - 2);
                        if (Double.parseDouble(value) > 3000) {
                            livenessProbes.add(line);
                        }
                    }
                }
                line = reader.readLine();
            } while (line != null);
        }finally {
            if (in != null) {
                in.close();
            }
        }


        File fout = new File("/Users/dars/livenessProbe.log");
        FileOutputStream fos = new FileOutputStream(fout);

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

//        for (TimeStat stat : stats) {
//            bw.write(stat.timestamp + " " + stat.count);
//            bw.newLine();
//        }

        for (String line : livenessProbes) {
            bw.write(line);
            bw.newLine();
        }
        bw.close();

        DecentralizedControlPlaneHost h = new DecentralizedControlPlaneHost();
        h.initialize(args);
        h.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            h.log(Level.WARNING, "Host stopping ...");
            h.stop();
            h.log(Level.WARNING, "Host is stopped");
        }));
    }

    private static String extractTimeStamp(String logLine) {
        int start = logLine.indexOf('T');
        int end = logLine.indexOf('Z');
        return logLine.substring(start + 1, end - 4);
    }

    @Override
    public ServiceHost initialize(String[] args) throws Throwable {
        this.args = new ServiceHostArguments();
        super.initialize(args, this.args);

        if (this.args.adminPassword == null) {
            throw new IllegalStateException("adminPassword is required. specify \"--adminPassword=<pass>\" param");
        }

        return this;
    }

    @Override
    public ServiceHost start() throws Throwable {
        super.start();

        startDefaultCoreServicesSynchronously();

        setAuthorizationContext(this.getSystemAuthorizationContext());

        super.startService(new RootNamespaceService());

        // start an example factory for folks that want to experiment with service instances
        super.startFactory(ExampleService.class, ExampleService::createFactory);

        // Start UI service
        super.startService(new UiService());

        AuthorizationSetupHelper.create()
                .setHost(this)
                .setUserEmail(this.args.adminUser)
                .setUserPassword(this.args.adminPassword)
                .setIsAdmin(true)
                .start();

        setAuthorizationContext(null);

        return this;
    }
}
