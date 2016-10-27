/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common.test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.vmware.xenon.common.Utils;

public final class SystemUtils {

    private SystemUtils() {

    }

    public static class ProcessInfo {
        public Long parentPid;
        public String name;
        public Long pid;

        @Override
        public String toString() {
            return "ProcessInfo{" +
                    "this.parentPid=" + this.parentPid +
                    ", this.name='" + this.name + '\'' +
                    ", this.pid=" + this.pid +
                    '}';
        }
    }

    public static List<ProcessInfo> findUnixProcessInfoByName(String name) {
        return findUnixProcessInfo("-e", name);
    }

    public static ProcessInfo findUnixProcessInfoByPid(Long pid) {
        List<ProcessInfo> parent = findUnixProcessInfo(String.format("-p %d", pid),
                null);
        if (parent.size() != 1) {
            return null;
        }
        return parent.get(0);
    }

    private static List<ProcessInfo> findUnixProcessInfo(String filter,
            String name) {
        List<ProcessInfo> processes = new ArrayList<>();

        try {
            String line;
            String cmd = String.format("ps -o ppid,pid,ucomm %s", filter);
            Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader input = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), Utils.CHARSET));
            input.readLine(); // skip header
            while ((line = input.readLine()) != null) {
                String[] columns = line.trim().split("\\s+", 3);
                String ucomm = columns[2].trim();
                if (name != null && !ucomm.equalsIgnoreCase(name)) {
                    continue;
                }

                ProcessInfo info = new ProcessInfo();
                try {
                    info.parentPid = Long.parseLong(columns[0].trim());
                    info.pid = Long.parseLong(columns[1].trim());
                    info.name = ucomm;
                    processes.add(info);
                } catch (Throwable e) {
                    continue;
                }
            }
            input.close();
        } catch (Throwable err) {
            // ignore
        }

        return processes;
    }

    public static void killUnixProcess(Long pid) {
        try {
            Runtime.getRuntime().exec("kill " + pid);
        } catch (Throwable e) {
        }
    }

}
