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

package com.vmware.xenon.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

public class LoaderService extends StatefulService {
    public static final long MAINTENANCE_INTERVAL_MICROS = 30000000L;
    public static final String FILESYSTEM_DEFAULT_GROUP = "default";
    public static final String FILESYSTEM_DEFAULT_PATH = "services";

    public enum LoaderType {
        FILESYSTEM
    }

    public static class LoaderServiceInfo {
        public String name;
        public Long fileUpdateTimeMillis;
        public Map<String, String> serviceClasses = new HashMap<String, String>();
    }

    public static class LoaderServiceState extends ServiceDocument {
        public LoaderType loaderType;
        public String path;
        public Map<String, LoaderServiceInfo> servicePackages;
    }

    public LoaderService() {
        super(LoaderServiceState.class);
        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
    }

    @Override
    public void handleStart(Operation op) {
        if (op.hasBody()) {
            LoaderServiceState s = op.getBody(LoaderServiceState.class);
            if (s.loaderType == null) {
                s.loaderType = LoaderType.FILESYSTEM;
            }

            logFine("Initial path is %s", s.path);
        }

        super.setMaintenanceIntervalMicros(MAINTENANCE_INTERVAL_MICROS);

        op.complete();
    }

    @Override
    public void handlePatch(Operation patch) {
        // Get the current state.
        final LoaderServiceState currentState = getState(patch);
        final LoaderServiceState patchBody = getBody(patch);

        if (patchBody.path != null) {
            currentState.path = patchBody.path;
        }

        if (patchBody.loaderType != null) {
            currentState.loaderType = patchBody.loaderType;
        } else {
            currentState.loaderType = LoaderType.FILESYSTEM;
        }

        if (patchBody.servicePackages != null) {
            currentState.servicePackages = patchBody.servicePackages;
        }

        // Update the state.
        patch.setBody(currentState).complete();
    }

    @Override
    public void handlePut(Operation put) {
        final LoaderServiceState newState = put.getBody(LoaderServiceState.class);
        if (newState.loaderType == null) {
            newState.loaderType = LoaderType.FILESYSTEM;
        }
        setState(put, newState);
        put.setBody(newState).complete();
    }

    @Override
    public void handlePost(Operation op) {
        logFine("Post called");

        if (!op.hasBody()) {
            op.fail(new IllegalArgumentException("body is required"));
            return;
        }

        LoaderServiceState localState = getState(op);
        if (localState == null || localState.loaderType == null
                || localState.path == null) {
            op.fail(new IllegalStateException("Service state is null or invalid"));
            return;
        }

        // Acknowledge the request and complete the operation before actual loading
        op.setStatusCode(Operation.STATUS_CODE_ACCEPTED).complete();

        loadServices(localState);
    }

    @Override
    public void handleMaintenance(Operation op) {
        logFine("Maintenance called");

        sendRequest(Operation.createGet(getUri()).setCompletion(
                (o, e) -> performMaintenance(op, o, e)));
    }

    private void performMaintenance(Operation maint, Operation get,
            Throwable getEx) {
        if (getEx != null) {
            logWarning("Failure getting state: %s", getEx.toString());
            maint.complete();
            return;
        }

        if (!get.hasBody()) {
            maint.complete();
            return;
        }

        if (getHost().isStopping()) {
            maint.complete();
            return;
        }

        LoaderServiceState localState = get.getBody(LoaderServiceState.class);
        if (localState == null || localState.loaderType == null
                || localState.path == null) {
            maint.complete();
            return;
        }

        loadServices(localState);

        maint.complete();
    }

    private void loadServices(LoaderServiceState localState) {
        Map<String, LoaderServiceInfo> discoveredPackages = null;
        switch (localState.loaderType) {
        case FILESYSTEM:
            try {
                discoveredPackages = loadFromFileSystem(localState);
            } catch (Exception e) {
                logWarning("Failed to load packages from path %s, %s.",
                        localState.path, Utils.toString(e));
            }
            break;
        default:
            logWarning("Unknown loader type: %s", localState.loaderType);
        }

        if (discoveredPackages != null) {
            LoaderServiceState newState = new LoaderServiceState();
            newState.servicePackages = discoveredPackages;
            final Operation patch = Operation.createPatch(getUri()).setBody(newState);
            sendRequest(patch);
        }
    }

    private Map<String, LoaderServiceInfo> loadFromFileSystem(LoaderServiceState state)
            throws ClassNotFoundException, InstantiationException,
            IllegalAccessException {
        File libDir = new File(state.path);
        if (!libDir.isAbsolute()) {
            libDir = new File(new File(getHost().getStorageSandbox()), state.path);
        }

        if (!libDir.exists()) {
            if (!libDir.mkdirs()) {
                logWarning("Failed to precreate the Loader path directory %s", libDir);
                return null;
            }
        } else if (!libDir.isDirectory()) {
            logWarning("Loader path %s is not a directory.", libDir.getAbsolutePath());
            return null;
        }

        Map<String, LoaderServiceInfo> services = discoverServices(libDir, state.servicePackages);

        if (services != null) {
            startDiscoveredServices(services, state);
        }

        return services;
    }

    private void startDiscoveredServices(Map<String, LoaderServiceInfo> services,
            LoaderServiceState state)
                    throws ClassNotFoundException, InstantiationException,
                    IllegalAccessException {
        logFine("Updating the class loader with new libraries");

        URL[] urls = new URL[services.size()];
        int i = 0;
        for (String servicePackage : services.keySet()) {
            try {
                urls[i++] = new URI(servicePackage).toURL();
            } catch (MalformedURLException | URISyntaxException e) {
                logWarning("Failed to convert path to URL", Utils.toString(e));
            }
        }

        URLClassLoader cl = null;
        try {
            cl = new URLClassLoader(urls);

            for (LoaderServiceInfo packageInfo : services.values()) {
                logFine("Processing package %s", packageInfo.name);
                for (String serviceClass : packageInfo.serviceClasses.keySet()) {
                    Class<?> clazz = cl.loadClass(serviceClass);

                    if (isValidDynamicService(clazz)) {
                        Service service = startDynamicService(clazz);
                        packageInfo.serviceClasses.put(serviceClass, service.getSelfLink());
                    }
                }
            }
        } finally {
            if (cl != null) {
                try {
                    cl.close();
                } catch (IOException e) {
                }
            }
        }
    }

    private Service startDynamicService(Class<?> clazz)
            throws IllegalArgumentException, IllegalAccessException, InstantiationException {
        Service service = (Service) clazz.newInstance();
        URI link = null;
        // If it's a service
        if (getSelfOrFactoryLink(clazz).getName().equals(UriUtils.FIELD_NAME_SELF_LINK)) {
            link = UriUtils.buildUri(getHost(), service.getClass());
            getHost().startService(
                    Operation.createPost(link),
                    service);
        } else {
            // If it's a factory
            link = UriUtils.buildFactoryUri(getHost(), service.getClass());
            getHost().startFactory(service);
        }

        logInfo("Started service " + link);
        return service;
    }

    private boolean isValidDynamicService(Class<?> clazz) throws IllegalArgumentException,
            IllegalAccessException {
        return (Service.class.isAssignableFrom(clazz) && getSelfOrFactoryLink(clazz) != null);
    }

    private Field getSelfOrFactoryLink(Class<?> clazz)
            throws IllegalArgumentException, IllegalAccessException {
        try {
            Field link = clazz.getField(UriUtils.FIELD_NAME_SELF_LINK);
            logFine("Class %s self link %s", clazz, link.get(null));
            return link;
        } catch (NoSuchFieldException e) {
            try {
                Field link = clazz.getField(UriUtils.FIELD_NAME_FACTORY_LINK);
                logFine("Class %s factory link %s", clazz, link.get(null));
                return link;
            } catch (NoSuchFieldException e2) {
                logFine("Self link fields wasn't found in %s", clazz);
            }
        }
        return null;
    }

    private Map<String, LoaderServiceInfo> discoverServices(File libDir,
            Map<String, LoaderServiceInfo> existingPackages) {
        logFine("Checking for updates in " + libDir.toURI());
        Map<String, LoaderServiceInfo> discoveredPackages = new HashMap<>();

        boolean updated = false;
        File[] files = libDir.listFiles();
        if (files == null) {
            return null;
        }
        for (File file : files) {
            if (!file.getName().endsWith(".jar")) {
                continue;
            }
            logFine("Found jar file %s", file.toURI());

            LoaderServiceInfo packageInfo = existingPackages.get(file.toURI().toString());
            if (packageInfo != null) {
                long lastModified = file.lastModified();
                if (lastModified == packageInfo.fileUpdateTimeMillis) {
                    // Jar file has been previously loaded.
                    // Add to the list of discovered packages and skip.
                    discoveredPackages.put(file.toURI().toString(), packageInfo);
                    continue;
                }
            }

            try (JarInputStream jar = new JarInputStream(new FileInputStream(file))) {
                while (true) {
                    JarEntry e = jar.getNextJarEntry();
                    if (e == null) {
                        break;
                    }

                    String name = e.getName();

                    // Assuming specific naming convention for
                    // Service classes
                    if (isValidServiceClassName(name)) {
                        logFine("Found service class %s", name);
                        String className = name.replaceAll(".class", "").replaceAll("/", ".");

                        if (packageInfo == null) {
                            packageInfo = new LoaderServiceInfo();
                        }

                        packageInfo.name = file.getName();
                        packageInfo.fileUpdateTimeMillis = file.lastModified();
                        updated |= (null == packageInfo.serviceClasses.put(className, null));

                        discoveredPackages.put(file.toURI().toString(), packageInfo);
                    }
                }
            } catch (IOException e) {
                logWarning("Problem loading package %s, Exception %s",
                        file.getName(), Utils.toString(e));
            }
        }

        if (updated) {
            return discoveredPackages;
        }

        return null;
    }

    /**
     * Initial filter for class files to avoid loading all classes into the class loader. The method
     * checks for class name ending with "Factory" or "Service", such as "ExampleFactory.class"
     *
     * @param name
     *            class file name to validate
     * @return
     */
    private boolean isValidServiceClassName(String name) {
        return name.endsWith("Factory.class") ||
                name.endsWith("Service.class");
    }
}
