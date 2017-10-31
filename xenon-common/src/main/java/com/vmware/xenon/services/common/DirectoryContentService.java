/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
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

import java.io.File;
import java.nio.file.Path;

import com.vmware.xenon.common.FileUtils;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;

/**
 * Serves the contents of a directory, on GET
 */
public class DirectoryContentService extends StatelessService {

    private final Path root;

    public DirectoryContentService(Path root) {
        this.root = root;
        toggleOption(ServiceOption.URI_NAMESPACE_OWNER, true);
    }

    @Override
    public void authorizeRequest(Operation op) {
        op.complete();
    }

    @Override
    public void handleGet(Operation get) {
        String path = get.getUri().getPath();
        if (path.contains("..")) {
            // prevent serving any file by means of path manipulation
            get.fail(Operation.STATUS_CODE_NOT_FOUND);
            return;
        }

        path = path.substring(getSelfLink().length() + 1);
        File file = this.root.resolve(path).toFile();
        if (file.isDirectory() || !file.canRead()) {
            get.fail(Operation.STATUS_CODE_NOT_FOUND);
            return;
        }

        FileUtils.readFileAndComplete(get, file);
    }
}
