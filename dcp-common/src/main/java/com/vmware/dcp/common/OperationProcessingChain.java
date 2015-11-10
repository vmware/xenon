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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * A chain of filters, each of them is a {@link Predicate<Operation>}. When {@link processRequest} is called
 * the filters are evaluated sequentially, where each filter's {@link Predicate<Operation>#test} can return
 * <code>true</code> to have the next filter in the chain continue process the request or
 * <code>false</code> to stop processing.
 */
public class OperationProcessingChain {
    private List<Predicate<Operation>> filters;

    public OperationProcessingChain() {
        this.filters = new ArrayList<>();
    }

    public OperationProcessingChain add(Predicate<Operation> filter) {
        this.filters.add(filter);
        return this;
    }

    public List<Predicate<Operation>> getFilters() {
        return this.filters;
    }

    public boolean processRequest(Operation op) {
        for (Predicate<Operation> filter : this.filters) {
            if (!filter.test(op)) {
                return false;
            }
        }

        return true;
    }
}
