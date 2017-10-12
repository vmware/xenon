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

package com.vmware.xenon.common.filters;

import java.util.function.BiFunction;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationProcessingChain;


public class LambdaFilter implements OperationProcessingChain.Filter {

    public BiFunction<Operation, OperationProcessingChain.OperationProcessingContext, OperationProcessingChain.FilterReturnCode> lambda;

    public LambdaFilter(BiFunction<Operation, OperationProcessingChain.OperationProcessingContext, OperationProcessingChain.FilterReturnCode> lambda) {
        this.lambda = lambda;
    }

    @Override
    public OperationProcessingChain.FilterReturnCode processRequest(Operation op, OperationProcessingChain.OperationProcessingContext context) {
        return this.lambda.apply(op, context);
    }
}
