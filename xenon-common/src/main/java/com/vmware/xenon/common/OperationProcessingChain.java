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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.logging.Level;

/**
 * A chain of filters, each of them is a {@link Predicate<Operation>}. When {@link #processRequest} is called
 * the filters are evaluated sequentially, where each filter's {@link Predicate<Operation>#test} can return
 * <code>true</code> to have the next filter in the chain continue process the request or
 * <code>false</code> to stop processing.
 */
public class OperationProcessingChain {

    public enum FilterReturnCode {
        /**
         * The filter has done processing the operation. The operation should
         * continue to the next filter in chain.
         */
        CONTINUE_PROCESSING,

        /**
         * Operation processing should be resumed on the current filter.
         */
        RESUME_PROCESSING,

        /**
         * The filter has failed the operation. The operation should not
         * be passed to the next filter in chain.
         */
        FAILED_STOP_PROCESSING,

        /*
         * The filter has marked the operation as successfully completed. The
         * operation should not be passed to the next filter in chain.
         */
        SUCCESS_STOP_PROCESSING,

        /**
         * The filter intends to continue processing the operation
         * asynchronously. The operation should not be passed to the next
         * filter in chain.
         */
        SUSPEND_PROCESSING
    }

    public static class OperationProcessingContext {
        /**
         * The ServiceHost this context is associated with
         */
        private ServiceHost host;

        /**
         * The service that the operation is targeting, if found
         */
        private Service service;

        /**
         * The chain that this context is associated with
         */
        private OperationProcessingChain opProcessingChain;

        /**
         * The position in the chain of the current filter being executed
         */
        private int currentFilterPosition;

        /**
         * A callback to invoke after the filters in the chain have processed the operation
         * and returned CONTINUE_PROCESSING
         */
        private Consumer<Operation> operationConsumer;

        /**
         * A callback to invoke when a filter suspends the operation.
         * The callback is expected to perform non-blocking operations.
         */
        private Consumer<Operation> suspendConsumer;

        private OperationProcessingContext(ServiceHost host, OperationProcessingChain opProcessingChain) {
            this.host = host;
            this.opProcessingChain = opProcessingChain;
            this.currentFilterPosition = -1;
        }

        public ServiceHost getHost() {
            return this.host;
        }

        public Service getService() {
            return this.service;
        }

        public OperationProcessingChain getOpProcessingChain() {
            return this.opProcessingChain;
        }

        public int getCurrentFilterPosition() {
            return this.currentFilterPosition;
        }

        public void setService(Service service) {
            this.service = service;
        }

        public void setSuspendConsumer(Consumer<Operation> suspendConsumer) {
            this.suspendConsumer = suspendConsumer;
        }

        /**
         * Delegate to {@link OperationProcessingChain} with current context.
         *
         * @param op               the operation that has been suspended and is now being resumed
         * @param filterReturnCode the filter's indication of whether to continue processing
         *                         the request or to stop it (with success or with failure)
         * @param e                a Throwable in case the filter return code is FAILED_STOP_PROCESSING
         */
        public void resumeProcessingRequest(Operation op, FilterReturnCode filterReturnCode, Throwable e) {
            this.opProcessingChain.resumeProcessingRequest(op, this, filterReturnCode, e);
        }
    }

    public interface Filter {
        FilterReturnCode processRequest(Operation op, OperationProcessingContext context);

        default void init() {}

        default void close() {}
    }

    public OperationProcessingContext createContext(ServiceHost host) {
        return new OperationProcessingContext(host, this);
    }

    public OperationProcessingChain setLogLevel(Level logLevel) {
        this.logLevel = logLevel;
        return this;
    }

    public OperationProcessingChain toggleLogging(boolean loggingEnabled) {
        this.loggingEnabled = loggingEnabled;
        return this;
    }

    public OperationProcessingChain setLogFilter(Predicate<Operation> logFilter) {
        this.logFilter = logFilter;
        return this;
    }

    private Level logLevel;
    private boolean loggingEnabled;
    private Predicate<Operation> logFilter;

    private List<Filter> filters;

    private OperationProcessingChain() {
        this.filters = new ArrayList<>();
    }

    public static OperationProcessingChain create(Filter... filters) {
        OperationProcessingChain opProcessingChain = new OperationProcessingChain();
        for (Filter filter : filters) {
            filter.init();
            opProcessingChain.filters.add(filter);
        }

        return opProcessingChain;
    }

    public void close() {
        for (Filter filter : this.filters) {
            filter.close();
        }
        this.filters.clear();
    }


    /**
     * Processes an operation by passing it through the chain of filters.
     * After the filters in the chain have processed the operation, the provided
     * consumerOperation's accept() method is invoked if and when the caller
     * should continue processing the operation.
     */
    public void processRequest(Operation op, OperationProcessingContext context,
            Consumer<Operation> operationConsumer) {
        // sticking the operation consumer in the context, so that we can find it
        // in case a filter decides to suspend processing of the operation and later
        // resume it. This detail is abstracted from the caller of this public method.
        context.operationConsumer = operationConsumer;

        FilterReturnCode rc = processRequest(op, context, 0);
        if (rc == FilterReturnCode.CONTINUE_PROCESSING) {
            operationConsumer.accept(op);
        }
    }

    /**
     * A reentrant method to allow a filter to resume processing the request by chain filters.
     * The filters in the chain after the invoking one are invoked sequentially, as usual,
     * and if the chain end is reached, i.e. the request has not been dropped by any
     * filter, the request is passed to the service for continued processing.
     *
     * @param op the operation the has been suspended and is now being resumed
     * @param context the operation's processing context
     * @param filterReturnCode the filter's indication of whether to continue processing
     *  the request or to stop it (with success or with failure)
     * @param e a Throwable in case the filter return code is FAILED_STOP_PROCESSING
     */
    public void resumeProcessingRequest(Operation op, OperationProcessingContext context,
            FilterReturnCode filterReturnCode, Throwable e) {
        if (shouldLog(op)) {
            String msg = "";
            switch (filterReturnCode) {
            case SUCCESS_STOP_PROCESSING:
                msg = "Operation completed";
                break;

            case FAILED_STOP_PROCESSING:
                msg = "Operation failed";
                if (e != null) {
                    msg += ": " + e.getMessage();
                }
                break;

            case CONTINUE_PROCESSING:
                msg = "Operation processing continued";
                break;

            case RESUME_PROCESSING:
                msg = "Operation processing resumed";
                break;

            default:
                throw new IllegalArgumentException("unexpected FilterReturnCode: " + filterReturnCode);
            }

            log(op, context, msg, this.logLevel);
        }

        if (filterReturnCode != FilterReturnCode.CONTINUE_PROCESSING &&
                filterReturnCode != FilterReturnCode.RESUME_PROCESSING) {
            // filter has instructed us not not continue processing
            return;
        }

        // resume or continue processing
        int nextFilterStartIndex = filterReturnCode == FilterReturnCode.RESUME_PROCESSING ?
                context.currentFilterPosition : context.currentFilterPosition + 1;
        FilterReturnCode rc = FilterReturnCode.CONTINUE_PROCESSING;

        if (nextFilterStartIndex < this.filters.size()) {
            rc = processRequest(op, context, nextFilterStartIndex);
        }

        if (rc == FilterReturnCode.CONTINUE_PROCESSING) {
            context.operationConsumer.accept(op);
        }
    }

    public Filter findFilter(Predicate<Filter> tester) {
        return this.filters.stream().filter(tester).findFirst().orElse(null);
    }

    private FilterReturnCode processRequest(Operation op, OperationProcessingContext context, int startIndex) {
        boolean shouldLog = shouldLog(op);

        for (int i = startIndex; i < this.filters.size(); i++) {
            Filter filter = this.filters.get(i);
            context.currentFilterPosition = i;
            FilterReturnCode rc = filter.processRequest(op, context);

            String msg = shouldLog ? String.format("returned %s", rc) : null;

            switch (rc) {
            case CONTINUE_PROCESSING:
                if (shouldLog) {
                    log(op, context, msg, this.logLevel);
                }
                continue;

            case SUCCESS_STOP_PROCESSING:
                if (shouldLog) {
                    msg += ". Operation completed - stopping processing";
                    log(op, context, msg, this.logLevel);
                }
                return rc;

            case FAILED_STOP_PROCESSING:
                if (shouldLog) {
                    msg += ". Operation failed - stopping processing";
                    log(op, context, msg, this.logLevel);
                }
                return rc;

            case SUSPEND_PROCESSING:
                if (shouldLog) {
                    msg += ". Operation will be resumed asynchronously - suspend processing";
                    log(op, context, msg, this.logLevel);
                }

                if (context.suspendConsumer == null) {
                    throw new IllegalStateException(
                            String.format("Operation %d %s has been suspended with a null suspendConsumer",
                            op.getId(), op.getAction()));
                }

                Consumer<Operation> suspendConsumer = context.suspendConsumer;
                context.suspendConsumer = null;
                suspendConsumer.accept(op);

                return rc;

            default:
                msg += ". Unexpected returned code - failing operation and stopping processing";
                log(op, context, msg, Level.SEVERE);
            }
        }

        return FilterReturnCode.CONTINUE_PROCESSING;
    }

    private boolean shouldLog(Operation op) {
        boolean shouldLog = this.loggingEnabled;
        if (this.logFilter != null) {
            shouldLog &= this.logFilter.test(op);
        }

        return shouldLog;
    }

    private void log(Operation op, OperationProcessingContext context, String msg, Level logLevel) {
        String hostId = context.host != null ? context.host.getId() : "";
        String path = op.getUri() != null ? op.getUri().getPath() : "";
        Filter filter = this.filters.get(context.currentFilterPosition);
        String filterName = filter != null ? filter.getClass().getSimpleName() : "";
        String logMsg = String.format("(host: %s, op %d %s %s) filter %s: %s",
                hostId, op.getId(), op.getAction(),  path, filterName, msg);
        Level level = logLevel != null ? logLevel : Level.INFO;
        Utils.log(getClass(), op.getUri().getPath(), level, logMsg);
    }
}
