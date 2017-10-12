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

import io.opentracing.ActiveSpan;

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
        private ServiceHost host;
        private Service service;
        private OperationProcessingChain opProcessingChain;
        private int currentFilterPosition;
        private Consumer<Operation> operationConsumer;
        /** OpenTracing span to supply to each filter and to the completion at the end of the chain */
        private ActiveSpan.Continuation tracingContinuation;

        /**
         * Recover a usable copy of the stored span. The result must be closed when finished with.
         * @return
         */
        public ActiveSpan getSpan() {
            if (this.tracingContinuation == null) {
                return null;
            }
            ActiveSpan span = this.tracingContinuation.activate();
            this.tracingContinuation = null;
            setSpan(span);
            return span;
        }

        /**
         * Hand a span into the chain context. Once set, this span will be set as the active span around
         * each call into a {@link Filter} as well as the call into the completion of the chain.
         * @param span
         */
        public void setSpan(ActiveSpan span) {
            if (this.tracingContinuation != null) {
                this.tracingContinuation.activate().close();
            }
            if (span != null) {
                this.tracingContinuation = span.capture();
            } else {
                this.tracingContinuation = null;
            }
        }

        private OperationProcessingContext(ServiceHost host, OperationProcessingChain opProcessingChain) {
            this.host = host;
            this.opProcessingChain = opProcessingChain;
            this.currentFilterPosition = -1;
            this.tracingContinuation = null;
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
    }

    public interface Filter {
        FilterReturnCode processRequest(Operation op, OperationProcessingContext context);

        default void init() {}

        default void close() {}
    }

    public OperationProcessingContext createContext(ServiceHost host) {
        return new OperationProcessingContext(host, this);
    }

    /**
     * Finish working with context.
     * Mainly exists to cleanup the span in the context.
     * @param context
     */
    private void finishContext(OperationProcessingContext context, FilterReturnCode rc) {
        if (rc != FilterReturnCode.SUSPEND_PROCESSING) {
            context.setSpan(null);
        }
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
        complete(op, context, rc);
    }

    /**
     * A reentrant method to allow a filter to resume processing the request by chain filters.
     * The filters in the chain after the invoking one are invoked sequentially, as usual,
     * and if the chain end is reached, i.e. the request has not been dropped by any
     * filter, the request is passed to the service for continued processing.
     */
    public void resumeProcessingRequest(Operation op, OperationProcessingContext context) {
        if (shouldLog(op)) {
            log(op, context, "operation processing resumed", this.logLevel);
        }

        FilterReturnCode rc = FilterReturnCode.CONTINUE_PROCESSING;

        if (context.currentFilterPosition < this.filters.size() - 1) {
            rc = processRequest(op, context, context.currentFilterPosition + 1);
        }

        complete(op, context, rc);
    }

    @SuppressWarnings("try")
    private void complete(Operation op, OperationProcessingContext context, FilterReturnCode rc) {
        try (ActiveSpan span = context.getSpan()) {
            if (rc == FilterReturnCode.CONTINUE_PROCESSING) {
                context.operationConsumer.accept(op);
            }
        }
        finishContext(context, rc);
    }

    /**
     * Enables a filter that has previously suspended the operation to notify
     * the processing chain it has resumed processing and completed the operation.
     *
     * NOTE: this should be called in addition to {@link OperationProcessingChain.resumeProcessingRequest}
     */
    public void resumedRequestCompleted(Operation op, OperationProcessingContext context) {
        if (shouldLog(op)) {
            log(op, context, "Operation completed", this.logLevel);
        }
        finishContext(context, FilterReturnCode.SUCCESS_STOP_PROCESSING);
    }

    /**
     * Enables a filter that has previously suspended the operation to notify
     * the processing chain it has resumed processing and failed the operation.
     *
     * NOTE: this should be called in addition to {@link OperationProcessingChain.resumeProcessingRequest}
     */
    public void resumedRequestFailed(Operation op, OperationProcessingContext context, Throwable e) {
        if (shouldLog(op)) {
            log(op, context, "Operation failed: " + e.getMessage(), this.logLevel);
        }
    }

    public Filter findFilter(Predicate<Filter> tester) {
        return this.filters.stream().filter(tester).findFirst().orElse(null);
    }

    @SuppressWarnings("try")
    private FilterReturnCode processRequest(Operation op, OperationProcessingContext context, int startIndex) {
        // Establish a span around the calls into filters
        boolean shouldLog = shouldLog(op);

        for (int i = startIndex; i < this.filters.size(); i++) {
            try (ActiveSpan span = context.getSpan()) {
                context.currentFilterPosition = i;
                FilterReturnCode rc = processRequestInternal(op, context, shouldLog);
                if (rc != FilterReturnCode.CONTINUE_PROCESSING) {
                    return rc;
                }
            }
        }

        return FilterReturnCode.CONTINUE_PROCESSING;
    }

    private FilterReturnCode processRequestInternal(Operation op, OperationProcessingContext context, boolean shouldLog) {
        Filter filter = this.filters.get(context.currentFilterPosition);
        FilterReturnCode rc = filter.processRequest(op, context);

        String msg = shouldLog ? String.format("returned %s", rc) : null;

        switch (rc) {
        case CONTINUE_PROCESSING:
            if (shouldLog) {
                log(op, context, msg, this.logLevel);
            }
            return FilterReturnCode.CONTINUE_PROCESSING;

        case SUCCESS_STOP_PROCESSING:
            if (shouldLog) {
                msg += ". Operation completed - stopping processing";
                log(op, context, msg, this.logLevel);
            }
            return FilterReturnCode.SUCCESS_STOP_PROCESSING;

        case FAILED_STOP_PROCESSING:
            if (shouldLog) {
                msg += ". Operation failed - stopping processing";
                log(op, context, msg, this.logLevel);
            }
            return FilterReturnCode.FAILED_STOP_PROCESSING;

        case SUSPEND_PROCESSING:
            if (shouldLog) {
                msg += ". Operation will be resumed asynchronously - suspend processing";
                log(op, context, msg, this.logLevel);
            }
            return FilterReturnCode.SUSPEND_PROCESSING;

        default:
            msg += ". Unexpected returned code - failing operation and stopping processing";
            log(op, context, msg, Level.SEVERE);
            return FilterReturnCode.FAILED_STOP_PROCESSING;
        }
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
