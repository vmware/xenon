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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.vmware.xenon.common.OperationJoin.JoinedCompletionHandler;

/**
 * Operations to be executed in sequence.
 *
 * Example usage:
 *
 * <pre>
 * {@code
 *   OperationSequence.create(op1, op2, op3)
 *                    .next(op4, op5, op6)
 *                    .setCompletion((ops, exs) -> { // shared completion handler
 *                          if (exs != null) {
 *                               return;
 *                          }
 *
 *                          Operation opr1 = ops.get(op1.getId());
 *                          Operation opr4 = ops.get(op4.getId());
 *                          // ....
 *                     })
 *                     .sendWith(host);
 * }
 * </pre>
 *
 * Advanced example usage:
 *
 * <pre>
 * {@code
 * OperationSequence.create(op1, op2, op3) // initial joined operations to be executed in parallel
 *            .setCompletion((ops, exs) -> { //shared completion handler for the first level (optional)
 *               if (exs != null) {
 *                   // Map<Long,Throwable> exceptions = exc;
 *                   for(Throwable e:exc.values()){
 *                       //log exceptions or something else.
 *                    }
 *
 *                    // NOTE: if there is at least one exception on the current level
 *                    // the next level will not be executed.
 *                    // In case, the next level should proceed the exception map
 *                    // should be cleared: exc.clear()
 *                    // This might lead to inconsistent data in the next level completions.
 *
 *                    return;
 *                }
 *
 *                // Map<Long,Operation> operations = ops;
 *                Operation opr1 = ops.get(op1.getId());
 *                SomeState body = opr1.getBody(SomeState.class);
 *
 *                // Can set properties on the operations in the next levels
 *                NextState nextStateBody = new NextState();
 *                nextState.property = body.otherProperty;
 *
 *                op4.setUri(body.selfLink);
 *                op4.setBody(nextStateBody);
 *            })
 *            // next level of parallel operation to be executed after the first level operations
 *            // are completed first.
 *            .next(op4, op5, op6)
 *            .setCompletion((ops, exs) -> { // shared completion handler for the second level (optional)
 *                   if (exs != null) {
 *                      return;
 *                   }
 *
 *                   Operation opr4 = ops.get(op4.getId());
 *
 *                   // have access to the first level completed operations
 *                   Operation opr1 = ops.get(op1.getId());
 *             })
 *             .next(op7, op8, op9)
 *             .setCompletion((ops, exs) -> {
 *                 // shared completion handler for the third level (optional)
 *                 // all previously completed operations are accessible.
 *                 Operation opr1 = ops.get(op1.getId());
 *                 Operation opr4 = ops.get(op4.getId());
 *                 Operation opr1 = ops.get(op1.getId());
 *                 // In many cases, the last shared completion could be the only one needed.
 *             })
 *             .sendWith(host);
 * }
 * </pre>
 */
public class OperationSequence {
    private final OperationJoin join;
    private OperationSequence child;
    private OperationSequence parent;
    private ServiceRequestSender sender;
    private boolean cumulative = true;
    private boolean abortOnFirstFailure = false;

    private OperationSequence(OperationJoin join) {
        this.join = join;
    }

    /**
     * Create {@link OperationSequence} with an instance of {@link OperationJoin} to be linked in a
     * sequence with other {@link OperationJoin}s.
     */
    public static OperationSequence create(OperationJoin... joins) {
        if (joins.length == 0) {
            throw new IllegalArgumentException("At least one 'operationJoin' is required.");
        }

        return chainJoins(null, joins);
    }

    private static OperationSequence chainJoins(OperationSequence root, OperationJoin... joins) {
        for (OperationJoin join : joins) {
            OperationSequence current = new OperationSequence(join);
            if (root != null) {
                root.child = current;
                current.parent = root;
            }
            root = current;
        }
        return root;
    }

    /**
     * Create {@link OperationSequence} with a list of {@link Operation}s to be joined together in
     * parallel execution.
     */
    public static OperationSequence create(Operation... ops) {
        return create(OperationJoin.create(ops));
    }

    /**
     * Create {@link OperationSequence} with an instance of {@link OperationJoin} to be linked in a
     * sequence with the current {@link OperationSequence}s.
     */
    public OperationSequence next(OperationJoin... joins) {
        return chainJoins(this, joins);
    }

    public OperationSequence next(Operation... ops) {
        return next(OperationJoin.create(ops));
    }

    public OperationSequence setCompletion(JoinedCompletionHandler joinedCompletion) {
        return setCompletion(true, joinedCompletion);
    }

    public OperationSequence setCompletion(boolean cumulative, JoinedCompletionHandler joinedCompletion) {
        this.cumulative = cumulative;
        this.join.setCompletion(joinedCompletion);
        return this;
    }

    /**
     * Abort entire sequence on first operation failure.
     *
     * The joinedCompletion handler set by {@link #setCompletion(JoinedCompletionHandler)}
     * will NOT be called when the sequence encounters an operation failure.
     *
     * <pre>
     * {@code
     *    Operation op1 = Operation.createGet(...)
     *        .setCompletion((o, e) -> {
     *            // This will be called always
     *        });
     *
     *    OperationSequence.create(op1)
     *        .setCompletion((ops, exs) -> {
     *            // This will NOT be called if op1 failed
     *         })
     *        .abortOnFirstFailure();
     * }
     * </pre>
     */
    public OperationSequence abortOnFirstFailure() {
        this.abortOnFirstFailure = true;
        return this;
    }

    /**
     * Send using the {@link ServiceRequestSender}.
     * @see OperationJoin#sendWith(ServiceRequestSender)
     */
    public void sendWith(ServiceRequestSender sender) {
        if (this.parent != null) {
            this.parent.sendWith(sender);
        } else {
            send(sender);
        }
    }

    private void send(ServiceRequestSender sender) {
        validateSendRequest(sender);
        this.sender = sender;
        setProxyCompletion();
        this.join.sendWith(sender);
    }

    private void setProxyCompletion() {
        this.join
                .setCompletion(new CompletionHandlerSequenceProxy(this, this.join.joinedCompletion));
    }

    private static class CompletionHandlerSequenceProxy implements JoinedCompletionHandler {
        private final JoinedCompletionHandler joinedCompletionHandler;
        private final OperationSequence sequence;
        private final AtomicBoolean completed;

        private CompletionHandlerSequenceProxy(OperationSequence sequence,
                JoinedCompletionHandler joinedCompletionHandler) {
            this.sequence = sequence;
            this.joinedCompletionHandler = joinedCompletionHandler;
            this.completed = new AtomicBoolean();
        }

        @Override
        public void handle(final Map<Long, Operation> ops, final Map<Long, Throwable> failures) {
            if (!this.completed.compareAndSet(false, true)) {
                return;
            }

            boolean hasFailure = failures != null && !failures.isEmpty();
            boolean abortImmediately = this.sequence.hasAbortOnFirstFailureInChildSequences();
            if (hasFailure && abortImmediately) {
                return;
            }

            final AtomicBoolean errors = new AtomicBoolean();
            if (this.joinedCompletionHandler != null) {
                if (this.sequence.cumulative) {
                    final Map<Long, Operation> allOps = this.sequence.getAllCompletedOperations();
                    final Map<Long, Throwable> allFailures = this.sequence.getAllFailures();
                    this.joinedCompletionHandler.handle(allOps, allFailures);
                    errors.set(allFailures != null && !allFailures.isEmpty());
                } else {
                    this.joinedCompletionHandler.handle(ops, failures);
                    errors.set(failures != null && !failures.isEmpty());
                }
            }
            if (this.sequence.child != null && !errors.get()) {
                try {
                    this.sequence.child.send(this.sequence.sender);
                } catch (Throwable t) {
                    // complete with failure
                    this.sequence.child.join.fail(t);
                }
            }
        }
    }

    private Map<Long, Operation> getAllCompletedOperations() {
        final Map<Long, Operation> operations = new ConcurrentHashMap<>();
        OperationSequence current = this;
        while (current != null) {
            for (Operation op : current.join.getOperations()) {
                operations.put(op.getId(), op);
            }
            current = current.parent;
        }
        return operations;
    }

    private Map<Long, Throwable> getAllFailures() {
        Map<Long, Throwable> failures = null;
        OperationSequence current = this;
        while (current != null) {
            Map<Long, Throwable> currentFailures = current.join.getFailures();
            if (currentFailures != null) {
                if (failures == null) {
                    failures = currentFailures;
                } else {
                    failures.putAll(currentFailures);
                }
            }
            current = current.parent;
        }
        return failures;
    }

    private void validateSendRequest(Object sender) {
        if (sender == null) {
            throw new IllegalArgumentException("'sender' must not be null.");
        }
        if (this.join == null) {
            throw new IllegalStateException("No joined operation to be sent.");
        }
    }

    private boolean hasAbortOnFirstFailureInChildSequences() {
        OperationSequence childSequence = this.child;
        while (childSequence != null) {
            if (childSequence.abortOnFirstFailure) {
                return true;
            }
            childSequence = childSequence.child;
        }
        return false;
    }

}
