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

package com.vmware.xenon.test;

public class ExceptionTestUtils {

    private ExceptionTestUtils() {
    }

    /**
     * Throw any {@link Throwable} as unchecked exception.
     * This does not wrap the {@link Throwable}, rather uses java trick to throw it as is.
     * TODO: more documentation
     */
    public static void throwAsUnchecked(Throwable throwable) {
        throwsUnchecked(throwable);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Exception> void throwsUnchecked(Throwable throwable) throws T {
        throw (T) throwable;
    }

    @FunctionalInterface
    public interface CodeWithThrowable {
        void execute() throws Throwable;
    }

    public static void executeSafely(CodeWithThrowable block) {
        try {
            block.execute();
        } catch (Throwable throwable) {
            throwsUnchecked(throwable);
        }
    }

}
