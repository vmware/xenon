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

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.Locale;

import org.junit.Before;
import org.junit.Test;

public class TestLocalizationUtil {

    private static final String SYSTEM_EX_MESSAGE = "System exception message";

    private static final String ERROR_MESSAGE_DE = "Random test error message in German!: argValue";

    private static final String ERROR_MESSAGE_EN = "Random test error message: argValue";

    private static final String ERROR_MESSAGE_CODE = "random.message.code";

    private static final String EX_ARG_VALUE = "argValue";

    private LocalizableValidationException ex;

    @Before
    public void setUp() {
        this.ex = new LocalizableValidationException(SYSTEM_EX_MESSAGE, ERROR_MESSAGE_CODE, EX_ARG_VALUE);
    }

    @Test
    public void testLocalizatioMessageResolution() {
        Operation op = Operation.createGet(URI.create("127.0.0.1"))
                .addRequestHeader(Operation.ACCEPT_LANGUAGE_HEADER, "de,en-US;q=0.8,en;q=0.6");

        Locale locale = LocalizationUtil.resolveLocale(op);
        assertEquals(Locale.GERMAN, locale);

        String message = LocalizationUtil.resolveMessage(this.ex, op);
        assertEquals(ERROR_MESSAGE_DE, message);
    }

    @Test
    public void testResolveSecondaryLocaleMessage() {
        Operation op = Operation.createGet(URI.create("127.0.0.1"))
                .addRequestHeader(Operation.ACCEPT_LANGUAGE_HEADER, "cn,en-GB;q=0.8,en;q=0.6");

        Locale locale = LocalizationUtil.resolveLocale(op);
        assertEquals(Locale.ENGLISH, locale);

        String message = LocalizationUtil.resolveMessage(this.ex, op);
        assertEquals(ERROR_MESSAGE_EN, message);
    }

    @Test
    public void testResolveUnsupportedLocaleMessage() {
        Operation op = Operation.createGet(URI.create("127.0.0.1"))
                .addRequestHeader(Operation.ACCEPT_LANGUAGE_HEADER, "cn");

        Locale locale = LocalizationUtil.resolveLocale(op);
        assertEquals(LocalizationUtil.DEFAULT_LOCALE, locale);

        String message = LocalizationUtil.resolveMessage(this.ex, op);
        assertEquals(ERROR_MESSAGE_EN, message);
    }

    @Test
    public void testWithNullLocale() {
        // Locale should resolve to default
        String message = LocalizationUtil.resolveMessage(this.ex, null);
        assertEquals(ERROR_MESSAGE_EN, message);
    }

    @Test
    public void testMissingErrorCodeResolution() {
        this.ex = new LocalizableValidationException(SYSTEM_EX_MESSAGE, "missing.error.code", EX_ARG_VALUE);
        String message = LocalizationUtil.resolveMessage(this.ex, null);
        assertEquals(SYSTEM_EX_MESSAGE, message);
    }
}
