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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class LocalizationManager {

    public static final Locale DEFAULT_LOCALE = Locale.US;
    public static final String ACCEPT_LANGUAGE_HEADER = "accept-language";

    private LocalizationManager() {
    }

    public static String resolveMessage(LocalizableValidationException e, Operation op) {

        if (op == null) {
            return e.getSystemMessage();
        }

        Locale requested = resolveLocale(op);
        return resolveMessage(e, requested);
    }

    private static String resolveMessage(LocalizableValidationException e, Locale locale) {

        if (locale == null) {
            return e.getSystemMessage();
        }

        Properties messages = new Properties();
        try {
            String resourceName = SupportedLocales.buildLocaleMessagesFileName(locale);
            InputStream in = LocalizationManager.class.getClassLoader()
                    .getResourceAsStream(resourceName);
            messages.load(in);
        } catch (IOException e1) {
            return e.getSystemMessage();
        }

        String message = messages.getProperty(e.getErrorMessageCode(), e.getSystemMessage());
        for (Map.Entry<String, String> arg : e.getArguments().entrySet()) {
            String replaceStr = String.format("\\{%s\\}", arg.getKey());
            message = message.replaceAll(replaceStr, arg.getValue());
        }

        return message;
    }

    public static Locale resolveLocale(Operation op) {
        String requestedLangs = op.getRequestHeader(ACCEPT_LANGUAGE_HEADER);

        if (requestedLangs == null) {
            return null;
        }

        List<Locale> locales = Locale.LanguageRange.parse(requestedLangs).stream()
                .map(rang‌​e -> new Locale(rang‌​e.getRange()))
                .collect(Collectors.toList());

        for (Locale locale : locales) {
            if (SupportedLocales.isSupported(locale)) {
                return locale;
            }
        }

        return DEFAULT_LOCALE;

    }



}
