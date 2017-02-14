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

import java.text.MessageFormat;
import java.util.List;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Localization utility for the purpose of translating localization error codes to localized messages.
 */
public class LocalizationUtil {

    public static final Locale DEFAULT_LOCALE = Locale.ENGLISH;

    private static final String MESSAGES_BASE_FILENAME = "i18n/messages";

    private LocalizationUtil() {
    }

    /**
     * Resolve a localized error message from the provided exception and error message.
     * @param e LocalizableValidationException to resolve
     * @param op operation where the exception originated.
     * @return the resolved error message
     */
    public static String resolveMessage(LocalizableValidationException e, Operation op) {

        Locale requested;
        if (op == null) {
            Logger.getAnonymousLogger().fine("Request not provided for localization, using default locale.");
            requested = DEFAULT_LOCALE;
        } else {
            requested = resolveLocale(op);
        }
        return resolveMessage(e, requested);
    }

    private static String resolveMessage(LocalizableValidationException e, Locale locale) {

        if (locale == null) {
            Logger.getAnonymousLogger().fine("Locale not provided for localization, using default locale.");
            locale = DEFAULT_LOCALE;
        }

        ResourceBundle messages = ResourceBundle.getBundle(MESSAGES_BASE_FILENAME, locale);

        String message;
        try {
            message = messages.getString(e.getErrorMessageCode());
        } catch (MissingResourceException ex) {
            message = e.getMessage();
        }

        MessageFormat f = new MessageFormat(message, locale);
        message = f.format(e.getArguments());

        return message;
    }

    public static Locale resolveLocale(Operation op) {
        String requestedLangs = op.getRequestHeader(Operation.ACCEPT_LANGUAGE_HEADER);

        if (requestedLangs == null) {
            return DEFAULT_LOCALE;
        }

        List<Locale> locales = Locale.LanguageRange.parse(requestedLangs).stream()
                .map(range -> new Locale(range.getRange()))
                .collect(Collectors.toList());

        for (Locale locale : locales) {
            if (SupportedLocales.isSupported(locale)) {
                return locale;
            }
        }

        return DEFAULT_LOCALE;
    }
}
