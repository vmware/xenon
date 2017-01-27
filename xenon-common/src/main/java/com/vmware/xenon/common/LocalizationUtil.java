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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Localization utility for the purpose of translating localization error codes to localized messages.
 */
public class LocalizationUtil {

    private static final String PROPERTIES_EXT = "properties";

    private static final String I18N_FOLDER = "i18n";

    public static final Locale DEFAULT_LOCALE = Locale.ENGLISH;

    private static final String MESSAGES_BASE_FILENAME = "/messages";

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

        String message  = "";
        try {
            Enumeration<URL> urls = e.getClass().getClassLoader().getResources(I18N_FOLDER);
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                ResourceBundle messages = getResourceBundle(locale, url);

                if (messages.containsKey(e.getErrorMessageCode())) {
                    message = messages.getString(e.getErrorMessageCode());
                    break;
                }
            }

            if (message.isEmpty()) {
                message = e.getMessage();
            } else {
                MessageFormat f = new MessageFormat(message, locale);
                message = f.format(e.getArguments());
            }
        } catch (MissingResourceException | IOException ex) {
            message = e.getMessage();
        }

        return message;
    }

    private static ResourceBundle getResourceBundle(Locale locale, URL url) {
        // Load the file from the file system instead of the classpath in order to be able to load by
        // absolute path each of the resource files i18n/messages_*.properties. URL here represents the
        // path to the current resources/i18n folder.
        ResourceBundle messages = ResourceBundle.getBundle(url.getPath() + MESSAGES_BASE_FILENAME, locale,
                new ResourceBundle.Control() {
                @Override
                public ResourceBundle newBundle(String baseName, Locale locale,
                        String format, ClassLoader loader, boolean reload)
                    throws IllegalAccessException, InstantiationException,
                    IOException {
                    if (format.equals("java.properties")) {
                        String bundleName = toBundleName(baseName, locale);
                        final String resourceName = toResourceName(bundleName, PROPERTIES_EXT);

                        InputStream stream = null;
                        try {
                            stream = AccessController.doPrivileged(
                                new PrivilegedExceptionAction<InputStream>() {
                                    @Override
                                    public InputStream run() throws IOException {
                                        InputStream is = new FileInputStream(resourceName);
                                        return is;
                                    }
                                });
                        } catch (PrivilegedActionException e) {
                            throw (IOException) e.getException();
                        }
                        ResourceBundle bundle = null;
                        if (stream != null) {
                            try {
                                bundle = new PropertyResourceBundle(stream);
                            } finally {
                                stream.close();
                            }
                        }
                        return bundle;
                    } else {
                        return super.newBundle(baseName, locale, format, loader, reload);
                    }
                }
            });
        return messages;
    }

    public static Locale resolveLocale(Operation op) {
        String requestedLangs = op.getRequestHeader(Operation.ACCEPT_LANGUAGE_HEADER);

        if (requestedLangs == null) {
            return DEFAULT_LOCALE;
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
