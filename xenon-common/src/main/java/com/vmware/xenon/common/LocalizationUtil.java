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
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
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
            Logger.getAnonymousLogger()
                    .fine("Locale not provided for localization, using default locale.");
            locale = DEFAULT_LOCALE;
        }

        ResourceBundle messages = ResourceBundle.getBundle(MESSAGES_BASE_FILENAME, locale,
                new Utf8LoaderControl());

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

    /**
     * This is the default Control implementation with the only difference that UTF-8 is used to
     * load the resource bundles instead of ISO-8859-1. This is needed since some resource bundles
     * contain messages that are not ISO-8859-1 compatible and therefore are wrongly displayed.
     */
    private static class Utf8LoaderControl extends ResourceBundle.Control {
        @Override
        public ResourceBundle newBundle(String baseName, Locale locale, String format,
                ClassLoader loader, boolean reload)
                throws IllegalAccessException, InstantiationException, IOException {
            String bundleName = toBundleName(baseName, locale);
            ResourceBundle bundle = null;
            if (format.equals("java.class")) {
                try {
                    @SuppressWarnings("unchecked")
                    Class<? extends ResourceBundle> bundleClass = (Class<? extends ResourceBundle>) loader
                            .loadClass(bundleName);

                    // If the class isn't a ResourceBundle subclass, throw a
                    // ClassCastException.
                    if (ResourceBundle.class.isAssignableFrom(bundleClass)) {
                        bundle = bundleClass.newInstance();
                    } else {
                        throw new ClassCastException(bundleClass.getName()
                                + " cannot be cast to ResourceBundle");
                    }
                } catch (ClassNotFoundException e) {
                }
            } else if (format.equals("java.properties")) {
                final String resourceName = toResourceName(bundleName, "properties");
                if (resourceName == null) {
                    return bundle;
                }
                final ClassLoader classLoader = loader;
                final boolean reloadFlag = reload;
                InputStream stream = null;
                try {
                    stream = AccessController.doPrivileged(
                            new PrivilegedExceptionAction<InputStream>() {
                                public InputStream run() throws IOException {
                                    InputStream is = null;
                                    if (reloadFlag) {
                                        URL url = classLoader.getResource(resourceName);
                                        if (url != null) {
                                            URLConnection connection = url.openConnection();
                                            if (connection != null) {
                                                // Disable caches to get fresh data for
                                                // reloading.
                                                connection.setUseCaches(false);
                                                is = connection.getInputStream();
                                            }
                                        }
                                    } else {
                                        is = classLoader.getResourceAsStream(resourceName);
                                    }
                                    return is;
                                }
                            });
                } catch (PrivilegedActionException e) {
                    throw (IOException) e.getException();
                }
                if (stream != null) {
                    try {
                        // Wrap the stream in a reader in order to change the default character
                        // encoding (ISO-8859-1) to UTF-8
                        bundle = new PropertyResourceBundle(
                                new InputStreamReader(stream, StandardCharsets.UTF_8));
                    } finally {
                        stream.close();
                    }
                }
            } else {
                throw new IllegalArgumentException("unknown format: " + format);
            }
            return bundle;
        }
    }
}
