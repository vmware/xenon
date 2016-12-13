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

import java.io.File;
import java.io.FilenameFilter;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class SupportedLocales {

    private static final String PROPERTIES_SUFFIX = ".properties";

    private static final String PROPERTIES_FILE_EXTENSION = ".properties";

    private static final String MESSAGES_BASE_FILENAME = "messages_";

    private static final String MESSAGES_FOLDER = "i18n";

    private static List<Locale> supportedLocales = loadSupportedLocales();

    private SupportedLocales() {
    }

    public static boolean isSupported(Locale locale) {
        return supportedLocales.stream()
                .filter(l -> {
                    boolean langSame = l.getLanguage().equals(locale.getLanguage());
                    if (langSame && locale.getLanguage() != null) {
                        return locale.getCountry().equals(l.getCountry());
                    }

                    return langSame;
                })
                .count() > 0;
    }

    private static List<Locale> loadSupportedLocales() {
        String[] messageFiles = getMessageFiles();

        List<Locale> supportedLocales = new ArrayList<>(messageFiles.length);
        for (String messageFile : messageFiles) {
            Locale locale = parseLocale(messageFile);
            supportedLocales.add(locale);
        }

        return supportedLocales;
    }

    private static String[] getMessageFiles() {
        URL url = LocalizationManager.class.getClassLoader().getResource(MESSAGES_FOLDER);
        String[] messageFiles = null;
        if ("file".equals(url.getProtocol())) {
            try {
                messageFiles = new File(url.toURI()).list(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.contains(MESSAGES_BASE_FILENAME);
                    }
                });
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }

        if (messageFiles == null) {
            messageFiles = new String[0];
        }

        if (messageFiles.length == 0) {
        }

        return messageFiles;
    }

    private static Locale parseLocale(String messageFilename) {
        String localeValue = messageFilename.replace(MESSAGES_BASE_FILENAME, "")
                .replace(PROPERTIES_SUFFIX, "");
        Locale locale = null;
        if (localeValue.isEmpty()) {
            locale = LocalizationManager.DEFAULT_LOCALE;
        } else {
            localeValue = localeValue.replace("_", "-");
            locale = Locale.forLanguageTag(localeValue);
        }

        if (locale.getLanguage() == null || locale.getLanguage().isEmpty()) {
            throw new IllegalArgumentException("No valid Locale could be parsed for: "
                    + localeValue);
        }
        return locale;
    }

    public static String buildLocaleMessagesFileName(Locale locale) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(MESSAGES_FOLDER);
        stringBuilder.append(File.separator);
        stringBuilder.append(MESSAGES_BASE_FILENAME);
        stringBuilder.append(locale.getLanguage());

        if (locale.getCountry() != null && !locale.getCountry().isEmpty()) {
            stringBuilder.append("_");
            stringBuilder.append(locale.getCountry());
        }

        stringBuilder.append(PROPERTIES_FILE_EXTENSION);
        return stringBuilder.toString();
    }


}
