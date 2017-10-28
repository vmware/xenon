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
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Logger;

public class SupportedLocales {

    private static final String MESSAGES_BASE_FILENAME = "messages_";

    private static final String MESSAGES_FOLDER = "i18n";

    private static final CharSequence PROPERTIES_SUFFIX = ".properties";

    private static Map<String, Locale> supportedLocales = loadSupportedLocales();

    private SupportedLocales() {
    }

    public static boolean isSupported(Locale locale) {
        return supportedLocales.get(locale.toString()) != null;
    }

    private static Map<String, Locale> loadSupportedLocales() {

        String[] messageFiles = getMessageFiles();

        Map<String, Locale> supportedLocales = new HashMap<>(messageFiles.length);
        for (String messageFile : messageFiles) {
            Locale locale = parseLocale(messageFile);
            supportedLocales.put(locale.toString(), locale);
        }

        return supportedLocales;
    }

    private static String[] getMessageFiles() {
        URL url = LocalizationUtil.class.getClassLoader().getResource(MESSAGES_FOLDER);
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
                Logger.getAnonymousLogger().warning(
                        String.format("Unable to load message files from %s: %s ", url, e));
            }
        } else if ("jar".equals(url.getProtocol())) {
            try (FileSystem fileSystem = FileSystems.newFileSystem(url.toURI(),
                    Collections.emptyMap())) {
                Path myPath = fileSystem.getPath(MESSAGES_FOLDER);
                messageFiles = Files.walk(myPath, 1)
                        .filter(path -> path.toString().contains(MESSAGES_BASE_FILENAME))
                        .map(path -> path.getFileName().toString())
                        .toArray(String[]::new);
            } catch (IOException | URISyntaxException e) {
                Logger.getAnonymousLogger().warning(
                        String.format("Unable to load message files from %s: %s ", url, e));
            }
        }

        if (messageFiles == null) {
            messageFiles = new String[0];
        }

        return messageFiles;
    }

    private static Locale parseLocale(String messageFilename) {
        String localeValue = messageFilename.replace(MESSAGES_BASE_FILENAME, "")
                .replace(PROPERTIES_SUFFIX, "");
        Locale locale = null;
        if (localeValue.isEmpty()) {
            locale = LocalizationUtil.DEFAULT_LOCALE;
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
}
