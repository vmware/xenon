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

public class LocalizableValidationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private String systemMessage;
    private String errorMessageCode;
    private Map<String, String> arguments;

    public LocalizableValidationException(String systemMsg, String errorMessageCode,
            Map<String, String> errorMessageArguments) {
        this.setSystemMessage(systemMsg);
        this.errorMessageCode = errorMessageCode;
        this.arguments = errorMessageArguments;
    }

    public String getErrorMessageCode() {
        return this.errorMessageCode;
    }

    public void setErrorMessageCode(String errorMessageCode) {
        this.errorMessageCode = errorMessageCode;
    }

    public Map<String, String> getArguments() {
        return this.arguments;
    }

    public void setArguments(Map<String, String> arguments) {
        this.arguments = arguments;
    }

    public String getSystemMessage() {
        return this.systemMessage;
    }

    public void setSystemMessage(String systemMessage) {
        this.systemMessage = systemMessage;
    }
}
