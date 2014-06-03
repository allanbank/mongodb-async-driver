/*
 * #%L
 * RedactOption.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package com.allanbank.mongodb.builder;

/**
 * RedactOption provides options for what to do when evaluating a
 * {@code $redact} condition within the {@link Aggregate} pipeline.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum RedactOption {
    /**
     * Option for the redact to descend into the sub-documents of the current
     * document.
     */
    DESCEND("$$DESCEND"),

    /** Option to keep the current document and not inspect the sub-documents. */
    KEEP("$$KEEP"),

    /** Option to prune the current document. */
    PRUNE("$$PRUNE");

    /** The token for the option. */
    private final String myToken;

    /**
     * Creates a new RedactOption.
     * 
     * @param token
     *            The token for the option.
     */
    private RedactOption(final String token) {
        myToken = token;
    }

    /**
     * Returns the token for the option.
     * 
     * @return The token for the option.
     */
    public String getToken() {
        return myToken;
    }
}
