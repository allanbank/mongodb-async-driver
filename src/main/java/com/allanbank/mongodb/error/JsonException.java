/*
 * #%L
 * JsonException.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.error;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.MongoDbException;

/**
 * JsonException provides an exception to throw when processing JSON documents
 * fail.
 *
 * @api.yes This exception is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class JsonException
        extends MongoDbException {

    /** Serialization version of the class. */
    private static final long serialVersionUID = 8248891467581639959L;

    /**
     * Creates a new JsonParseException.
     */
    public JsonException() {
        super();
    }

    /**
     * Creates a new JsonParseException.
     *
     * @param message
     *            Reason for the exception.
     */
    public JsonException(final String message) {
        super(message);
    }

    /**
     * Creates a new JsonParseException.
     *
     * @param message
     *            Reason for the exception.
     * @param cause
     *            The exception causing the MongoDbException.
     */
    public JsonException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new JsonParseException.
     *
     * @param cause
     *            The exception causing the MongoDbException.
     */
    public JsonException(final Throwable cause) {
        super(cause);
    }
}
