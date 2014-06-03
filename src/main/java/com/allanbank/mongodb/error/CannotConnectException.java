/*
 * #%L
 * CannotConnectException.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import com.allanbank.mongodb.MongoDbException;

/**
 * CannotConnectException is thrown to report a failure when attempting to
 * connect to MongoDB.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CannotConnectException extends MongoDbException {

    /** Serialization exception for the class. */
    private static final long serialVersionUID = 1729264905521755667L;

    /**
     * Creates a new CannotConnectException.
     */
    public CannotConnectException() {
        super();
    }

    /**
     * Creates a new CannotConnectException.
     * 
     * @param message
     *            Message for the exception.
     */
    public CannotConnectException(final String message) {
        super(message);
    }

    /**
     * Creates a new CannotConnectException.
     * 
     * @param message
     *            Message for the exception.
     * @param cause
     *            The cause of the error.
     */
    public CannotConnectException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new CannotConnectException.
     * 
     * @param cause
     *            The cause of the error.
     */
    public CannotConnectException(final Throwable cause) {
        super(cause);
    }
}
