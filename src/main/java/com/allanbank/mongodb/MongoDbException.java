/*
 * #%L
 * MongoDbException.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Exception base class for all MongoDB exceptions.
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class MongoDbException
        extends RuntimeException {

    /** Serialization version for the class. */
    private static final long serialVersionUID = 8065038814148830471L;

    /**
     * Creates a new MongoDbException.
     */
    public MongoDbException() {
        super();
    }

    /**
     * Creates a new MongoDbException.
     *
     * @param message
     *            Reason for the exception.
     */
    public MongoDbException(final String message) {
        super(message);
    }

    /**
     * Creates a new MongoDbException.
     *
     * @param message
     *            Reason for the exception.
     * @param cause
     *            The exception causing the MongoDbException.
     */
    public MongoDbException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new MongoDbException.
     *
     * @param cause
     *            The exception causing the MongoDbException.
     */
    public MongoDbException(final Throwable cause) {
        super((cause == null) ? "" : cause.getMessage(), cause);
    }

}
