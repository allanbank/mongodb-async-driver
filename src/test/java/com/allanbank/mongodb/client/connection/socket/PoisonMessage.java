/*
 * #%L
 * PoisonMessage.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.connection.socket;

import java.io.IOException;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.BufferingBsonOutputStream;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.VersionRange;
import com.allanbank.mongodb.error.DocumentToLargeException;

/**
 * PoisonMessage provides a message that throws an exception when you try to
 * write it.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class PoisonMessage implements Message {

    /** The exception to throw. */
    private final Throwable myToThrow;

    /**
     * Creates a new PoisonMessage.
     * 
     * @param toThrow
     *            The exception to throw.
     */
    public PoisonMessage(final Throwable toThrow) {
        myToThrow = toThrow;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCollectionName() {
        return "c";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDatabaseName() {
        return "f";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getOperationName() {
        return "poison";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReadPreference getReadPreference() {
        return ReadPreference.PRIMARY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VersionRange getRequiredVersionRange() {
        return null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return zero.
     * </p>
     */
    @Override
    public int size() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validateSize(final int maxDocumentSize)
            throws DocumentToLargeException {
        // Nothing.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(final int messageId, final BsonOutputStream out)
            throws IOException {
        if (myToThrow instanceof IOException) {
            throw (IOException) myToThrow;
        }
        else if (myToThrow instanceof RuntimeException) {
            throw (RuntimeException) myToThrow;
        }
        else if (myToThrow instanceof Error) {
            throw (Error) myToThrow;
        }
        else {
            throw new MongoDbException(myToThrow);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(final int messageId, final BufferingBsonOutputStream out)
            throws IOException {
        write(messageId, (BsonOutputStream) null);
    }
}