/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.socket;

import java.io.IOException;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.BufferingBsonOutputStream;
import com.allanbank.mongodb.bson.io.SizeOfVisitor;
import com.allanbank.mongodb.client.Message;
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
    public Version getRequiredServerVersion() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validateSize(final SizeOfVisitor visitor,
            final int maxDocumentSize) throws DocumentToLargeException {
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