/*
 * #%L
 * Command.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.message;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.JsonSerializationVisitor;
import com.allanbank.mongodb.bson.impl.EmptyDocument;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.BufferingBsonOutputStream;
import com.allanbank.mongodb.bson.io.StringEncoder;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.client.VersionRange;
import com.allanbank.mongodb.error.DocumentToLargeException;

/**
 * Helper class to make generating command queries easier. Commands are
 * communicated to the server as {@link Operation#QUERY} messages. We don't use
 * the Query class as a base class as it adds a lot of weight to the commands.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Command extends AbstractMessage {

    /** The collection to use when issuing commands to the database. */
    public static final String COMMAND_COLLECTION = "$cmd";

    /** The amount of head room for jumbo documents. */
    private static final int HEADROOM = 16 * 1024;

    /**
     * If true then the command document is allowed to slightly exceed the
     * document size limit.
     */
    private boolean myAllowJumbo = false;

    /** The command's document. */
    private final Document myCommand;

    /** The command's document to use in routing decisions. */
    private final Document myRoutingDocument;

    /**
     * Create a new Command.
     * 
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection the command is using. This should
     *            be the real collection and not {@value #COMMAND_COLLECTION} if
     *            the real collection is known.
     * @param commandDocument
     *            The command document containing the command and options.
     */
    public Command(final String databaseName, final String collectionName,
            final Document commandDocument) {
        this(databaseName, collectionName, commandDocument,
                ReadPreference.PRIMARY);
    }

    /**
     * Create a new Command.
     * 
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection the command is using. This should
     *            be the real collection and not {@value #COMMAND_COLLECTION} if
     *            the real collection is known.
     * @param commandDocument
     *            The command document containing the command and options.
     * @param routingDocument
     *            The document that should be used for routing the command.
     * @param readPreference
     *            The preference for which servers to use to retrieve the
     *            results.
     * @param requiredServerVersion
     *            The required version of the server to support processing the
     *            message.
     */
    public Command(final String databaseName, final String collectionName,
            final Document commandDocument, final Document routingDocument,
            final ReadPreference readPreference,
            final VersionRange requiredServerVersion) {
        super(databaseName, collectionName, readPreference,
                requiredServerVersion);

        myCommand = commandDocument;
        myRoutingDocument = routingDocument;
    }

    /**
     * Create a new Command.
     * 
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection the command is using. This should
     *            be the real collection and not {@value #COMMAND_COLLECTION} if
     *            the real collection is known.
     * @param commandDocument
     *            The command document containing the command and options.
     * @param readPreference
     *            The preference for which servers to use to retrieve the
     *            results.
     */
    public Command(final String databaseName, final String collectionName,
            final Document commandDocument, final ReadPreference readPreference) {
        this(databaseName, collectionName, commandDocument, readPreference,
                null);
    }

    /**
     * Create a new Command.
     * 
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection the command is using. This should
     *            be the real collection and not {@value #COMMAND_COLLECTION} if
     *            the real collection is known.
     * @param commandDocument
     *            The command document containing the command and options.
     * @param readPreference
     *            The preference for which servers to use to retrieve the
     *            results.
     * @param requiredServerVersion
     *            The required version of the server to support processing the
     *            message.
     */
    public Command(final String databaseName, final String collectionName,
            final Document commandDocument,
            final ReadPreference readPreference,
            final VersionRange requiredServerVersion) {
        this(databaseName, collectionName, commandDocument,
                EmptyDocument.INSTANCE, readPreference, requiredServerVersion);
    }

    /**
     * Determines if the passed object is of this same type as this object and
     * if so that its fields are equal.
     * 
     * @param object
     *            The object to compare to.
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object object) {
        boolean result = false;
        if (this == object) {
            result = true;
        }
        else if ((object != null) && (getClass() == object.getClass())) {
            final Command other = (Command) object;

            result = super.equals(object) && myCommand.equals(other.myCommand);
        }
        return result;
    }

    /**
     * Returns the command's document.
     * 
     * @return The command's document.
     */
    public Document getCommand() {
        return myCommand;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the command name.
     * </p>
     */
    @Override
    public String getOperationName() {
        // Return the value for the first element in the document.
        final Iterator<Element> iter = myCommand.iterator();
        if (iter.hasNext()) {
            return iter.next().getName();
        }
        // Not expected. Command documents should have atleast one element. Just
        // return a generic name here.
        return "command";
    }

    /**
     * Returns the routingDocument value.
     * 
     * @return The routingDocument value.
     */
    public Document getRoutingDocument() {
        return myRoutingDocument;
    }

    /**
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + super.hashCode();
        result = (31 * result) + myCommand.hashCode();
        return result;
    }

    /**
     * Returns true if the command document is allowed to slightly exceed the
     * document size limit.
     * 
     * @return True if the command document is allowed to slightly exceed the
     *         document size limit.
     */
    public boolean isAllowJumbo() {
        return myAllowJumbo;
    }

    /**
     * If set to true then the command document is allowed to slightly exceed
     * the document size limit. This allows us to pack a full size document in a
     * insert command.
     * 
     * @param allowJumbo
     *            If true then the command document is allowed to slightly
     *            exceed the document size limit.
     */
    public void setAllowJumbo(final boolean allowJumbo) {
        myAllowJumbo = allowJumbo;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the size of the {@link Command}.
     * </p>
     */
    @Override
    public int size() {
        int size = HEADER_SIZE + 18; // See below.
        // size += 4; // flags;
        size += StringEncoder.utf8Size(myDatabaseName);
        // size += 1; // StringEncoder.utf8Size(".");
        // size += 4; // StringEncoder.utf8Size(COMMAND_COLLECTION);
        // size += 1; // \0 on the CString.
        // size += 4; // numberToSkip
        // size += 4; // numberToReturn
        size += myCommand.size();

        return size;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a human readable form of the command.
     * </p>
     */
    @Override
    public String toString() {
        final StringWriter builder = new StringWriter();
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                builder, true);

        builder.append(getOperationName());
        builder.append("([db=");
        builder.append(myDatabaseName);
        builder.append(",collection=");
        builder.append(myCollectionName);

        if (getReadPreference() != null) {
            builder.append(",readPreference=");
            builder.append(String.valueOf(getReadPreference()));
        }
        if (getRequiredVersionRange() != null) {
            builder.append(",requiredVersionRange=");
            builder.append(String.valueOf(getRequiredVersionRange()));
        }
        builder.append("]:");
        myCommand.accept(visitor);
        builder.append(")");

        return builder.toString();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to make sure the command document is not too large.
     * </p>
     */
    @Override
    public void validateSize(final int maxDocumentSize)
            throws DocumentToLargeException {
        final long size = myCommand.size();

        if (isAllowJumbo()) {
            if ((maxDocumentSize + HEADROOM) < size) {
                throw new DocumentToLargeException((int) size, maxDocumentSize
                        + HEADROOM, myCommand);
            }
        }
        else if (maxDocumentSize < size) {
            throw new DocumentToLargeException((int) size, maxDocumentSize,
                    myCommand);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the Command as a {@link Operation#QUERY} message.
     * </p>
     */
    @Override
    public void write(final int messageId, final BsonOutputStream out)
            throws IOException {
        final int numberToSkip = 0;
        final int numberToReturn = -1; // Unlimited
        final int flags = computeFlags();

        int size = HEADER_SIZE;
        size += 4; // flags;
        size += out.sizeOfCString(myDatabaseName, ".", COMMAND_COLLECTION);
        size += 4; // numberToSkip
        size += 4; // numberToReturn
        size += myCommand.size();

        writeHeader(out, messageId, 0, Operation.QUERY, size);
        out.writeInt(flags);
        out.writeCString(myDatabaseName, ".", COMMAND_COLLECTION);
        out.writeInt(numberToSkip);
        out.writeInt(numberToReturn);
        out.writeDocument(myCommand);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the Command as a {@link Operation#QUERY} message.
     * </p>
     */
    @Override
    public void write(final int messageId, final BufferingBsonOutputStream out)
            throws IOException {
        final int numberToSkip = 0;
        final int numberToReturn = -1; // Unlimited
        final int flags = computeFlags();

        final long start = writeHeader(out, messageId, 0, Operation.QUERY);
        out.writeInt(flags);
        out.writeCString(myDatabaseName, ".", COMMAND_COLLECTION);
        out.writeInt(numberToSkip);
        out.writeInt(numberToReturn);
        out.writeDocument(myCommand);
        finishHeader(out, start);

        out.flushBuffer();
    }

    /**
     * Computes the message flags bit field.
     * 
     * @return The message flags bit field.
     */
    private int computeFlags() {
        int flags = 0;
        if (getReadPreference().isSecondaryOk()) {
            flags += Query.REPLICA_OK_FLAG_BIT;
        }
        return flags;
    }
}
