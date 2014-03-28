/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import java.io.IOException;
import java.util.Iterator;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.BufferingBsonOutputStream;
import com.allanbank.mongodb.bson.io.SizeOfVisitor;
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

    /** The amount of headroom for jumbo documents. */
    private static final int HEADROOM = 16 * 1024;

    /**
     * If true then the command document is allowed to slightly exceed the
     * document size limit.
     */
    private boolean myAllowJumbo = false;

    /** The command's document. */
    private final Document myCommand;

    /**
     * The size of the message. If negative then the size has not been computed.
     */
    private int myMessageSize;

    /**
     * Create a new Command.
     * 
     * @param databaseName
     *            The name of the database.
     * @param commandDocument
     *            The command document containing the command and options.
     */
    public Command(final String databaseName, final Document commandDocument) {
        this(databaseName, commandDocument, ReadPreference.PRIMARY);
    }

    /**
     * Create a new Command.
     * 
     * @param databaseName
     *            The name of the database.
     * @param commandDocument
     *            The command document containing the command and options.
     * @param readPreference
     *            The preference for which servers to use to retrieve the
     *            results.
     */
    public Command(final String databaseName, final Document commandDocument,
            final ReadPreference readPreference) {
        this(databaseName, commandDocument, readPreference, null);
    }

    /**
     * Create a new Command.
     * 
     * @param databaseName
     *            The name of the database.
     * @param commandDocument
     *            The command document containing the command and options.
     * @param readPreference
     *            The preference for which servers to use to retrieve the
     *            results.
     * @param requiredServerVersion
     *            The required version of the server to support processing the
     *            message.
     */
    public Command(final String databaseName, final Document commandDocument,
            final ReadPreference readPreference,
            final VersionRange requiredServerVersion) {
        super(databaseName, COMMAND_COLLECTION, readPreference,
                requiredServerVersion);

        myCommand = commandDocument;
        myMessageSize = -1;
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
     * Overridden to return a human readable form of the command.
     * </p>
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("Command[");
        builder.append(getOperationName());
        builder.append(", db=");
        builder.append(myDatabaseName);
        builder.append(", collection=");
        builder.append(myCollectionName);
        if (getReadPreference() != null) {
            builder.append(", readPreference=");
            builder.append(getReadPreference());
        }
        if (getRequiredVersionRange() != null) {
            builder.append(", requiredVersionRange=");
            builder.append(getRequiredVersionRange());
        }
        builder.append("]: ");
        builder.append(myCommand);

        return builder.toString();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to make sure the command document is not too large.
     * </p>
     */
    @Override
    public void validateSize(final SizeOfVisitor visitor,
            final int maxDocumentSize) throws DocumentToLargeException {
        if (myMessageSize < 0) {
            visitor.reset();

            if (myCommand != null) {
                myCommand.accept(visitor);
            }

            myMessageSize = visitor.getSize();
        }

        if (isAllowJumbo()) {
            if ((maxDocumentSize + HEADROOM) < myMessageSize) {
                throw new DocumentToLargeException(myMessageSize,
                        maxDocumentSize + HEADROOM, myCommand);
            }
        }
        else if (maxDocumentSize < myMessageSize) {
            throw new DocumentToLargeException(myMessageSize, maxDocumentSize,
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
        size += out.sizeOfCString(myDatabaseName, ".", myCollectionName);
        size += 4; // numberToSkip
        size += 4; // numberToReturn
        size += out.sizeOf(myCommand);

        writeHeader(out, messageId, 0, Operation.QUERY, size);
        out.writeInt(flags);
        out.writeCString(myDatabaseName, ".", myCollectionName);
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
        out.writeCString(myDatabaseName, ".", myCollectionName);
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
