/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import java.io.IOException;
import java.util.Iterator;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.SizeOfVisitor;
import com.allanbank.mongodb.client.Operation;
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
            final Version requiredServerVersion) {
        super(databaseName, COMMAND_COLLECTION, readPreference,
                requiredServerVersion);

        myCommand = commandDocument;
        myMessageSize = -1;
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

        if (maxDocumentSize < myMessageSize) {
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
        int flags = 0;
        if (getReadPreference().isSecondaryOk()) {
            flags += Query.REPLICA_OK_FLAG_BIT;
        }

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

}
