/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.messsage;

import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.Operation;

/**
 * Base class for a MongoDB message.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractMessage implements Message {

    /** The size of a message header. */
    public static final int HEADER_SIZE = Header.SIZE;

    /** The name of the collection to operate on. */
    protected String myCollectionName;

    /** The name of the database to operate on. */
    protected String myDatabaseName;

    /**
     * Create a new AbstractMessage.
     */
    public AbstractMessage() {
        myDatabaseName = "";
        myCollectionName = "";
    }

    /**
     * Create a new AbstractMessage.
     * 
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection.
     */
    public AbstractMessage(final String databaseName,
            final String collectionName) {
        myDatabaseName = databaseName;
        myCollectionName = collectionName;
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
            final AbstractMessage other = (AbstractMessage) object;

            result = myCollectionName.equals(other.myCollectionName)
                    && myDatabaseName.equals(other.myDatabaseName);
        }
        return result;
    }

    /**
     * Returns the name of the collection.
     * 
     * @return The name of the collection.
     */
    public String getCollectionName() {
        return myCollectionName;
    }

    /**
     * Returns the name of the database.
     * 
     * @return The name of the database.
     */
    @Override
    public String getDatabaseName() {
        return myDatabaseName;
    }

    /**
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + myCollectionName.hashCode();
        result = (31 * result) + myDatabaseName.hashCode();
        return result;
    }

    /**
     * Initializes the database and collection name from the full database name.
     * 
     * @param name
     *            The full database name.
     */
    protected void init(final String name) {
        final int firstDot = name.indexOf('.');
        if (firstDot >= 0) {
            myDatabaseName = name.substring(0, firstDot);
            myCollectionName = name.substring(firstDot + 1);
        }
        else {
            myDatabaseName = name;
        }
    }

    /**
     * Writes the MsgHeader to the <tt>stream</tt>.
     * 
     * <pre>
     * <code>
     * struct MsgHeader {
     *     int32   messageLength; // total message size, including this
     *     int32   requestID;     // identifier for this message
     *     int32   responseTo;    // requestID from the original request
     *                            //   (used in reponses from db)
     *     int32   opCode;        // request type - see table below
     * }
     * </code>
     * </pre>
     * 
     * @param stream
     *            The stream to write to.
     * @param requestId
     *            The requestID from above.
     * @param responseTo
     *            The responseTo from above.
     * @param op
     *            The operation for the opCode field.
     * @param length
     *            The length of the message including the header.
     */
    protected void writeHeader(final BsonOutputStream stream,
            final int requestId, final int responseTo, final Operation op,
            final int length) {
        stream.writeInt(length);
        stream.writeInt(requestId);
        stream.writeInt(responseTo);
        stream.writeInt(op.getCode());
    }

}