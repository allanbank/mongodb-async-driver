/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.client.message;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.Operation;

/**
 * Base class for a MongoDB message.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractMessage implements Message {

    /** The size of a message header. */
    public static final int HEADER_SIZE = Header.SIZE;

    /** The name of the collection to operate on. */
    protected String myCollectionName;

    /** The name of the database to operate on. */
    protected String myDatabaseName;

    /** The details on which servers may be sent the message. */
    private final ReadPreference myReadPreference;

    /**
     * Create a new AbstractMessage.
     */
    public AbstractMessage() {
        myDatabaseName = "";
        myCollectionName = "";
        myReadPreference = ReadPreference.PRIMARY;
    }

    /**
     * Create a new AbstractMessage.
     * 
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection.
     * @param readPreference
     *            The preferences for which servers to send the message.
     */
    public AbstractMessage(final String databaseName,
            final String collectionName, final ReadPreference readPreference) {
        myDatabaseName = databaseName;
        myCollectionName = collectionName;
        myReadPreference = readPreference;
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

        // This should never return false as derived classes should have
        // verified.
        if ((object != null) && (getClass() == object.getClass())) {
            final AbstractMessage other = (AbstractMessage) object;

            result = myCollectionName.equals(other.myCollectionName)
                    && myDatabaseName.equals(other.myDatabaseName)
                    && myReadPreference.equals(other.myReadPreference);
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
     * {@inheritDoc}
     * <p>
     * Returns the message's read preference.
     * </p>
     */
    @Override
    public ReadPreference getReadPreference() {
        return myReadPreference;
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
        result = (31 * result) + myReadPreference.hashCode();
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