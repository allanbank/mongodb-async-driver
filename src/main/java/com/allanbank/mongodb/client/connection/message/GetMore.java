/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.client.connection.message;

import java.io.IOException;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.SizeOfVisitor;
import com.allanbank.mongodb.client.connection.Message;
import com.allanbank.mongodb.client.connection.Operation;
import com.allanbank.mongodb.error.DocumentToLargeException;

/**
 * Message to <a href=
 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPGETMORE"
 * >getmore</a> documents from a cursor. The format of the message is:
 * 
 * <pre>
 * <code>
 * struct {
 *     MsgHeader header;             // standard message header
 *     int32     ZERO;               // 0 - reserved for future use
 *     cstring   fullCollectionName; // "dbname.collectionname"
 *     int32     numberToReturn;     // number of documents to return
 *     int64     cursorID;           // cursorID from the OP_REPLY
 * }
 * </code>
 * </pre>
 * 
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class GetMore extends AbstractMessage {

    /** The id for the cursor. */
    private final long myCursorId;

    /** The number of documents to be returned. */
    private final int myNumberToReturn;

    /**
     * Creates a new GetMore.
     * 
     * @param in
     *            The stream to read the get_more message from.
     * @throws IOException
     *             On a failure reading the get_more message.
     */
    public GetMore(final BsonInputStream in) throws IOException {
        in.readInt(); // reserved - 0.
        init(in.readCString());
        myNumberToReturn = in.readInt();
        myCursorId = in.readLong();
    }

    /**
     * Creates a new GetMore.
     * 
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection.
     * @param cursorId
     *            The id of the cursor.
     * @param numberToReturn
     *            The number of documents to return.
     * @param readPreference
     *            The preferences for which server to send the request.
     */
    public GetMore(final String databaseName, final String collectionName,
            final long cursorId, final int numberToReturn,
            final ReadPreference readPreference) {
        super(databaseName, collectionName, readPreference);

        myCursorId = cursorId;
        myNumberToReturn = numberToReturn;
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
            final GetMore other = (GetMore) object;

            result = super.equals(object) && (myCursorId == other.myCursorId)
                    && (myNumberToReturn == other.myNumberToReturn);
        }
        return result;
    }

    /**
     * Returns the id of the cursor to get more documents from.
     * 
     * @return The id of the cursor to get more documents from.
     */
    public long getCursorId() {
        return myCursorId;
    }

    /**
     * Return the number of documents to return from the cursor.
     * 
     * @return The number of documents to return from the cursor.
     */
    public int getNumberToReturn() {
        return myNumberToReturn;
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
        result = (31 * result) + (int) (myCursorId >> Integer.SIZE);
        result = (31 * result) + (int) myCursorId;
        result = (31 * result) + myNumberToReturn;
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overrridden to be a no-op since the size of a GetMore is fixed.
     * </p>
     */
    @Override
    public void validateSize(final SizeOfVisitor visitor,
            final int maxDocumentSize) throws DocumentToLargeException {
        // Can't be too large.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write a get_more message.
     * </p>
     * 
     * @see Message#write(int, BsonOutputStream)
     */
    @Override
    public void write(final int messageId, final BsonOutputStream out)
            throws IOException {
        int size = HEADER_SIZE;
        size += 4; // reserved - 0;
        size += out.sizeOfCString(myDatabaseName, ".", myCollectionName);
        size += 4; // numberToReturn - int32
        size += 8; // cursorId - long(64)

        writeHeader(out, messageId, 0, Operation.GET_MORE, size);
        out.writeInt(0);
        out.writeCString(myDatabaseName, ".", myCollectionName);
        out.writeInt(myNumberToReturn);
        out.writeLong(myCursorId);
    }
}
