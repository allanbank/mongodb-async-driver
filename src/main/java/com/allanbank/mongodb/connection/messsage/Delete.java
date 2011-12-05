/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.messsage;

import java.io.IOException;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.Operation;

/**
 * Message to <a href=
 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPDELETE"
 * >delete</a> documents from a collection. The format of the message is:
 * 
 * <pre>
 * <code>
 * struct {
 *     MsgHeader header;             // standard message header
 *     int32     ZERO;               // 0 - reserved for future use
 *     cstring   fullCollectionName; // "dbname.collectionname"
 *     int32     flags;              // bit vector - see below for details.
 *     document  selector;           // query object.  See below for details.
 * }
 * </code>
 * </pre>
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Delete extends AbstractMessage {

    /** The flag bit for performing a single delete only. */
    public static final int SINGLE_DELETE_BIT = 1;

    /** The query for selecting the documents to delete. */
    private final Document myQuery;

    /**
     * If true, only the first document found should be deleted, otherwise all
     * matching documents should be deleted.
     */
    private final boolean mySingleDelete;

    /**
     * Create a new Delete message.
     * 
     * @param in
     *            The stream to read the delete message from.
     * @throws IOException
     *             On a failure reading the delete message.
     */
    public Delete(final BsonInputStream in) throws IOException {
        in.readInt(); // reserved - 0.
        init(in.readCString());
        final int flags = in.readInt();
        myQuery = in.readDocument();
        mySingleDelete = (flags & SINGLE_DELETE_BIT) == SINGLE_DELETE_BIT;
    }

    /**
     * Create a new Delete message.
     * 
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection.
     * @param query
     *            The query document for selecting documents to delete.
     * @param singleDelete
     *            If true, only the first document found should be deleted,
     *            otherwise all matching documents should be deleted.
     */
    public Delete(final String databaseName, final String collectionName,
            final Document query, final boolean singleDelete) {
        super(databaseName, collectionName);
        myQuery = query;
        mySingleDelete = singleDelete;
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
            final Delete other = (Delete) object;

            result = super.equals(object)
                    && (mySingleDelete == other.mySingleDelete)
                    && myQuery.equals(other.myQuery);
        }
        return result;
    }

    /**
     * Returns the query {@link Document}.
     * 
     * @return The query {@link Document}.
     */
    public Document getQuery() {
        return myQuery;
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
        result = (31 * result) + (mySingleDelete ? 1 : 3);
        result = (31 * result) + myQuery.hashCode();
        return result;
    }

    /**
     * Returns if only a single or all matching documents should be deleted.
     * 
     * @return True if only the first document found will be deleted, otherwise
     *         all matching documents will be deleted.
     */
    public boolean isSingleDelete() {
        return mySingleDelete;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write a delete message.
     * </p>
     * 
     * @see Message#write
     */
    @Override
    public void write(final int messageId, final BsonOutputStream out)
            throws IOException {
        int flags = 0;
        if (mySingleDelete) {
            flags += SINGLE_DELETE_BIT;
        }

        int size = HEADER_SIZE;
        size += 4; // reserved - 0;
        size += out.sizeOfCString(myDatabaseName, ".", myCollectionName);
        size += 4; // flags
        size += out.sizeOf(myQuery);

        writeHeader(out, messageId, 0, Operation.DELETE, size);
        out.writeInt(0);
        out.writeCString(myDatabaseName, ".", myCollectionName);
        out.writeInt(flags);
        out.writeDocument(myQuery);
    }
}
