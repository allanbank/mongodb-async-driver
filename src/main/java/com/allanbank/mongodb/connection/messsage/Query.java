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
 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPQUERY"
 * >query</a> documents from the database matching a criteria. Also used to
 * issue commands to the database.
 * 
 * <pre>
 * <code>
 * struct OP_QUERY {
 *     MsgHeader header;                // standard message header
 *     int32     flags;                  // bit vector of query options.  See below for details.
 *     cstring   fullCollectionName;    // "dbname.collectionname"
 *     int32     numberToSkip;          // number of documents to skip
 *     int32     numberToReturn;        // number of documents to return
 *                                      //  in the first OP_REPLY batch
 *     document  query;                 // query object. 
 *   [ document  returnFieldSelector; ] // Optional. Selector indicating the fields
 *                                      //  to return.  
 * }
 * </code>
 * </pre>
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Query extends AbstractMessage {

    /** Flag bit for the await data. */
    public static final int AWAIT_DATA_FLAG_BIT = 0x20;

    /** Flag bit for the exhaust results. */
    public static final int EXHAUST_FLAG_BIT = 0x40;

    /** Flag bit for the no cursor timeout. */
    public static final int NO_CURSOR_TIMEOUT_FLAG_BIT = 0x10;

    /** Flag bit for the OPLOG_REPLAY. */
    public static final int OPLOG_REPLAY_FLAG_BIT = 0x08;

    /** Flag bit for the partial results. */
    public static final int PARTIAL_FLAG_BIT = 0x80;

    /** Flag bit for the replica OK. */
    public static final int REPLICA_OK_FLAG_BIT = 0x04;

    /** Flag bit for the tailable cursors. */
    public static final int TAILABLE_CURSOR_FLAG_BIT = 0x02;

    /**
     * If true and if using a tailable cursor then the connection will block
     * waiting for more data.
     */
    private final boolean myAwaitData;

    /** If true, all results should be returned in multiple results. */
    private final boolean myExhaust;

    /** If true, marks the cursor as not having a timeout. */
    private final boolean myNoCursorTimeout;

    /** The number of documents to be returned. */
    private final int myNumberToReturn;

    /**
     * The number of documents to skip before starting to return documents.
     */
    private final int myNumberToSkip;

    /**
     * If true, return the results found and suppress shard down errors.
     */
    private final boolean myPartial;

    /**
     * The query document containing the expression to select documents from the
     * collection.
     */
    private final Document myQuery;

    /**
     * If true, then the query can be run against a replica which might be
     * slightly behind the primary.
     */
    private final boolean myReplicaOk;

    /** Optional document containing the fields to be returned. */
    private final Document myReturnFields;

    /**
     * If true, then the cursor created should follow additional documents being
     * inserted.
     */
    private final boolean myTailable;

    /**
     * Creates a new Query.
     * 
     * @param header
     *            The header for the query message.
     * @param in
     *            The stream to read the kill_cursors message from.
     * @throws IOException
     *             On a failure reading the kill_cursors message.
     */
    public Query(final Header header, final BsonInputStream in)
            throws IOException {
        final long position = in.getBytesRead();
        final long end = (position + header.getLength()) - Header.SIZE;

        final int flags = in.readInt();
        init(in.readCString());
        myNumberToSkip = in.readInt();
        myNumberToReturn = in.readInt();
        myQuery = in.readDocument();
        if (in.getBytesRead() < end) {
            myReturnFields = in.readDocument();
        }
        else {
            myReturnFields = null;
        }
        myAwaitData = (flags & AWAIT_DATA_FLAG_BIT) == AWAIT_DATA_FLAG_BIT;
        myExhaust = (flags & EXHAUST_FLAG_BIT) == EXHAUST_FLAG_BIT;
        myNoCursorTimeout = (flags & NO_CURSOR_TIMEOUT_FLAG_BIT) == NO_CURSOR_TIMEOUT_FLAG_BIT;
        myPartial = (flags & PARTIAL_FLAG_BIT) == PARTIAL_FLAG_BIT;
        myReplicaOk = (flags & REPLICA_OK_FLAG_BIT) == REPLICA_OK_FLAG_BIT;
        myTailable = (flags & TAILABLE_CURSOR_FLAG_BIT) == TAILABLE_CURSOR_FLAG_BIT;
    }

    /**
     * Creates a new Query.
     * 
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection.
     * @param query
     *            The query document containing the expression to select
     *            documents from the collection.
     * @param returnFields
     *            Optional document containing the fields to be returned.
     * @param numberToReturn
     *            The number of documents to be returned.
     * @param numberToSkip
     *            The number of documents to skip before starting to return
     *            documents.
     * @param tailable
     *            If true, then the cursor created should follow additional
     *            documents being inserted.
     * @param replicaOk
     *            If true, then the query can be run against a replica which
     *            might be slightly behind the primary.
     * @param noCursorTimeout
     *            If true, marks the cursor as not having a timeout.
     * @param awaitData
     *            If true and if using a tailable cursor then the connection
     *            will block waiting for more data.
     * @param exhaust
     *            If true, all results should be returned in multiple results.
     * @param partial
     *            If true, return the results found and suppress shard down
     *            errors.
     */
    public Query(final String databaseName, final String collectionName,
            final Document query, final Document returnFields,
            final int numberToReturn, final int numberToSkip,
            final boolean tailable, final boolean replicaOk,
            final boolean noCursorTimeout, final boolean awaitData,
            final boolean exhaust, final boolean partial) {
        super(databaseName, collectionName);

        myQuery = query;
        myReturnFields = returnFields;
        myNumberToReturn = numberToReturn;
        myNumberToSkip = numberToSkip;
        myTailable = tailable;
        myReplicaOk = replicaOk;
        myNoCursorTimeout = noCursorTimeout;
        myAwaitData = awaitData;
        myExhaust = exhaust;
        myPartial = partial;
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
            final Query other = (Query) object;

            result = super.equals(object)
                    && (myAwaitData == other.myAwaitData)
                    && (myExhaust == other.myExhaust)
                    && (myNoCursorTimeout == other.myNoCursorTimeout)
                    && (myPartial == other.myPartial)
                    && (myReplicaOk == other.myReplicaOk)
                    && (myTailable == other.myTailable)
                    && (myNumberToReturn == other.myNumberToReturn)
                    && (myNumberToSkip == other.myNumberToSkip)
                    && myQuery.equals(other.myQuery)
                    && ((myReturnFields == other.myReturnFields) || ((myReturnFields != null) && myReturnFields
                            .equals(other.myReturnFields)));
        }
        return result;
    }

    /**
     * Returns the number of documents to be returned.
     * 
     * @return The number of documents to be returned.
     */
    public int getNumberToReturn() {
        return myNumberToReturn;
    }

    /**
     * Returns the number of documents to skip before starting to return
     * documents.
     * 
     * @return The number of documents to skip before starting to return
     *         documents.
     */
    public int getNumberToSkip() {
        return myNumberToSkip;
    }

    /**
     * Returns the query document containing the expression to select documents
     * from the collection.
     * 
     * @return The query document containing the expression to select documents
     *         from the collection.
     */
    public Document getQuery() {
        return myQuery;
    }

    /**
     * Returns the optional document containing the fields to be returned.
     * Optional here means this method may return <code>null</code>.
     * 
     * @return The optional document containing the fields to be returned.
     */
    public Document getReturnFields() {
        return myReturnFields;
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
        result = (31 * result) + (myAwaitData ? 1 : 3);
        result = (31 * result) + (myExhaust ? 1 : 7);
        result = (31 * result) + (myNoCursorTimeout ? 1 : 11);
        result = (31 * result) + (myPartial ? 1 : 13);
        result = (31 * result) + (myReplicaOk ? 1 : 17);
        result = (31 * result) + (myTailable ? 1 : 19);
        result = (31 * result) + myNumberToReturn;
        result = (31 * result) + myNumberToSkip;
        result = (31 * result) + myQuery.hashCode();
        result = (31 * result)
                + (myReturnFields == null ? 1 : myReturnFields.hashCode());
        return result;
    }

    /**
     * Returns true and if using a tailable cursor then the connection will
     * block waiting for more data.
     * 
     * @return True and if using a tailable cursor then the connection will
     *         block waiting for more data.
     */
    public boolean isAwaitData() {
        return myAwaitData;
    }

    /**
     * Returns true if all results should be returned in multiple results.
     * 
     * @return True if all results should be returned in multiple results.
     */
    public boolean isExhaust() {
        return myExhaust;
    }

    /**
     * Returns true if marking the cursor as not having a timeout.
     * 
     * @return True if marking the cursor as not having a timeout.
     */
    public boolean isNoCursorTimeout() {
        return myNoCursorTimeout;
    }

    /**
     * Returns true if return the results found and suppress shard down errors.
     * 
     * @return True if return the results found and suppress shard down errors..
     */
    public boolean isPartial() {
        return myPartial;
    }

    /**
     * Returns true if the query can be run against a replica which might be
     * slightly behind the primary.
     * 
     * @return True if the query can be run against a replica which might be
     *         slightly behind the primary.
     */
    public boolean isReplicaOk() {
        return myReplicaOk;
    }

    /**
     * Returns true if the cursor created should follow additional documents
     * being inserted.
     * 
     * @return True if the cursor created should follow additional documents
     *         being inserted.
     */
    public boolean isTailable() {
        return myTailable;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the query message.
     * </p>
     * 
     * @see Message#write(int, BsonOutputStream)
     */
    @Override
    public void write(final int messageId, final BsonOutputStream out)
            throws IOException {
        int flags = 0;
        if (myAwaitData) {
            flags += AWAIT_DATA_FLAG_BIT;
        }
        if (myExhaust) {
            flags += EXHAUST_FLAG_BIT;
        }
        if (myNoCursorTimeout) {
            flags += NO_CURSOR_TIMEOUT_FLAG_BIT;
        }
        if (myPartial) {
            flags += PARTIAL_FLAG_BIT;
        }
        if (myReplicaOk) {
            flags += REPLICA_OK_FLAG_BIT;
        }
        if (myTailable) {
            flags += TAILABLE_CURSOR_FLAG_BIT;
        }

        int size = HEADER_SIZE;
        size += 4; // flags;
        size += out.sizeOfCString(myDatabaseName, ".", myCollectionName);
        size += 4; // numberToSkip
        size += 4; // numberToReturn
        size += out.sizeOf(myQuery);
        if (myReturnFields != null) {
            size += out.sizeOf(myReturnFields);
        }

        writeHeader(out, messageId, 0, Operation.QUERY, size);
        out.writeInt(flags);
        out.writeCString(myDatabaseName, ".", myCollectionName);
        out.writeInt(myNumberToSkip);
        out.writeInt(myNumberToReturn);
        out.writeDocument(myQuery);
        if (myReturnFields != null) {
            out.writeDocument(myReturnFields);
        }
    }
}
