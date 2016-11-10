/*
 * #%L
 * Query.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.element.JsonSerializationVisitor;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.BufferingBsonOutputStream;
import com.allanbank.mongodb.bson.io.StringEncoder;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.error.DocumentToLargeException;

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
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Query
        extends AbstractMessage
        implements CursorableMessage {

    /** Flag bit for the await data. */
    public static final int AWAIT_DATA_FLAG_BIT = 0x20;

    /** The default batch size for a MongoDB query. */
    public static final int DEFAULT_BATCH_SIZE = 101;

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

    /** The number of documents to be returned in each batch. */
    private final int myBatchSize;

    /** If true, all results should be returned in multiple results. */
    private final boolean myExhaust;

    /** The maximum number of documents to be returned. */
    private final int myLimit;

    /**
     * The size of the message. If negative then the size has not been computed.
     */
    private int myMessageSize;

    /** If true, marks the cursor as not having a timeout. */
    private final boolean myNoCursorTimeout;

    /** The number of documents to be returned in the first batch. */
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
        myTailable = (flags & TAILABLE_CURSOR_FLAG_BIT) == TAILABLE_CURSOR_FLAG_BIT;

        myLimit = 0;
        myBatchSize = 0;
        myMessageSize = -1;
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
     * @param batchSize
     *            The number of documents to be returned in each batch.
     * @param limit
     *            The limit on the number of documents to return.
     * @param numberToSkip
     *            The number of documents to skip before starting to return
     *            documents.
     * @param tailable
     *            If true, then the cursor created should follow additional
     *            documents being inserted.
     * @param readPreference
     *            The preference for which servers to use to retrieve the
     *            results.
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
            final int batchSize, final int limit, final int numberToSkip,
            final boolean tailable, final ReadPreference readPreference,
            final boolean noCursorTimeout, final boolean awaitData,
            final boolean exhaust, final boolean partial) {
        super(databaseName, collectionName, readPreference, QueryVersionVisitor
                .version(query));

        myQuery = query;
        myReturnFields = returnFields;
        myLimit = limit;
        myBatchSize = batchSize;
        myNumberToSkip = numberToSkip;
        myTailable = tailable;
        myNoCursorTimeout = noCursorTimeout;
        myAwaitData = awaitData;
        myExhaust = exhaust;
        myPartial = partial;
        myMessageSize = -1;

        if (isBatchSizeSet()) {
            if (isLimitSet() && (myLimit <= myBatchSize)) {
                myNumberToReturn = myLimit;
            }
            else {
                myNumberToReturn = myBatchSize;
            }
        }
        else if (isLimitSet() && (myLimit <= DEFAULT_BATCH_SIZE)) {
            myNumberToReturn = myLimit;
        }
        else {
            myNumberToReturn = 0;
        }
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
                    && (myTailable == other.myTailable)
                    && (myBatchSize == other.myBatchSize)
                    && (myLimit == other.myLimit)
                    && (myNumberToReturn == other.myNumberToReturn)
                    && (myNumberToSkip == other.myNumberToSkip)
                    && myQuery.equals(other.myQuery)
                    && ((myReturnFields == other.myReturnFields) || ((myReturnFields != null) && myReturnFields
                            .equals(other.myReturnFields)));
        }
        return result;
    }

    /**
     * Returns the number of documents to be returned in each batch of results.
     *
     * @return The number of documents to be returned in each batch of results.
     */
    @Override
    public int getBatchSize() {
        return myBatchSize;
    }

    /**
     * Returns the total number of documents to be returned.
     *
     * @return The total number of documents to be returned.
     */
    @Override
    public int getLimit() {
        return myLimit;
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
     * {@inheritDoc}
     * <p>
     * Overridden to return the name of the operation: "QUERY".
     * </p>
     */
    @Override
    public String getOperationName() {
        return Operation.QUERY.name();
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
        result = (31 * result) + (myTailable ? 1 : 19);
        result = (31 * result) + myBatchSize;
        result = (31 * result) + myLimit;
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
     * Returns true if the batch size is greater than zero.
     *
     * @return True if the batch size is greater than zero.
     */
    public boolean isBatchSizeSet() {
        return 0 < myBatchSize;
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
     * Returns true if the limit is greater than zero.
     *
     * @return True if the limit is greater than zero.
     */
    public boolean isLimitSet() {
        return 0 < myLimit;
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
     * Overridden to return the size of the {@link Query}.
     * </p>
     */
    @Override
    public int size() {

        int size = HEADER_SIZE + 14; // See below.
        // size += 4; // flags;
        size += StringEncoder.utf8Size(myDatabaseName);
        // size += 1; // StringEncoder.utf8Size(".");
        size += StringEncoder.utf8Size(myCollectionName);
        // size += 1; // \0 on the CString.
        // size += 4; // numberToSkip
        // size += 4; // numberToReturn
        size += myQuery.size();
        if (myReturnFields != null) {
            size += myReturnFields.size();
        }

        return size;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a string form of the query.
     * </p>
     */
    @Override
    public String toString() {
        final StringWriter builder = new StringWriter();
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                builder, true);

        builder.append("Query(");

        emit(builder, myAwaitData, "await");
        emit(builder, myExhaust, "exhaust");
        emit(builder, myNoCursorTimeout, "noCursorTimeout");
        emit(builder, myPartial, "partial");
        emit(builder, myTailable, "tailable");

        builder.append("batchSize=");
        builder.append(String.valueOf(myBatchSize));
        builder.append(",limit=");
        builder.append(String.valueOf(myLimit));
        builder.append(",messageSize=");
        builder.append(String.valueOf(myMessageSize));
        builder.append(",numberToReturn=");
        builder.append(String.valueOf(myNumberToReturn));
        builder.append(",numberToSkip=");
        builder.append(String.valueOf(myNumberToSkip));

        builder.append(",query=");
        myQuery.accept(visitor);

        if (myReturnFields != null) {
            builder.append(",returnFields=");
            myReturnFields.accept(visitor);
        }
        builder.append(")");
        return builder.toString();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to ensure the inserted documents are not too large in
     * aggregate.
     * </p>
     */
    @Override
    public void validateSize(final int maxDocumentSize)
            throws DocumentToLargeException {
        if (myMessageSize < 0) {
            long size = 0;
            if (myQuery != null) {
                size += myQuery.size();
            }
            if (myReturnFields != null) {
                size += myReturnFields.size();
            }

            myMessageSize = (int) size;
        }

        if (maxDocumentSize < myMessageSize) {
            throw new DocumentToLargeException(myMessageSize, maxDocumentSize,
                    myQuery);
        }
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
        final int flags = computeFlags();

        int size = HEADER_SIZE;
        size += 4; // flags;
        size += out.sizeOfCString(myDatabaseName, ".", myCollectionName);
        size += 4; // numberToSkip
        size += 4; // numberToReturn
        size += myQuery.size();
        if (myReturnFields != null) {
            size += myReturnFields.size();
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

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the query message.
     * </p>
     *
     * @see Message#write(int, BsonOutputStream)
     */
    @Override
    public void write(final int messageId, final BufferingBsonOutputStream out)
            throws IOException {
        final int flags = computeFlags();

        final long start = writeHeader(out, messageId, 0, Operation.QUERY);
        out.writeInt(flags);
        out.writeCString(myDatabaseName, ".", myCollectionName);
        out.writeInt(myNumberToSkip);
        out.writeInt(myNumberToReturn);
        out.writeDocument(myQuery);
        if (myReturnFields != null) {
            out.writeDocument(myReturnFields);
        }
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
        if (getReadPreference().isSecondaryOk()) {
            flags += REPLICA_OK_FLAG_BIT;
        }
        if (myTailable) {
            flags += TAILABLE_CURSOR_FLAG_BIT;
        }
        return flags;
    }
}
