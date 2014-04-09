/*
 * Copyright 2011-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */
package com.allanbank.mongodb.client.message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.BufferingBsonOutputStream;
import com.allanbank.mongodb.bson.io.SizeOfVisitor;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.error.DocumentToLargeException;

/**
 * Message received from the database in <a href=
 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPREPLY"
 * >reply</a> to a query.
 * 
 * <pre>
 * <code>
 * struct {
 *     MsgHeader header;         // standard message header
 *     int32     responseFlags;  // bit vector - see details below
 *     int64     cursorID;       // cursor id if client needs to do get more's
 *     int32     startingFrom;   // where in the cursor this reply is starting
 *     int32     numberReturned; // number of documents in the reply
 *     document* documents;      // documents
 * }
 * </code>
 * </pre>
 * 
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Reply extends AbstractMessage {
    /** Bit for the await capable flag. */
    public static final int AWAIT_CAPABLE_BIT = 8;

    /** Bit for the cursor not found flag. */
    public static final int CURSOR_NOT_FOUND_BIT = 1;

    /** Bit for the query failure flag. */
    public static final int QUERY_FAILURE_BIT = 2;

    /** Bit for the shard configuration stale flag. */
    public static final int SHARD_CONFIG_STALE_BIT = 4;

    /** Indicates the server is await capable for tailable cursors. */
    private final boolean myAwaitCapable;

    /**
     * The id of the cursor if the user needs to do a get_more to get the
     * complete results.
     */
    private final long myCursorId;

    /** Indicates that the cursor in the <tt>getmore</tt> command was not found. */
    private final boolean myCursorNotFound;

    /** The offset (index) of the first document returned from the cursor. */
    private final int myCursorOffset;

    /** Indicates that the query failed. */
    private final boolean myQueryFailed;

    /** The id of the request this response is for. */
    private final int myResponseToId;

    /** The returned documents. */
    private final List<Document> myResults;

    /** Indicates (to a MongoS?) that its shard configuration is stale. */
    private final boolean myShardConfigStale;

    /**
     * Creates a new Reply.
     * 
     * @param header
     *            The header from the reply message.
     * @param in
     *            Stream to read the reply message from.
     * @throws IOException
     *             On a failure to read the reply.
     */
    public Reply(final Header header, final BsonInputStream in)
            throws IOException {
        init(".");

        myResponseToId = header.getResponseId();

        final int flags = in.readInt();
        myCursorId = in.readLong();
        myCursorOffset = in.readInt();

        final int docCount = in.readInt();
        myResults = new ArrayList<Document>(docCount);
        for (int i = 0; i < docCount; ++i) {
            myResults.add(in.readDocument());
        }

        myAwaitCapable = (flags & AWAIT_CAPABLE_BIT) == AWAIT_CAPABLE_BIT;
        myCursorNotFound = (flags & CURSOR_NOT_FOUND_BIT) == CURSOR_NOT_FOUND_BIT;
        myQueryFailed = (flags & QUERY_FAILURE_BIT) == QUERY_FAILURE_BIT;
        myShardConfigStale = (flags & SHARD_CONFIG_STALE_BIT) == SHARD_CONFIG_STALE_BIT;
    }

    /**
     * Creates a new Reply.
     * 
     * @param responseToId
     *            The id of the request this response is for.
     * @param cursorId
     *            The id of the cursor if the user needs to do a get_more to get
     *            the complete results.
     * @param cursorOffset
     *            The offset (index) of the first document returned from the
     *            cursor.
     * @param results
     *            The returned documents.
     * @param awaitCapable
     *            If true, indicates the server is await capable for tailable
     *            cursors.
     * @param cursorNotFound
     *            If true, indicates that the cursor in the <tt>get_more</tt>
     *            message was not found.
     * @param queryFailed
     *            If true, indicates that the query failed.
     * @param shardConfigStale
     *            If true, indicates (to a MongoS?) that its shard configuration
     *            is stale.
     * 
     */
    public Reply(final int responseToId, final long cursorId,
            final int cursorOffset, final List<Document> results,
            final boolean awaitCapable, final boolean cursorNotFound,
            final boolean queryFailed, final boolean shardConfigStale) {
        super("", "", ReadPreference.PRIMARY);

        myResponseToId = responseToId;
        myCursorId = cursorId;
        myCursorOffset = cursorOffset;
        myResults = new ArrayList<Document>(results);
        myAwaitCapable = awaitCapable;
        myCursorNotFound = cursorNotFound;
        myQueryFailed = queryFailed;
        myShardConfigStale = shardConfigStale;
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
            final Reply other = (Reply) object;

            // Base class fields are always the same ""."".
            result = (myAwaitCapable == other.myAwaitCapable)
                    && (myCursorNotFound == other.myCursorNotFound)
                    && (myQueryFailed == other.myQueryFailed)
                    && (myShardConfigStale == other.myShardConfigStale)
                    && (myResponseToId == other.myResponseToId)
                    && (myCursorOffset == other.myCursorOffset)
                    && (myCursorId == other.myCursorId)
                    && myResults.equals(other.myResults);
        }
        return result;
    }

    /**
     * Returns the id of the cursor if the user needs to do a get_more to get
     * the complete results.
     * 
     * @return The id of the cursor if the user needs to do a get_more to get
     *         the complete results.
     */
    public long getCursorId() {
        return myCursorId;
    }

    /**
     * Returns the offset (index) of the first document returned from the
     * cursor.
     * 
     * @return The offset (index) of the first document returned from the
     *         cursor.
     */
    public int getCursorOffset() {
        return myCursorOffset;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the name of the operation: "REPLY".
     * </p>
     */
    @Override
    public String getOperationName() {
        return Operation.REPLY.name();
    }

    /**
     * Returns the id of the request this response is for.
     * 
     * @return The id of the request this response is for.
     */
    public int getResponseToId() {
        return myResponseToId;
    }

    /**
     * Returns the query results.
     * 
     * @return The query results.
     */
    public List<Document> getResults() {
        return myResults;
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
        result = (31 * result) + (myAwaitCapable ? 1 : 3);
        result = (31 * result) + (myCursorNotFound ? 1 : 3);
        result = (31 * result) + (myQueryFailed ? 1 : 3);
        result = (31 * result) + (myShardConfigStale ? 1 : 3);
        result = (31 * result) + myResponseToId;
        result = (31 * result) + myCursorOffset;
        result = (31 * result) + (int) (myCursorId >> Integer.SIZE);
        result = (31 * result) + (int) myCursorId;
        result = (31 * result) + myResults.hashCode();
        return result;
    }

    /**
     * Returns true if the server is await capable for tailable cursors.
     * 
     * @return True if the server is await capable for tailable cursors.
     */
    public boolean isAwaitCapable() {
        return myAwaitCapable;
    }

    /**
     * Returns true if the cursor in the <tt>get_more</tt> message was not
     * found.
     * 
     * @return True if the cursor in the <tt>get_more</tt> message was not
     *         found.
     */
    public boolean isCursorNotFound() {
        return myCursorNotFound;
    }

    /**
     * Returns true if the query failed.
     * 
     * @return True if the query failed.
     */
    public boolean isQueryFailed() {
        return myQueryFailed;
    }

    /**
     * Returns true if the shard configuration is stale.
     * 
     * @return True if the shard configuration is stale.
     */
    public boolean isShardConfigStale() {
        return myShardConfigStale;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overrridden to be a no-op since we normally only receive a reply and
     * don't care about the size.
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
     * Overridden to write the reply message.
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
        size += 8; // cursorId
        size += 4; // cursorOffset
        size += 4; // result count.
        for (final Document result : myResults) {
            size += out.sizeOf(result);
        }

        writeHeader(out, messageId, myResponseToId, Operation.REPLY, size);
        out.writeInt(flags);
        out.writeLong(myCursorId);
        out.writeInt(myCursorOffset);
        out.writeInt(myResults.size());
        for (final Document result : myResults) {
            out.writeDocument(result);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the reply message.
     * </p>
     * 
     * @see Message#write(int, BsonOutputStream)
     */
    @Override
    public void write(final int messageId, final BufferingBsonOutputStream out)
            throws IOException {
        final int flags = computeFlags();

        final long start = writeHeader(out, messageId, myResponseToId,
                Operation.REPLY);
        out.writeInt(flags);
        out.writeLong(myCursorId);
        out.writeInt(myCursorOffset);
        out.writeInt(myResults.size());
        for (final Document result : myResults) {
            out.writeDocument(result);
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
        if (myAwaitCapable) {
            flags += AWAIT_CAPABLE_BIT;
        }
        if (myCursorNotFound) {
            flags += CURSOR_NOT_FOUND_BIT;
        }
        if (myQueryFailed) {
            flags += QUERY_FAILURE_BIT;
        }
        if (myShardConfigStale) {
            flags += SHARD_CONFIG_STALE_BIT;
        }
        return flags;
    }

}
