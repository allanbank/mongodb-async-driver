/*
 * #%L
 * KillCursors.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import java.util.Arrays;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.BufferingBsonOutputStream;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.error.DocumentToLargeException;

/**
 * Message to <a href=
 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPKILLCURSORS"
 * >killcursor</a>s that a client no longer needs.
 * 
 * <pre>
 * <code>
 * struct {
 *     MsgHeader header;            // standard message header
 *     int32     ZERO;              // 0 - reserved for future use
 *     int32     numberOfCursorIDs; // number of cursorIDs in message
 *     int64*    cursorIDs;         // sequence of cursorIDs to close
 * }
 * </code>
 * </pre>
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class KillCursors extends AbstractMessage {

    /** The ids of the cursors to be killed. */
    private final long[] myCursorIds;

    /**
     * Creates a new KillCursors.
     * 
     * @param in
     *            The stream to read the kill_cursors message from.
     * @throws IOException
     *             On a failure reading the kill_cursors message.
     */
    public KillCursors(final BsonInputStream in) throws IOException {
        init(".");

        in.readInt(); // 0 - reserved.
        final int numberOfCursors = in.readInt();
        myCursorIds = new long[numberOfCursors];
        for (int i = 0; i < numberOfCursors; ++i) {
            myCursorIds[i] = in.readLong();
        }
    }

    /**
     * Creates a new KillCursors.
     * 
     * @param cursorIds
     *            The ids of the cursors to kill.
     * @param readPreference
     *            The preferences for which server to send the request.
     */
    public KillCursors(final long[] cursorIds,
            final ReadPreference readPreference) {
        super("", "", readPreference);
        myCursorIds = Arrays.copyOf(cursorIds, cursorIds.length);
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
            final KillCursors other = (KillCursors) object;

            // Base class fields are always the same ""."".
            result = Arrays.equals(myCursorIds, other.myCursorIds);
        }
        return result;
    }

    /**
     * Returns the ids of the cursors to be killed.
     * 
     * @return The ids of the cursors to be killed.
     */
    public long[] getCursorIds() {
        return Arrays.copyOf(myCursorIds, myCursorIds.length);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the name of the operation: "KILL_CURSORS".
     * </p>
     */
    @Override
    public String getOperationName() {
        return Operation.KILL_CURSORS.name();
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
        result = (31 * result) + Arrays.hashCode(myCursorIds);
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the size of the {@link KillCursors}.
     * </p>
     */
    @Override
    public int size() {

        int size = HEADER_SIZE + 8; // See below.
        // size += 4; // 0 - reserved
        // size += 4; // number of cursors.
        size += (8 * myCursorIds.length);

        return size;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a string form of the message.
     * </p>
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        builder.append("KillCursors(cursorIds=");
        builder.append(Arrays.toString(myCursorIds));
        builder.append(")");

        return builder.toString();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to ensure the size of the cursors ids array is not too large.
     * </p>
     */
    @Override
    public void validateSize(final int maxDocumentSize)
            throws DocumentToLargeException {
        if (maxDocumentSize < (myCursorIds.length * 8)) {
            throw new DocumentToLargeException((myCursorIds.length * 8),
                    maxDocumentSize, null);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the kill_cursors message.
     * </p>
     * 
     * @see Message#write(int, BsonOutputStream)
     */
    @Override
    public void write(final int messageId, final BsonOutputStream out)
            throws IOException {
        int size = HEADER_SIZE;
        size += 4; // 0 - reserved
        size += 4; // number of cursors.
        size += (8 * myCursorIds.length);

        writeHeader(out, messageId, 0, Operation.KILL_CURSORS, size);
        out.writeInt(0);
        out.writeInt(myCursorIds.length);
        for (final long myCursorId : myCursorIds) {
            out.writeLong(myCursorId);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the kill_cursors message.
     * </p>
     * 
     * @see Message#write(int, BsonOutputStream)
     */
    @Override
    public void write(final int messageId, final BufferingBsonOutputStream out)
            throws IOException {
        final long start = writeHeader(out, messageId, 0,
                Operation.KILL_CURSORS);
        out.writeInt(0);
        out.writeInt(myCursorIds.length);
        for (final long myCursorId : myCursorIds) {
            out.writeLong(myCursorId);
        }
        finishHeader(out, start);

        out.flushBuffer();
    }
}
