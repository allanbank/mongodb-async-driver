/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.message;

import java.io.IOException;
import java.util.Arrays;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.Operation;

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
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
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
        size += 8 * myCursorIds.length;

        writeHeader(out, messageId, 0, Operation.KILL_CURSORS, size);
        out.writeInt(0);
        out.writeInt(myCursorIds.length);
        for (final long myCursorId : myCursorIds) {
            out.writeLong(myCursorId);
        }
    }
}
