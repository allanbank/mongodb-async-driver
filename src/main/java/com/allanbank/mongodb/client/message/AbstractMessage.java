/*
 * #%L
 * AbstractMessage.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.io.StringWriter;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.BufferingBsonOutputStream;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.client.VersionRange;

/**
 * Base class for a MongoDB message.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractMessage
implements Message {

    /** The size of a message header. */
    public static final int HEADER_SIZE = Header.SIZE;

    /**
     * Determines the {@link ReadPreference} to be used based on the command's
     * {@code ReadPreference} or the collection's if the command's
     * {@code ReadPreference} is <code>null</code>. Updates the command's
     * {@link DocumentBuilder} with the {@code ReadPreference} details if
     * connected to a sharded cluster and the resulting {@code ReadPreference}
     * is not supported by the legacy settings.
     *
     * @param builder
     *            The builder for the command document to augment with the read
     *            preferences if connected to a sharded cluster.
     * @param readPreference
     *            The read preferences to add to the command.
     */
    protected static void updateReadPreference(final DocumentBuilder builder,
            final ReadPreference readPreference) {

        if (!readPreference.isLegacy()) {
            final Document query = builder.asDocument();
            builder.reset();
            builder.add("$query", query);
            builder.add(ReadPreference.FIELD_NAME, readPreference);
        }
    }

    /** The name of the collection to operate on. */
    protected String myCollectionName;

    /** The name of the database to operate on. */
    protected String myDatabaseName;

    /** The details on which servers may be sent the message. */
    private final ReadPreference myReadPreference;

    /** The required version of the server to support processing the message. */
    private final VersionRange myRequiredServerVersionRange;

    /**
     * Create a new AbstractMessage.
     */
    public AbstractMessage() {
        myDatabaseName = "";
        myCollectionName = "";
        myReadPreference = ReadPreference.PRIMARY;
        myRequiredServerVersionRange = null;
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
        myRequiredServerVersionRange = null;
    }

    /**
     * Creates a new AbstractMessage.
     *
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection.
     * @param readPreference
     *            The preferences for which servers to send the message.
     * @param versionRange
     *            The required range of versions of the server to support
     *            processing the message.
     */
    public AbstractMessage(final String databaseName,
            final String collectionName, final ReadPreference readPreference,
            final VersionRange versionRange) {
        myDatabaseName = databaseName;
        myCollectionName = collectionName;
        myReadPreference = readPreference;
        myRequiredServerVersionRange = versionRange;
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
    @Override
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
     * {@inheritDoc}
     */
    @Override
    public VersionRange getRequiredVersionRange() {
        return myRequiredServerVersionRange;
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
     * {@inheritDoc}
     * <p>
     * Overridden to return {@code this}.
     * </p>
     *
     * @see Message#transformFor
     */
    @Override
    public Message transformFor(final Version serverVersion) {
        return this;
    }

    /**
     * Helper method to emit the field into the {@link StringWriter}.
     *
     * @param builder
     *            The {@link StringWriter} to append to.
     * @param value
     *            The value for the boolean.
     * @param label
     *            The label for the value.
     */
    protected void emit(final StringWriter builder, final boolean value,
            final String label) {
        if (!value) {
            builder.append("!");
        }
        builder.append(label);
        builder.append(",");
    }

    /**
     * Writes the MsgHeader messageLengthField in the header <tt>stream</tt>.
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
     * @param start
     *            The position of the start of the header for the message.
     */
    protected void finishHeader(final BufferingBsonOutputStream stream,
            final long start) {

        final long end = stream.getPosition();

        stream.writeIntAt(start, (int) (end - start));
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
     * @return The position of the start of the header for the message.
     */
    protected long writeHeader(final BufferingBsonOutputStream stream,
            final int requestId, final int responseTo, final Operation op) {

        final long start = stream.getPosition();

        stream.writeInt(0);
        stream.writeInt(requestId);
        stream.writeInt(responseTo);
        stream.writeInt(op.getCode());

        return start;
    }

}