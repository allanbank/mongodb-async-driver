/*
 * #%L
 * Update.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.BufferingBsonOutputStream;
import com.allanbank.mongodb.bson.io.SizeOfVisitor;
import com.allanbank.mongodb.bson.io.StringEncoder;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.error.DocumentToLargeException;

/**
 * Message to apply an <a href=
 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPUPDATE"
 * >update</a> to a document.
 * 
 * <pre>
 * <code>
 * struct OP_UPDATE {
 *     MsgHeader header;             // standard message header
 *     int32     ZERO;               // 0 - reserved for future use
 *     cstring   fullCollectionName; // "dbname.collectionname"
 *     int32     flags;              // bit vector. see below
 *     document  selector;           // the query to select the document
 *     document  update;             // specification of the update to perform
 * }
 * </code>
 * </pre>
 * 
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Update extends AbstractMessage {

    /** Flag bit for a multi-update. */
    public static final int MULTIUPDATE_FLAG_BIT = 0x02;

    /** Flag bit for an upsert. */
    public static final int UPSERT_FLAG_BIT = 0x01;

    /**
     * If true then all documents matching the query should be updated,
     * otherwise only the first document should be updated.
     */
    private final boolean myMultiUpdate;

    /** The query to select the document to update. */
    private final Document myQuery;

    /** The updates to apply to the selected document(s). */
    private final Document myUpdate;

    /**
     * If true then a document should be inserted if none match the query
     * criteria.
     */
    private final boolean myUpsert;

    /**
     * Creates a new Update.
     * 
     * @param in
     *            The input stream to read the update message from.
     * @throws IOException
     *             On an error reading the update message.
     */
    public Update(final BsonInputStream in) throws IOException {
        in.readInt(); // 0 - reserved.
        init(in.readCString());
        final int flags = in.readInt();
        myQuery = in.readDocument();
        myUpdate = in.readDocument();

        myUpsert = (flags & UPSERT_FLAG_BIT) == UPSERT_FLAG_BIT;
        myMultiUpdate = (flags & MULTIUPDATE_FLAG_BIT) == MULTIUPDATE_FLAG_BIT;
    }

    /**
     * Creates a new Update.
     * 
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection.
     * @param query
     *            The query to select the document to update.
     * @param update
     *            The updates to apply to the selected document(s).
     * @param multiUpdate
     *            If true then all documents matching the query should be
     *            updated, otherwise only the first document should be updated.
     * @param upsert
     *            If true then a document should be inserted if none match the
     *            query criteria.
     */
    public Update(final String databaseName, final String collectionName,
            final Document query, final Document update,
            final boolean multiUpdate, final boolean upsert) {
        super(databaseName, collectionName, ReadPreference.PRIMARY);
        myQuery = query;
        myUpdate = update;
        myMultiUpdate = multiUpdate;
        myUpsert = upsert;
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
            final Update other = (Update) object;

            result = super.equals(object)
                    && (myMultiUpdate == other.myMultiUpdate)
                    && (myUpsert == other.myUpsert)
                    && myUpdate.equals(other.myUpdate)
                    && myQuery.equals(other.myQuery);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the name of the operation: "UPDATE".
     * </p>
     */
    @Override
    public String getOperationName() {
        return Operation.UPDATE.name();
    }

    /**
     * Returns the query to select the document to update.
     * 
     * @return The query to select the document to update.
     */
    public Document getQuery() {
        return myQuery;
    }

    /**
     * Returns the updates to apply to the selected document(s).
     * 
     * @return The updates to apply to the selected document(s).
     */
    public Document getUpdate() {
        return myUpdate;
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
        result = (31 * result) + (myMultiUpdate ? 1 : 3);
        result = (31 * result) + (myUpsert ? 1 : 3);
        result = (31 * result) + myUpdate.hashCode();
        result = (31 * result) + myQuery.hashCode();
        return result;
    }

    /**
     * Returns true if all documents matching the query should be updated,
     * otherwise only the first document should be updated.
     * 
     * @return True if all documents matching the query should be updated,
     *         otherwise only the first document should be updated.
     */
    public boolean isMultiUpdate() {
        return myMultiUpdate;
    }

    /**
     * Returns true if the document should be inserted if none match the query
     * criteria.
     * 
     * @return True if the document should be inserted if none match the query
     *         criteria.
     */
    public boolean isUpsert() {
        return myUpsert;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the size of the {@link Query}.
     * </p>
     */
    @Override
    public int size() {

        int size = HEADER_SIZE + 10; // See below.
        // size += 4; // 0 - reserved.
        size += StringEncoder.utf8Size(myDatabaseName);
        // size += 1; // StringEncoder.utf8Size(".");
        size += StringEncoder.utf8Size(myCollectionName);
        // size += 1; // \0 on the CString.
        // size += 4; // flags
        size += myQuery.size();
        size += myUpdate.size();

        return size;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to ensure the inserted documents are not too large in
     * aggregate.
     * </p>
     */
    @Override
    public void validateSize(final SizeOfVisitor visitor,
            final int maxDocumentSize) throws DocumentToLargeException {
        final long querySize = (myQuery != null) ? myQuery.size() : 0;
        if (maxDocumentSize < querySize) {
            throw new DocumentToLargeException((int) querySize,
                    maxDocumentSize, myQuery);
        }
        final long updateSize = (myUpdate != null) ? myUpdate.size() : 0;
        if (maxDocumentSize < updateSize) {
            throw new DocumentToLargeException((int) updateSize,
                    maxDocumentSize, myUpdate);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the update message.
     * </p>
     * 
     * @see Message#write(int, BsonOutputStream)
     */
    @Override
    public void write(final int messageId, final BsonOutputStream out)
            throws IOException {

        final int flags = computeFlags();

        int size = HEADER_SIZE;
        size += 4; // 0 - reserved.
        size += out.sizeOfCString(myDatabaseName, ".", myCollectionName);
        size += 4; // flags
        size += out.sizeOf(myQuery); // Seeds the size list for later use.
        size += out.sizeOf(myUpdate); // Seeds the size list for later use.

        writeHeader(out, messageId, 0, Operation.UPDATE, size);
        out.writeInt(0);
        out.writeCString(myDatabaseName, ".", myCollectionName);
        out.writeInt(flags);
        out.writeDocument(myQuery);
        out.writeDocument(myUpdate);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the update message.
     * </p>
     * 
     * @see Message#write(int, BsonOutputStream)
     */
    @Override
    public void write(final int messageId, final BufferingBsonOutputStream out)
            throws IOException {

        final int flags = computeFlags();

        final long start = writeHeader(out, messageId, 0, Operation.UPDATE);
        out.writeInt(0);
        out.writeCString(myDatabaseName, ".", myCollectionName);
        out.writeInt(flags);
        out.writeDocument(myQuery);
        out.writeDocument(myUpdate);
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
        if (myMultiUpdate) {
            flags += MULTIUPDATE_FLAG_BIT;
        }
        if (myUpsert) {
            flags += UPSERT_FLAG_BIT;
        }
        return flags;
    }
}
