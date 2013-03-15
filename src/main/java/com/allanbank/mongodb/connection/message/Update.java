/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.message;

import java.io.IOException;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.SizeOfVisitor;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.Operation;
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
     * The size of the message. If negative then the size has not been computed.
     */
    private int myMessageSize;

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

        myMessageSize = -1;
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
        myMessageSize = -1;
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
     * Overridden to ensure the inserted documents are not too large in
     * aggregate.
     * </p>
     */
    @Override
    public void validateSize(final SizeOfVisitor visitor,
            final int maxDocumentSize) throws DocumentToLargeException {
        if (myMessageSize < 0) {
            visitor.reset();

            if (myQuery != null) {
                myQuery.accept(visitor);
            }
            if (myUpdate != null) {
                myUpdate.accept(visitor);
            }

            myMessageSize = visitor.getSize();
        }

        if (maxDocumentSize < myMessageSize) {
            throw new DocumentToLargeException(myMessageSize, maxDocumentSize,
                    myUpdate);
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

        int flags = 0;
        if (myMultiUpdate) {
            flags += MULTIUPDATE_FLAG_BIT;
        }
        if (myUpsert) {
            flags += UPSERT_FLAG_BIT;
        }

        int size = HEADER_SIZE;
        size += 4; // 0 - reserved.
        size += out.sizeOfCString(myDatabaseName, ".", myCollectionName);
        size += 4; // flags
        size += out.sizeOf(myQuery);
        size += out.sizeOf(myUpdate);

        writeHeader(out, messageId, 0, Operation.UPDATE, size);
        out.writeInt(0);
        out.writeCString(myDatabaseName, ".", myCollectionName);
        out.writeInt(flags);
        out.writeDocument(myQuery);
        out.writeDocument(myUpdate);
    }
}
