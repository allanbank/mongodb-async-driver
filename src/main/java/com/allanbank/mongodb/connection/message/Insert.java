/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.Operation;

/**
 * Message to <a href=
 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPINSERT"
 * >insert</a> a set of documents into a collection.
 * 
 * <pre>
 * <code>
 * struct {
 *     MsgHeader header;             // standard message header
 *     int32     flags;              // bit vector - see below
 *     cstring   fullCollectionName; // "dbname.collectionname"
 *     document* documents;          // one or more documents to insert into the collection
 * }
 * </code>
 * </pre>
 * 
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Insert extends AbstractMessage {

    /** The flag bit to keep inserting documents on an error. */
    public static final int CONTINUE_ON_ERROR_BIT = 1;

    /**
     * If true, then the insert of documents should continue if one document
     * causes an error.
     */
    private final boolean myContinueOnError;

    /** The documents to be inserted. */
    private final List<Document> myDocuments;

    /**
     * Creates a new Insert.
     * 
     * @param header
     *            The header proceeding the insert message. This is used to
     *            locate the end of the insert.
     * @param in
     *            The stream to read the insert message from.
     * @throws IOException
     *             On a failure reading the insert message.
     */
    public Insert(final Header header, final BsonInputStream in)
            throws IOException {

        final long position = in.getBytesRead();
        final long end = (position + header.getLength()) - Header.SIZE;

        final int flags = in.readInt();
        init(in.readCString());

        // Read the documents to the end of the message.
        myDocuments = new ArrayList<Document>();
        while (in.getBytesRead() < end) {
            myDocuments.add(in.readDocument());
        }

        myContinueOnError = (flags & CONTINUE_ON_ERROR_BIT) == CONTINUE_ON_ERROR_BIT;
    }

    /**
     * Creates a new Insert.
     * 
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection.
     * @param documents
     *            The documents to be inserted.
     * @param continueOnError
     *            If the insert should continue if one of the documents causes
     *            an error.
     */
    public Insert(final String databaseName, final String collectionName,
            final List<Document> documents, final boolean continueOnError) {
        super(databaseName, collectionName, ReadPreference.PRIMARY);

        myDocuments = new ArrayList<Document>(documents);
        myContinueOnError = continueOnError;
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
            final Insert other = (Insert) object;

            result = super.equals(object)
                    && (myContinueOnError == other.myContinueOnError)
                    && myDocuments.equals(other.myDocuments);
        }
        return result;
    }

    /**
     * Returns the documents to insert.
     * 
     * @return The documents to insert.
     */
    public List<Document> getDocuments() {
        return Collections.unmodifiableList(myDocuments);
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
        result = (31 * result) + (myContinueOnError ? 1 : 3);
        result = (31 * result) + myDocuments.hashCode();
        return result;
    }

    /**
     * Returns true if the insert should continue with other documents if one of
     * the document inserts encounters an error.
     * 
     * @return True if the insert should continue with other documents if one of
     *         the document inserts encounters an error.
     */
    public boolean isContinueOnError() {
        return myContinueOnError;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the insert message.
     * </p>
     * 
     * @see Message#write(int, BsonOutputStream)
     */
    @Override
    public void write(final int messageId, final BsonOutputStream out)
            throws IOException {
        int flags = 0;
        if (myContinueOnError) {
            flags += CONTINUE_ON_ERROR_BIT;
        }

        int size = HEADER_SIZE;
        size += 4; // flags
        size += out.sizeOfCString(myDatabaseName, ".", myCollectionName);
        for (final Document document : myDocuments) {
            size += out.sizeOf(document);
        }

        writeHeader(out, messageId, 0, Operation.INSERT, size);
        out.writeInt(flags);
        out.writeCString(myDatabaseName, ".", myCollectionName);
        for (final Document document : myDocuments) {
            out.writeDocument(document);
        }
    }

}
