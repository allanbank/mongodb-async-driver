/*
 * Copyright 2011-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */
package com.allanbank.mongodb.client.message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.BufferingBsonOutputStream;
import com.allanbank.mongodb.bson.io.SizeOfVisitor;
import com.allanbank.mongodb.bson.io.StringEncoder;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.client.VersionRange;
import com.allanbank.mongodb.error.DocumentToLargeException;

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
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
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
     * The documents to be inserted. If negative then the size has not been
     * computed.
     */
    private int myDocumentsSize;

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
        myDocumentsSize = -1;
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
        this(databaseName, collectionName, documents, continueOnError, null);
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
     * @param requiredServerVersion
     *            The required version of the server to support processing the
     *            message.
     */
    public Insert(final String databaseName, final String collectionName,
            final List<Document> documents, final boolean continueOnError,
            final VersionRange requiredServerVersion) {
        super(databaseName, collectionName, ReadPreference.PRIMARY,
                requiredServerVersion);

        myDocuments = new ArrayList<Document>(documents);
        myContinueOnError = continueOnError;
        myDocumentsSize = -1;
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
     * {@inheritDoc}
     * <p>
     * Overridden to return the name of the operation: "INSERT".
     * </p>
     */
    @Override
    public String getOperationName() {
        return Operation.INSERT.name();
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
     * Overridden to return the size of the {@link Insert}.
     * </p>
     */
    @Override
    public int size() {

        int size = HEADER_SIZE + 6; // See below.
        // size += 4; // flags
        size += StringEncoder.utf8Size(myDatabaseName);
        // size += 1; // StringEncoder.utf8Size(".");
        size += StringEncoder.utf8Size(myCollectionName);
        // size += 1; // \0 on the CString.
        for (final Document document : myDocuments) {
            size += document.size();
        }

        return size;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to output the documents and insert flags.
     * </p>
     */
    @Override
    public String toString() {
        return "Insert [myContinueOnError=" + myContinueOnError
                + ", myDocuments=" + myDocuments + "]";
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
        if (myDocumentsSize < 0) {
            visitor.reset();

            for (final Document doc : myDocuments) {
                doc.accept(visitor);
            }

            myDocumentsSize = visitor.getSize();
        }

        if (maxDocumentSize < myDocumentsSize) {
            throw new DocumentToLargeException(myDocumentsSize,
                    maxDocumentSize, myDocuments.get(0));
        }
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
        final int flags = computeFlags();

        int size = HEADER_SIZE;
        size += 4; // flags
        size += out.sizeOfCString(myDatabaseName, ".", myCollectionName);
        for (final Document document : myDocuments) {
            size += out.sizeOf(document); // Seeds length list for later use.
        }

        writeHeader(out, messageId, 0, Operation.INSERT, size);
        out.writeInt(flags);
        out.writeCString(myDatabaseName, ".", myCollectionName);
        for (final Document document : myDocuments) {
            out.writeDocument(document);
        }
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
    public void write(final int messageId, final BufferingBsonOutputStream out)
            throws IOException {
        final int flags = computeFlags();

        final long start = writeHeader(out, messageId, 0, Operation.INSERT);
        out.writeInt(flags);
        out.writeCString(myDatabaseName, ".", myCollectionName);
        for (final Document document : myDocuments) {
            out.writeDocument(document);
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
        if (myContinueOnError) {
            flags += CONTINUE_ON_ERROR_BIT;
        }
        return flags;
    }

}
