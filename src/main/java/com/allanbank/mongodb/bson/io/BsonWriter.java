/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Visitor;

/**
 * {@link BsonWriter} provides a class to write BSON documents based on the <a
 * href="http://bsonspec.org/">BSON specification</a>.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BsonWriter extends FilterOutputStream {
    /** The {@link Visitor} to write the BSON documents. */
    private final WriteVisitor myVisitor;

    /**
     * Creates a new {@link BsonWriter}.
     * 
     * @param output
     *            The stream to write to.
     */
    public BsonWriter(final OutputStream output) {
        super(output);

        myVisitor = new WriteVisitor();
    }

    /**
     * Creates a new {@link BsonWriter}.
     * 
     * @param output
     *            The stream to write to.
     */
    public BsonWriter(final RandomAccessOutputStream output) {
        super(output);

        myVisitor = new WriteVisitor(output);
    }

    /**
     * Writes the Document in BSON format to the underlying stream.
     * 
     * @param doc
     *            The document to write.
     * @return The number of bytes written for the document.
     * @throws IOException
     *             On a failure to write to the underlying document.
     */
    public long write(final Document doc) throws IOException {

        doc.accept(myVisitor);

        final long position = myVisitor.getSize();

        if (out != myVisitor.getOutputBuffer()) {
            myVisitor.writeTo(out);
            myVisitor.reset();
        }

        return position;
    }
}
