/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Visitor;

/**
 * {@link BufferingBsonOutputStream} provides a class to write BSON documents
 * based on the <a href="http://bsonspec.org/">BSON specification</a>.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BufferingBsonOutputStream extends FilterOutputStream {
    /** The {@link Visitor} to write the BSON documents. */
    private final BufferingWriteVisitor myVisitor;

    /**
     * Creates a new {@link BufferingBsonOutputStream}.
     * 
     * @param output
     *            The stream to write to.
     */
    public BufferingBsonOutputStream(final OutputStream output) {
        super(output);

        myVisitor = new BufferingWriteVisitor();
    }

    /**
     * Creates a new {@link BufferingBsonOutputStream}.
     * 
     * @param output
     *            The stream to write to.
     */
    public BufferingBsonOutputStream(final RandomAccessOutputStream output) {
        super(output);

        myVisitor = new BufferingWriteVisitor(output);
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
