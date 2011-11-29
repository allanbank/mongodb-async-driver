/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import java.io.IOException;
import java.io.OutputStream;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Visitor;

/**
 * {@link UnbufferedBsonWriter} provides a class to write BSON documents based
 * on the <a href="http://bsonspec.org/">BSON specification</a>.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class UnbufferedBsonWriter {
	/** The {@link Visitor} to write the BSON documents. */
	private final UnbufferedWriteVisitor myVisitor;

	/**
	 * Creates a new {@link UnbufferedBsonWriter}.
	 * 
	 * @param output
	 *            The stream to write to.
	 */
	public UnbufferedBsonWriter(final OutputStream output) {
		myVisitor = new UnbufferedWriteVisitor(output);
	}

	/**
	 * Creates a new {@link UnbufferedBsonWriter}.
	 * 
	 * @param output
	 *            The stream to write to.
	 */
	public UnbufferedBsonWriter(final BsonOutputStream output) {
		myVisitor = new UnbufferedWriteVisitor(output);
	}

	/**
	 * Writes the Document in BSON format to the underlying stream.
	 * 
	 * @param doc
	 *            The document to write.
	 * @throws IOException
	 *             On a failure to write to the underlying document.
	 */
	public void write(final Document doc) throws IOException {
		try {
			doc.accept(myVisitor);
			if (myVisitor.hasError()) {
				throw myVisitor.getError();
			}
		} finally {
			myVisitor.reset();
		}
	}

	/**
	 * Determines the size of the document written in BSON format. The
	 * {@link Document}'s size is cached for a subsequent
	 * {@link #write(Document)} operation.
	 * 
	 * @param doc
	 *            The document to determine the size of.
	 * @return The number of bytes require to Write the document.
	 */
	public int sizeOf(final Document doc) {
		myVisitor.reset();
		return myVisitor.sizeOf(doc);
	}
}
