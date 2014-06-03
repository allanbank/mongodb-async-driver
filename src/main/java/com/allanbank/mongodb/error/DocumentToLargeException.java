/*
 * #%L
 * DocumentToLargeException.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.error;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;

/**
 * DocumentToLargeException is thrown to report that an attempt was made to
 * serialize a Document that is over the maximum size limit.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DocumentToLargeException extends MongoDbException {

    /** Serialization exception for the class. */
    private static final long serialVersionUID = 8235621460369360624L;

    /** The document that was too big. */
    private final Document myDocument;

    /** The maximum size for a document. */
    private final int myMaximumSize;

    /** The size of the document that violated the maximum size. */
    private final int mySize;

    /**
     * Creates a new DocumentToLargeException.
     * 
     * @param size
     *            The size of the document that violated the maximum size.
     * @param maximum
     *            The maximum size for a document.
     * @param document
     *            The document that was too big.
     */
    public DocumentToLargeException(final int size, final int maximum,
            final Document document) {
        super("Attempted to serialize a document of size " + size
                + " when current maximum is " + maximum + ".");

        mySize = size;
        myMaximumSize = maximum;
        myDocument = document;
    }

    /**
     * Returns the document that was too big.
     * 
     * @return The document that was too big.
     */
    public Document getDocument() {
        return myDocument;
    }

    /**
     * Returns the maximum size for a document.
     * 
     * @return The maximum size for a document.
     */
    public int getMaximumSize() {
        return myMaximumSize;
    }

    /**
     * Returns the size of the document that violated the maximum size.
     * 
     * @return The size of the document that violated the maximum size.
     */
    public int getSize() {
        return mySize;
    }
}
