/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder.write;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.impl.RootDocument;

/**
 * InsertOperation provides a container for the fields in an insert request.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class InsertOperation implements WriteOperation {

    /** Serialization version for the class. */
    private static final long serialVersionUID = -197787616022990034L;

    /** The document to insert. */
    private final Document myDocument;

    /**
     * Creates a new InsertOperation.
     * 
     * @param document
     *            The document to insert.
     */
    public InsertOperation(final DocumentAssignable document) {
        myDocument = document.asDocument();
        if (myDocument instanceof RootDocument) {
            ((RootDocument) myDocument).injectId();
        }
    }

    /**
     * Returns the document to insert.
     * 
     * @return The document to insert.
     */
    public Document getDocument() {
        return myDocument;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return {@link WriteOperationType#INSERT}.
     * </p>
     */
    @Override
    public final WriteOperationType getType() {
        return WriteOperationType.INSERT;
    }
}
