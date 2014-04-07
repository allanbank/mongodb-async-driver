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
     * Determines if the passed object is of this same type as this object and
     * if so that its fields are equal.
     *
     * @param object
     *            The object to compare to.
     *
     * @see Object#equals(Object)
     */
    @Override
    public boolean equals(final Object object) {
        boolean result = false;
        if (this == object) {
            result = true;
        }
        else if ((object != null) && (getClass() == object.getClass())) {
            final InsertOperation other = (InsertOperation) object;

            result = myDocument.equals(other.myDocument);
        }
        return result;
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

    /**
     * Computes a reasonable hash code.
     *
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        return myDocument.hashCode();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to returns a representation of the update.
     * </p>
     */
    @Override
    public String toString() {
        return "Insert[" + myDocument + "]";
    }
}
