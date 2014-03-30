/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder.write;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;

/**
 * DeleteOperation provides a container for the fields in a delete request.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DeleteOperation implements WriteOperation {

    /** Serialization version for the class. */
    private static final long serialVersionUID = 3493986989972041392L;

    /** The query to find the documents to delete. */
    private final Document myQuery;

    /** If true then the operation should only delete at most one document. */
    private final boolean mySingleDelete;

    /**
     * Creates a new DeleteOperation.
     * 
     * @param query
     *            The query to find the documents to delete.
     * @param singleDelete
     *            If true then only a single document will be deleted. If
     *            running in a sharded environment then this field must be false
     *            or the query must contain the shard key.
     */
    public DeleteOperation(final DocumentAssignable query,
            final boolean singleDelete) {
        myQuery = query.asDocument();
        mySingleDelete = singleDelete;
    }

    /**
     * Returns the query to find the documents to delete.
     * 
     * @return The query to find the documents to delete.
     */
    public Document getQuery() {
        return myQuery;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return {@link WriteOperationType#DELETE}.
     * </p>
     */
    @Override
    public final WriteOperationType getType() {
        return WriteOperationType.DELETE;
    }

    /**
     * Returns true if the operation should only delete at most one document.
     * 
     * @return True if the operation should only delete at most one document.
     */
    public boolean isSingleDelete() {
        return mySingleDelete;
    }
}
