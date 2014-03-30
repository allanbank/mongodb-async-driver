/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder.write;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;

/**
 * UpdateOperation provides a container for the fields in an update request.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class UpdateOperation implements WriteOperation {

    /** Serialization version for the class. */
    private static final long serialVersionUID = 4657279430768594366L;

    /** If true then the update can modify multiple documents. */
    private final boolean myMultiUpdate;

    /** The query to find the documents to update. */
    private final Document myQuery;

    /** The update specification. */
    private final Document myUpdate;

    /** If true then the document should be inserted if not found. */
    private final boolean myUpsert;

    /**
     * Creates a new UpdateOperation.
     * 
     * @param query
     *            The query to find the documents to update.
     * @param update
     *            The update specification.
     * @param multiUpdate
     *            If true then the update is applied to all of the matching
     *            documents, otherwise only the first document found is updated.
     * @param upsert
     *            If true then if no document is found then a new document is
     *            created and updated, otherwise no operation is performed.
     */
    public UpdateOperation(final DocumentAssignable query,
            final DocumentAssignable update, final boolean multiUpdate,
            final boolean upsert) {
        myQuery = query.asDocument();
        myUpdate = update.asDocument();
        myUpsert = upsert;
        myMultiUpdate = multiUpdate;
    }

    /**
     * Returns the query to find the documents to update.
     * 
     * @return The query to find the documents to update.
     */
    public Document getQuery() {
        return myQuery;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return {@link WriteOperationType#UPDATE}.
     * </p>
     */
    @Override
    public final WriteOperationType getType() {
        return WriteOperationType.UPDATE;
    }

    /**
     * Returns the update specification.
     * 
     * @return The update specification.
     */
    public Document getUpdate() {
        return myUpdate;
    }

    /**
     * Returns true if the update can modify multiple documents.
     * 
     * @return True if the update can modify multiple documents.
     */
    public boolean isMultiUpdate() {
        return myMultiUpdate;
    }

    /**
     * Returns true if the document should be inserted if not found.
     * 
     * @return True if the document should be inserted if not found.
     */
    public boolean isUpsert() {
        return myUpsert;
    }
}
