/*
 * #%L
 * UpdateOperation.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
            final UpdateOperation other = (UpdateOperation) object;

            result = (myUpsert == other.myUpsert)
                    && (myMultiUpdate == other.myMultiUpdate)
                    && myQuery.equals(other.myQuery)
                    && myUpdate.equals(other.myUpdate);
        }
        return result;
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
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + (myUpsert ? 31 : 11);
        result = (31 * result) + (myMultiUpdate ? 31 : 11);
        result = (31 * result) + myQuery.hashCode();
        result = (31 * result) + myUpdate.hashCode();
        return result;
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

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to returns a representation of the update.
     * </p>
     */
    @Override
    public String toString() {
        return "Update[upsert=" + myUpsert + ",multi=" + myMultiUpdate
                + ",query=" + myQuery + ",update=" + myUpdate + "]";
    }
}
