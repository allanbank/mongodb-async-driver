/*
 * #%L
 * DeleteOperation.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
            final DeleteOperation other = (DeleteOperation) object;

            result = (mySingleDelete == other.mySingleDelete)
                    && myQuery.equals(other.myQuery);
        }
        return result;
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
     * Overridden to return the query for the delete.
     * </p>
     */
    @Override
    public Document getRoutingDocument() {
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
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + (mySingleDelete ? 31 : 11);
        result = (31 * result) + myQuery.hashCode();
        return result;
    }

    /**
     * Returns true if the operation should only delete at most one document.
     * 
     * @return True if the operation should only delete at most one document.
     */
    public boolean isSingleDelete() {
        return mySingleDelete;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to returns a representation of the delete.
     * </p>
     */
    @Override
    public String toString() {
        return "Delete[singleDelete=" + mySingleDelete + ",query=" + myQuery
                + "]";
    }
}
