/*
 * #%L
 * DocumentReference.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.bson;

import static com.allanbank.mongodb.util.Assertions.assertNotNull;

import java.io.Serializable;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * DocumentReference provides a standard MongoDB reference to a document within
 * a collection. This is commonly referred to as a DBRef.
 * <p>
 * A DocumentReference contains:
 * <ol>
 * <li>The name of the collection where the referenced document resides:
 * {@code $ref}.</li>
 * <li>The value of the _id field in the referenced document: {@code $id}.</li>
 * <li>The name of the database where the referenced document resides:
 * {@code $db} (Optional).</li>
 * </ol>
 *
 * @see <a
 *      href="http://docs.mongodb.org/manual/applications/database-references/#dbref">MongoDB
 *      DBRef Information</a>
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class DocumentReference
        implements DocumentAssignable, Serializable {

    /** The name for the collection name field. */
    public static final String COLLECTION_FIELD_NAME = "$ref";

    /** The name for the database name field. */
    public static final String DATABASE_FIELD_NAME = "$db";

    /** The name for the id field. */
    public static final String ID_FIELD_NAME = "$id";

    /** The serialization version for the class. */
    private static final long serialVersionUID = 7597767390422754639L;

    /** The name of the collection being referenced. */
    private final String myCollectionName;

    /** The name of the database being referenced. */
    private final String myDatabaseName;

    /** The id of the document being referenced. */
    private final Element myId;

    /**
     * Creates a new DocumentReference.
     *
     * @param collectionName
     *            The name of the collection being referenced.
     * @param id
     *            The id of the document being referenced. The name of the
     *            element is ignored within the {@link DocumentReference}.
     * @throws IllegalArgumentException
     *             If the {@code collectionName} or {@code id} are
     *             <code>null</code>.
     */
    public DocumentReference(final String collectionName, final Element id)
            throws IllegalArgumentException {
        this(null, collectionName, id);
    }

    /**
     * Creates a new DocumentReference.
     *
     * @param databaseName
     *            The name of the database being referenced.
     * @param collectionName
     *            The name of the collection being referenced.
     * @param id
     *            The id of the document being referenced. The name of the
     *            element is ignored within the {@link DocumentReference}.
     * @throws IllegalArgumentException
     *             If the {@code collectionName} or {@code id} are
     *             <code>null</code>.
     */
    public DocumentReference(final String databaseName,
            final String collectionName, final Element id)
            throws IllegalArgumentException {

        assertNotNull(collectionName,
                "The collection name of a Document Reference (DBRef) cannot be null.");
        assertNotNull(id,
                "The id of a Document Reference (DBRef) cannot be null.");

        myDatabaseName = databaseName;
        myCollectionName = collectionName;
        myId = id.withName(ID_FIELD_NAME);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a DBRef style document. This is the reference as a
     * document <em>not</em> the referenced document.
     * </p>
     */
    @Override
    public Document asDocument() {
        final DocumentBuilder builder = BuilderFactory.start();

        builder.add(COLLECTION_FIELD_NAME, myCollectionName);
        builder.add(myId.withName(ID_FIELD_NAME));
        if (myDatabaseName != null) {
            builder.add(DATABASE_FIELD_NAME, myDatabaseName);
        }

        return builder.asDocument();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to compare the {@code object} to this
     * {@link DocumentReference} .
     * </p>
     */
    @Override
    public boolean equals(final Object object) {
        boolean result = false;
        if (this == object) {
            result = true;
        }
        else if ((object != null) && (getClass() == object.getClass())) {
            final DocumentReference other = (DocumentReference) object;

            result = myCollectionName.equals(other.myCollectionName)
                    && myId.withName(ID_FIELD_NAME).equals(
                            other.myId.withName(ID_FIELD_NAME))
                    && nullSafeEquals(myDatabaseName, other.myDatabaseName);
        }
        return result;
    }

    /**
     * Returns the name of the collection being referenced.
     *
     * @return The name of the collection being referenced.
     */
    public String getCollectionName() {
        return myCollectionName;
    }

    /**
     * Returns the name of the database being referenced. This may be
     * <code>null</code>.
     *
     * @return The name of the database being referenced.
     */
    public String getDatabaseName() {
        return myDatabaseName;
    }

    /**
     * Returns the id of the document being referenced.
     *
     * @return The id of the document being referenced.
     */
    public Element getId() {
        return myId;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to compute a reasonable hash for this
     * {@link DocumentReference}.
     * </p>
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + myCollectionName.hashCode();
        result = (31 * result) + myId.withName(ID_FIELD_NAME).hashCode();
        result = (31 * result)
                + ((myDatabaseName != null) ? myDatabaseName.hashCode() : 3);
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a JSON like representation of the
     * {@link DocumentReference}.
     * </p>
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder("{ '");

        builder.append(COLLECTION_FIELD_NAME);
        builder.append("' : '");
        builder.append(myCollectionName);
        builder.append("', ");

        builder.append(myId.withName(ID_FIELD_NAME));

        if (myDatabaseName != null) {
            builder.append(", '");
            builder.append(DATABASE_FIELD_NAME);
            builder.append("' : '");
            builder.append(myDatabaseName);
            builder.append("'");
        }

        builder.append(" }");

        return builder.toString();
    }

    /**
     * Does a null safe equals comparison.
     *
     * @param rhs
     *            The right-hand-side of the comparison.
     * @param lhs
     *            The left-hand-side of the comparison.
     * @return True if the rhs equals the lhs. Note: nullSafeEquals(null, null)
     *         returns true.
     */
    protected boolean nullSafeEquals(final Object rhs, final Object lhs) {
        return (rhs == lhs) || ((rhs != null) && rhs.equals(lhs));
    }
}
