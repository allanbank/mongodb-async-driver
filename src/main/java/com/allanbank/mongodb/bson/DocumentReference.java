/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson;

import static com.allanbank.mongodb.util.Assertions.assertNotNull;

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
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DocumentReference implements DocumentAssignable {

    /** The name for the collection name field. */
    public static final String COLLECTION_FIELD_NAME = "$ref";

    /** The name for the database name field. */
    public static final String DATABASE_FIELD_NAME = "$db";

    /** The name for the id field. */
    public static final String ID_FIELD_NAME = "$id";

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
        myId = id;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overriden to return a DBRef style document. This is the reference as a
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
     * Overriden to compare the {@code object} to this {@link DocumentReference}
     * .
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
     * Overriden to compute a reasonable hash for this {@link DocumentReference}
     * .
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
     * Overriden to return a JSON like representation of the
     * {@link DocumentReference}.
     * </p>
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder("{ ");

        builder.append(COLLECTION_FIELD_NAME);
        builder.append(": \"");
        builder.append(myCollectionName);
        builder.append("\", ");

        builder.append(myId.withName(ID_FIELD_NAME));

        if (myDatabaseName != null) {
            builder.append(", ");
            builder.append(DATABASE_FIELD_NAME);
            builder.append(": \"");
            builder.append(myDatabaseName);
            builder.append('"');
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
