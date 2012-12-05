/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a deprecated BSON DB Pointer element.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @deprecated See BSON Specification.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
@Deprecated
public class DBPointerElement extends AbstractElement {

    /** The BSON type for a Object Id. */
    public static final ElementType TYPE = ElementType.DB_POINTER;

    /** Serialization version for the class. */
    private static final long serialVersionUID = 2736569317385382748L;

    /** The name of the collection containing the document. */
    private final String myCollectionName;

    /** The name of the database containing the document. */
    private final String myDatabaseName;

    /** The id for the document. */
    private final ObjectId myId;

    /**
     * Constructs a new {@link DBPointerElement}.
     * 
     * @param name
     *            The name for the BSON Object Id.
     * @param dbName
     *            The database name.
     * @param collectionName
     *            The name of the collection.
     * @param id
     *            The object id.
     * @throws IllegalArgumentException
     *             If the {@code name}, {@code dbName}, {@code collectionName},
     *             or {@code id} is <code>null</code>.
     */
    public DBPointerElement(final String name, final String dbName,
            final String collectionName, final ObjectId id) {
        super(name);

        assertNotNull(dbName,
                "DBPointer element's database name cannot be null.");
        assertNotNull(collectionName,
                "DBPointer element's collection name cannot be null.");
        assertNotNull(id, "DBPointer element's object id cannot be null.");

        myDatabaseName = dbName;
        myCollectionName = collectionName;
        myId = id;

    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitDBPointer} method.
     * 
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitDBPointer(getName(), myDatabaseName, myCollectionName,
                myId);
    }

    /**
     * Determines if the passed object is of this same type as this object and
     * if so that its fields are equal.
     * 
     * @param object
     *            The object to compare to.
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object object) {
        boolean result = false;
        if (this == object) {
            result = true;
        }
        else if ((object != null) && (getClass() == object.getClass())) {
            final DBPointerElement other = (DBPointerElement) object;

            result = super.equals(object)
                    && myDatabaseName.equals(other.myDatabaseName)
                    && myCollectionName.equals(other.myCollectionName)
                    && myId.equals(other.myId);
        }
        return result;
    }

    /**
     * Returns the collectionName value.
     * 
     * @return The collectionName value.
     */
    public String getCollectionName() {
        return myCollectionName;
    }

    /**
     * Returns the databaseName value.
     * 
     * @return The databaseName value.
     */
    public String getDatabaseName() {
        return myDatabaseName;
    }

    /**
     * Returns the id value.
     * 
     * @return The id value.
     */
    public ObjectId getId() {
        return myId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return TYPE;
    }

    /**
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + super.hashCode();
        result = (31 * result) + myDatabaseName.hashCode();
        result = (31 * result) + myCollectionName.hashCode();
        result = (31 * result) + myId.hashCode();
        return result;
    }

    /**
     * String form of the object.
     * 
     * @return A human readable form of the object.
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        builder.append('"');
        builder.append(getName());
        builder.append("\" : DBPointer( \"");
        builder.append(myDatabaseName);
        builder.append('.');
        builder.append(myCollectionName);
        builder.append("\", ");
        builder.append(myId);
        builder.append(")");

        return builder.toString();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link DBPointerElement}.
     * </p>
     */
    @Override
    public DBPointerElement withName(final String name) {
        return new DBPointerElement(name, myDatabaseName, myCollectionName,
                myId);
    }
}
