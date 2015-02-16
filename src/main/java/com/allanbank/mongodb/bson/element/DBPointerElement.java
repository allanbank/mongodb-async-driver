/*
 * #%L
 * DBPointerElement.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.bson.element;

import static com.allanbank.mongodb.util.Assertions.assertNotNull;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.bson.DocumentReference;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.io.StringEncoder;

/**
 * A wrapper for a deprecated BSON DB Pointer element.
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @deprecated See BSON Specification.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Deprecated
@Immutable
@ThreadSafe
public class DBPointerElement
        extends AbstractElement {

    /**
     * The {@link DBPointerElement}'s class to avoid the
     * {@link Class#forName(String) Class.forName(...)} overhead.
     */
    public static final Class<DBPointerElement> DB_POINTER_CLASS = DBPointerElement.class;

    /** The BSON type for a Object Id. */
    public static final ElementType TYPE = ElementType.DB_POINTER;

    /** Serialization version for the class. */
    private static final long serialVersionUID = 2736569317385382748L;

    /**
     * Computes and returns the number of bytes that are used to encode the
     * element.
     *
     * @param name
     *            The name for the BSON Object Id.
     * @param dbName
     *            The database name.
     * @param collectionName
     *            The name of the collection.
     * @return The size of the element when encoded in bytes.
     */
    private static long computeSize(final String name, final String dbName,
            final String collectionName) {
        long result = 20; // type (1) + name null byte (1) + ns length (4) + ns
        // "." (1) + ns null byte (1) + ObjectId length (12) .
        result += StringEncoder.utf8Size(name);
        result += StringEncoder.utf8Size(dbName);
        result += StringEncoder.utf8Size(collectionName);

        return result;
    }

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
        this(name, dbName, collectionName, id, computeSize(name, dbName,
                collectionName));
    }

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
     * @param size
     *            The size of the element when encoded in bytes. If not known
     *            then use the
     *            {@link DBPointerElement#DBPointerElement(String, String, String, ObjectId)}
     *            constructor instead.
     * @throws IllegalArgumentException
     *             If the {@code name}, {@code dbName}, {@code collectionName},
     *             or {@code id} is <code>null</code>.
     */
    public DBPointerElement(final String name, final String dbName,
            final String collectionName, final ObjectId id, final long size) {
        super(name, size);

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
     * {@inheritDoc}
     * <p>
     * Overridden to compare the (database, collection, id) if the base class
     * comparison is equals.
     * </p>
     */
    @Override
    public int compareTo(final Element otherElement) {
        int result = super.compareTo(otherElement);

        if (result == 0) {
            final DBPointerElement other = (DBPointerElement) otherElement;

            result = myDatabaseName.compareTo(other.myDatabaseName);
            if (result == 0) {
                result = myCollectionName.compareTo(other.myCollectionName);
                if (result == 0) {
                    result = myId.compareTo(other.myId);
                }
            }
        }

        return result;
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
     * {@inheritDoc}
     * <p>
     * Returns a {@link DocumentReference}.
     * </p>
     * <p>
     * <b>Note:</b> This value will not be recreated is a Object-->Element
     * conversion. A more generic sub-document following the DBRef convention is
     * created instead.
     * </p>
     */
    @Override
    public DocumentReference getValueAsObject() {
        return new DocumentReference(myDatabaseName, myCollectionName,
                new ObjectIdElement("_id", myId));
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
     * {@inheritDoc}
     * <p>
     * Returns a new {@link DBPointerElement}.
     * </p>
     */
    @Override
    public DBPointerElement withName(final String name) {
        if (getName().equals(name)) {
            return this;
        }
        return new DBPointerElement(name, myDatabaseName, myCollectionName,
                myId);
    }
}
