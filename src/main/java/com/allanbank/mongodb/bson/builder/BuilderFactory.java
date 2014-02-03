/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.builder;

import java.math.BigInteger;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementAssignable;
import com.allanbank.mongodb.bson.builder.impl.ArrayBuilderImpl;
import com.allanbank.mongodb.bson.builder.impl.DocumentBuilderImpl;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.LongElement;
import com.allanbank.mongodb.bson.element.NullElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.ObjectIdElement;
import com.allanbank.mongodb.bson.element.RegularExpressionElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.TimestampElement;
import com.allanbank.mongodb.bson.element.UuidElement;
import com.allanbank.mongodb.bson.impl.RootDocument;

/**
 * Helper class for getting started with a builder. The use of this class is
 * encouraged to avoid direct references to the builder implementations.
 * <p>
 * Two modes are provided by this class. The first uses the static methods
 * {@link #d}, {@link #a}, and {@link #e} to create documents, arrays, and
 * elements of a document respectfully. This allows for the quick construction
 * of documents with a known structure. As an example consider the following
 * code: <blockquote>
 * 
 * <pre>
 * <code>
 * DocumentBuilder movie = d( 
 *    e( "title", "Gone with the Wind" ),
 *    e( "directors", a( "Victor Fleming", "George Cukor", "Sam Wood" ) ),
 *    e( "stars", a( "Clark Gable", "Vivien Leigh", "Thomas Mitchell" ))
 * );
 * </code>
 * </pre>
 * 
 * </blockquote>
 * </p>
 * <p>
 * The above code creates a document with the following JSON
 * structure:<blockquote>
 * 
 * <pre>
 * <code>
 * {
 *    "title" : "Gone with the Wind",
 *    "directors" : [ "Victor Fleming", "George Cukor", "Sam Wood" ],
 *    "stars" : [ "Clark Gable", "Vivien Leigh", "Thomas Mitchell" ]
 * }
 * </code>
 * </pre>
 * 
 * </blockquote>
 * </p>
 * <p>
 * The second model for creating documents is to use the {@link DocumentBuilder}
 * s and {@link ArrayBuilder}s directly to dynamically construct the documents.
 * To create a dynamic builders call one of the {@link #start()} methods. Here
 * is an example creating the equivalent document we saw above: <blockquote>
 * 
 * <pre>
 * <code>
 * DocumentBuilder movie = BuilderFactory.start();
 * movie.add("title", "Gone with the Wind");
 * 
 * ArrayBuilder directorsArray = movie.pushArray("directors");
 * directorsArray.add("Victor Fleming")
 *               .add("George Cukor")
 *               .add("Sam Wood");
 * 
 * ArrayBuilder starsArray = movie.pushArray("stars");
 * starsArray.add("Clark Gable")
 *           .add("Vivien Leigh")
 *           .add("Thomas Mitchell");
 * </code>
 * </pre>
 * 
 * </blockquote>
 * </p>
 * <p>
 * The choice between the static or dynamic builders is based on a user's
 * preference.
 * </p>
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BuilderFactory {

    /**
     * Creates an array element containing boolean elements.
     * 
     * @param values
     *            The boolean value.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final ArrayElement a(final Boolean... values)
            throws IllegalArgumentException {
        final ArrayBuilder subArray = BuilderFactory.startArray();
        for (final Boolean entry : values) {
            if (entry != null) {
                subArray.add(entry.booleanValue());
            }
            else {
                subArray.addNull();
            }
        }
        return new ArrayElement("", subArray.build());
    }

    /**
     * Creates an array element containing binary elements using sub-type zero
     * (the default).
     * <p>
     * Will return a {@link NullElement} if any {@code data} is
     * <code>null</code>.
     * </p>
     * 
     * @param datas
     *            The binary value.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final ArrayElement a(final byte[]... datas)
            throws IllegalArgumentException {
        final ArrayBuilder subArray = BuilderFactory.startArray();
        for (final Object entry : datas) {
            subArray.add(entry);
        }
        return new ArrayElement("", subArray.build());
    }

    /**
     * Creates an array element containing timestamp elements. The timestamp is
     * the number of milliseconds since the Unix epoch.
     * <p>
     * Will return a {@link NullElement} if any {@code timestamp} is
     * <code>null</code>.
     * </p>
     * 
     * @param timestamps
     *            The number of milliseconds since the Unix epoch.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final ArrayElement a(final Date... timestamps)
            throws IllegalArgumentException {
        final ArrayBuilder subArray = BuilderFactory.startArray();
        for (final Object entry : timestamps) {
            subArray.add(entry);
        }
        return new ArrayElement("", subArray.build());
    }

    /**
     * Creates an array element containing pre-constructed document elements.
     * <p>
     * Will return a {@link NullElement} if any {@code document} is
     * <code>null</code>.
     * </p>
     * 
     * @param documents
     *            The document to wrap.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final ArrayElement a(final DocumentAssignable... documents)
            throws IllegalArgumentException {
        final ArrayBuilder subArray = BuilderFactory.startArray();
        for (final Object entry : documents) {
            subArray.add(entry);
        }
        return new ArrayElement("", subArray.build());
    }

    /**
     * Creates an array element containing double elements.
     * 
     * @param values
     *            The double value.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final ArrayElement a(final Double... values)
            throws IllegalArgumentException {
        final ArrayBuilder subArray = BuilderFactory.startArray();
        for (final Double entry : values) {
            if (entry != null) {
                subArray.add(entry.doubleValue());
            }
            else {
                subArray.addNull();
            }
        }
        return new ArrayElement("", subArray.build());
    }

    /**
     * Creates an array element containing the re-created slements.
     * <p>
     * Will return a {@link NullElement} if any {@code element} is
     * <code>null</code>.
     * </p>
     * 
     * @param elements
     *            The element to add to wrap.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final ArrayElement a(final ElementAssignable... elements)
            throws IllegalArgumentException {
        final ArrayBuilder subArray = BuilderFactory.startArray();
        for (final Object entry : elements) {
            subArray.add(entry);
        }
        return new ArrayElement("", subArray.build());
    }

    /**
     * Creates an array element containing integer (32-bit signed) elements.
     * 
     * @param values
     *            The integer value.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final ArrayElement a(final Integer... values)
            throws IllegalArgumentException {
        final ArrayBuilder subArray = BuilderFactory.startArray();
        for (final Integer entry : values) {
            if (entry != null) {
                subArray.add(entry.intValue());
            }
            else {
                subArray.addNull();
            }
        }
        return new ArrayElement("", subArray.build());
    }

    /**
     * Creates an array element containing long (64-bit signed) elements.
     * 
     * @param values
     *            The long value.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final ArrayElement a(final Long... values)
            throws IllegalArgumentException {
        final ArrayBuilder subArray = BuilderFactory.startArray();
        for (final Long entry : values) {
            if (entry != null) {
                subArray.add(entry.longValue());
            }
            else {
                subArray.addNull();
            }
        }
        return new ArrayElement("", subArray.build());
    }

    /**
     * Creates an ArrayElement after trying to coerce the values into the best
     * possible element type. If the coercion fails then an
     * {@link IllegalArgumentException} is thrown.
     * 
     * @param values
     *            The Object values to coerce into an element.
     * @return The {@link ArrayElement} with the name {@code ""} and the
     *         provided values.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code> or the {@code value}
     *             cannot be coerced into an element type.
     */
    public static final ArrayElement a(final Object... values) {
        final ArrayBuilder subArray = BuilderFactory.startArray();
        for (final Object entry : values) {
            subArray.add(entry);
        }
        return new ArrayElement("", subArray.build());
    }

    /**
     * Creates an array element containing ObjectId elements.
     * <p>
     * Will return a {@link NullElement} if any {@code id} is <code>null</code>.
     * </p>
     * 
     * @param ids
     *            The ObjectId to wrap.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code id} is <code>null</code>.
     */
    public static final ArrayElement a(final ObjectId... ids)
            throws IllegalArgumentException {
        final ArrayBuilder subArray = BuilderFactory.startArray();
        for (final Object entry : ids) {
            subArray.add(entry);
        }
        return new ArrayElement("", subArray.build());
    }

    /**
     * Creates an array element containing regular expression elements.
     * <p>
     * Will return a {@link NullElement} if any {@code pattern} is
     * <code>null</code>.
     * </p>
     * 
     * @param patterns
     *            The pattern for the regular expression.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final ArrayElement a(final Pattern... patterns)
            throws IllegalArgumentException {
        final ArrayBuilder subArray = BuilderFactory.startArray();
        for (final Object entry : patterns) {
            subArray.add(entry);
        }
        return new ArrayElement("", subArray.build());
    }

    /**
     * Creates an array element containing string elements.
     * <p>
     * Will return a {@link NullElement} if any {@code value} is
     * <code>null</code>.
     * </p>
     * 
     * @param values
     *            The string value.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final ArrayElement a(final String... values)
            throws IllegalArgumentException {
        final ArrayBuilder subArray = BuilderFactory.startArray();
        for (final Object entry : values) {
            subArray.add(entry);
        }
        return new ArrayElement("", subArray.build());
    }

    /**
     * Create an array element containing (sub-type 4) {@link UUID} elements.
     * <p>
     * Will return a {@link NullElement} if the {@code uuid} is
     * <code>null</code>.
     * </p>
     * 
     * @param uuids
     *            The {@link UUID}s to wrap in an element.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final ArrayElement a(final UUID... uuids)
            throws IllegalArgumentException {
        final ArrayBuilder subArray = BuilderFactory.startArray();
        for (final Object entry : uuids) {
            subArray.add(entry);
        }
        return new ArrayElement("", subArray.build());
    }

    /**
     * Helper method for creating static document structures.
     * 
     * @param elements
     *            The elements of the document. The elements may be created
     *            using the {@link #e} methods.
     * @return The document builder seeded with the specified elements.
     */
    public static final DocumentBuilder d(final Element... elements) {
        return new DocumentBuilderImpl(new RootDocument(elements));
    }

    /**
     * Creates a boolean element.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The boolean value.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final BooleanElement e(final String name, final boolean value)
            throws IllegalArgumentException {
        return new BooleanElement(name, value);
    }

    /**
     * Creates a binary element using sub-type zero (the default).
     * <p>
     * Will return a {@link NullElement} if the {@code data} is
     * <code>null</code>.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param data
     *            The binary value.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final Element e(final String name, final byte[] data)
            throws IllegalArgumentException {
        if (data != null) {
            return new BinaryElement(name, data);
        }
        return new NullElement(name);
    }

    /**
     * Creates a timestamp element. The timestamp is the number of milliseconds
     * since the Unix epoch.
     * <p>
     * Will return a {@link NullElement} if the {@code timestamp} is
     * <code>null</code>.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param timestamp
     *            The number of milliseconds since the Unix epoch.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final Element e(final String name, final Date timestamp)
            throws IllegalArgumentException {
        if (timestamp != null) {
            return new TimestampElement(name, timestamp.getTime());
        }
        return new NullElement(name);
    }

    /**
     * Creates a pre-constructed document element.
     * <p>
     * Will return a {@link NullElement} if the {@code document} is
     * <code>null</code>.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param document
     *            The document to wrap.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final Element e(final String name,
            final DocumentAssignable document) throws IllegalArgumentException {
        if (document != null) {
            return new DocumentElement(name, document.asDocument());
        }
        return new NullElement(name);
    }

    /**
     * Creates a double element.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The double value.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final DoubleElement e(final String name, final double value)
            throws IllegalArgumentException {
        return new DoubleElement(name, value);
    }

    /**
     * Re-creates the Element with the name provided.
     * <p>
     * Will return a {@link NullElement} if the {@code element} is
     * <code>null</code>.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param element
     *            The element to add to wrap.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final Element e(final String name,
            final ElementAssignable element) throws IllegalArgumentException {
        if (element != null) {
            return element.asElement().withName(name);
        }
        return new NullElement(name);
    }

    /**
     * Creates a integer (32-bit signed) element.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The integer value.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final IntegerElement e(final String name, final int value)
            throws IllegalArgumentException {
        return new IntegerElement(name, value);
    }

    /**
     * Creates a long (64-bit signed) element.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The long value.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final LongElement e(final String name, final long value)
            throws IllegalArgumentException {
        return new LongElement(name, value);
    }

    /**
     * Creates an element after trying to coerce the value into the best
     * possible element type. If the coercion fails then an
     * {@link IllegalArgumentException} is thrown.
     * <p>
     * This method does type inspection which can be slow. It is generally much
     * faster to use the type specific {@link #e} methods of this class.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The Object value to coerce into an element.
     * @return The element with the name and value.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code> or the {@code value}
     *             cannot be coerced into an element type.
     */
    public static final Element e(final String name, final Object value) {
        if (value == null) {
            return new NullElement(name);
        }
        else if (value instanceof Boolean) {
            return new BooleanElement(name, ((Boolean) value).booleanValue());
        }
        else if ((value instanceof Long) || (value instanceof BigInteger)) {
            return new LongElement(name, ((Number) value).longValue());
        }
        else if ((value instanceof Double) || (value instanceof Float)) {
            return new DoubleElement(name, ((Number) value).doubleValue());
        }
        else if (value instanceof Number) {
            return new IntegerElement(name, ((Number) value).intValue());
        }
        else if (value instanceof byte[]) {
            return new BinaryElement(name, (byte[]) value);
        }
        else if (value instanceof ObjectId) {
            return new ObjectIdElement(name, (ObjectId) value);
        }
        else if (value instanceof Pattern) {
            return new RegularExpressionElement(name, (Pattern) value);
        }
        else if (value instanceof String) {
            return new StringElement(name, (String) value);
        }
        else if (value instanceof Date) {
            return new TimestampElement(name, ((Date) value).getTime());
        }
        else if (value instanceof Calendar) {
            return new TimestampElement(name, ((Calendar) value).getTime()
                    .getTime());
        }
        else if (value instanceof UUID) {
            return new UuidElement(name, (UUID) value);
        }
        else if (value instanceof DocumentAssignable) {
            return new DocumentElement(name,
                    ((DocumentAssignable) value).asDocument());
        }
        else if (value instanceof ElementAssignable) {
            return ((ElementAssignable) value).asElement().withName(name);
        }
        else if (value instanceof Map) {
            final DocumentBuilder subDoc = BuilderFactory.start();
            for (final Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                subDoc.add(entry.getKey().toString(), entry.getValue());
            }
            return new DocumentElement(name, subDoc.build());
        }
        else if (value instanceof Collection) {
            final ArrayBuilder subArray = BuilderFactory.startArray();
            for (final Object entry : (Collection<?>) value) {
                subArray.add(entry);
            }
            return new ArrayElement(name, subArray.build());
        }
        else if (value instanceof Object[]) {
            final ArrayBuilder subArray = BuilderFactory.startArray();
            for (final Object entry : (Object[]) value) {
                subArray.add(entry);
            }
            return new ArrayElement(name, subArray.build());
        }

        throw new IllegalArgumentException("Could not coerce the type '"
                + value.getClass().getName()
                + "' into a valid BSON element type.");
    }

    /**
     * Creates an ObjectId element.
     * <p>
     * Will return a {@link NullElement} if the {@code id} is <code>null</code>.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param id
     *            The ObjectId to wrap.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code id} is <code>null</code>.
     */
    public static final Element e(final String name, final ObjectId id)
            throws IllegalArgumentException {
        if (id != null) {
            return new ObjectIdElement(name, id);
        }
        return new NullElement(name);
    }

    /**
     * Creates a regular expression element.
     * <p>
     * Will return a {@link NullElement} if the {@code pattern} is
     * <code>null</code>.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param pattern
     *            The pattern for the regular expression.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final Element e(final String name, final Pattern pattern)
            throws IllegalArgumentException {
        if (pattern != null) {
            return new RegularExpressionElement(name, pattern);
        }
        return new NullElement(name);
    }

    /**
     * Creates a string element.
     * <p>
     * Will return a {@link NullElement} if the {@code value} is
     * <code>null</code>.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The string value.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final Element e(final String name, final String value)
            throws IllegalArgumentException {
        if (value != null) {
            return new StringElement(name, value);
        }
        return new NullElement(name);
    }

    /**
     * Create a (sub-type 4) {@link UUID} element.
     * <p>
     * Will return a {@link NullElement} if the {@code uuid} is
     * <code>null</code>.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param uuid
     *            The {@link UUID} to wrap in an element.
     * @return The wrapped element.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public static final Element e(final String name, final UUID uuid)
            throws IllegalArgumentException {
        if (uuid != null) {
            return new UuidElement(name, UuidElement.UUID_SUBTTYPE, uuid);
        }
        return new NullElement(name);
    }

    /**
     * Creates a new {@link DocumentBuilder}.
     * 
     * @return The root level document builder.
     */
    public static final DocumentBuilder start() {
        return new DocumentBuilderImpl();
    }

    /**
     * Creates a new {@link DocumentBuilder} to append more elements to an
     * existing document.
     * 
     * @param seedDocument
     *            The document to seed the builder with. The builder will
     *            contain the seed document elements plus any added/appended
     *            elements.
     * @return The root level document builder.
     */
    public static final DocumentBuilder start(
            final DocumentAssignable seedDocument) {
        return new DocumentBuilderImpl(seedDocument);
    }

    /**
     * Creates a new {@link DocumentBuilder} to append more elements to an
     * existing set of documents.
     * 
     * @param seedDocuments
     *            The documents to seed the builder with. The builder will
     *            contain the seed document elements plus any added/appended
     *            elements.
     * @return The root level document builder.
     */
    public static final DocumentBuilder start(
            final DocumentAssignable... seedDocuments) {
        final DocumentBuilderImpl builder = new DocumentBuilderImpl();
        for (final DocumentAssignable seedDocument : seedDocuments) {
            for (final Element element : seedDocument.asDocument()) {
                builder.remove(element.getName());
                builder.add(element);
            }
        }
        return builder;
    }

    /**
     * Creates a new {@link ArrayBuilder}.
     * 
     * @return The root level array builder.
     */
    public static final ArrayBuilder startArray() {
        return new ArrayBuilderImpl();
    }

    /**
     * Creates a new builder factory.
     */
    private BuilderFactory() {
        // Nothing to do.
    }
}
