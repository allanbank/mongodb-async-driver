/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.json;

import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.DocumentReference;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.JsonSerializationVisitor;
import com.allanbank.mongodb.bson.element.LongElement;
import com.allanbank.mongodb.bson.element.MaxKeyElement;
import com.allanbank.mongodb.bson.element.MinKeyElement;
import com.allanbank.mongodb.bson.element.MongoTimestampElement;
import com.allanbank.mongodb.bson.element.NullElement;
import com.allanbank.mongodb.bson.element.ObjectIdElement;
import com.allanbank.mongodb.bson.element.RegularExpressionElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.SymbolElement;
import com.allanbank.mongodb.bson.element.TimestampElement;
import com.allanbank.mongodb.error.JsonException;
import com.allanbank.mongodb.error.JsonParseException;

/**
 * Json provides a simplified interface for parsing JSON documents into BSON
 * {@link Document}s and also serializing BSON {@link Document}s into JSON text.
 * <p>
 * Basic JSON types are parsed as follows:
 * <dl>
 * <dt>{@code true} or {@code false} token</dt>
 * <dd>Creates an {@link BooleanElement}. <br/>
 * <code>{ a : true }</code> or <code>{ a : false }</code></dd>
 * <dt>{@code null} token</dt>
 * <dd>Creates an {@link NullElement}. <br/>
 * <code>{ a : null }</code></dd>
 * <dt>Other Non-Quoted Strings</dt>
 * <dd>Creates a {@link SymbolElement}:<br/>
 * <code>{ a : b }</code></dd>
 * <dt>Quoted Strings (either single or double quotes)</dt>
 * <dd>Creates a {@link StringElement}:<br/>
 * <code>{ a : 'b' }</code> or <code>{ a : "b" }</code></dd>
 * <dt>Integers (Numbers without a {@code . } or exponent)</dt>
 * <dd>Creates an {@link IntegerElement} if within the range [
 * {@link Integer#MIN_VALUE}, {@link Integer#MAX_VALUE}], otherwise a
 * {@link LongElement}. Value is parsed by {@link Long#parseLong(String)}.<br/>
 * <code>{ a : 1234 }</code> or <code>{ a : 123456789012 }</code></dd>
 * <dt>Doubles (Numbers with a {@code . } or exponent)</dt>
 * <dd>Creates an {@link DoubleElement}. Value is parsed by
 * {@link Double#parseDouble(String)}.<br/>
 * <code>{ a : 1.2 }</code> or <code>{ a : 1e12 }</code></dd>
 * </p>
 * <p>
 * In addition to the basic JSON types the parser also supports the following
 * standard MongoDB/BSON extensions:
 * </p>
 * <dl>
 * <dt>BinData</dt>
 * <dd>Creates a {@link BinaryElement}. The first field is the sub-type,
 * normally zero. The second field is the base64 encoded binary value: <br/>
 * <code>{ a : BinData(0, "VVU=") }</code> or
 * <code>{ a : { $binary:"VVU=", $type:0 } }</code></dd>
 * <dt>HexData</dt>
 * <dd>Creates a {@link BinaryElement}. The first field is the sub-type,
 * normally zero. The second field is the hex encoded binary value: <br/>
 * <code>{ a : HexData(0, "cafe") }</code></dd>
 * <dt>ISODate</dt>
 * <dd>Creates a {@link TimestampElement}: <br/>
 * <code>{ a : ISODate("2012-07-14T01:00:00.000") }</code> or
 * <code>{ a : { $date : "2012-07-14T01:00:00.000" } }</code> or
 * <code>{ a : { $date : 1234567890 } }</code></dd>
 * <dt>MaxKey</dt>
 * <dd>Creates a {@link MaxKeyElement}: <br/>
 * <code>{ a : MaxKey }</code> or <code>{ a : MaxKey() }</code></dd>
 * <dt>MinKey</dt>
 * <dd>Creates a {@link MinKeyElement}: <br/>
 * <code>{ a : MinKey }</code> or <code>{ a : MinKey() }</code></dd>
 * <dt>NumberLong</dt>
 * <dd>Creates a {@link LongElement}: <br/>
 * <code>{ a : NumberLong("123456789") }</code></dd>
 * <dt>ObjectId</dt>
 * <dd>Creates an {@link ObjectIdElement}. The string is the hex encoding of the
 * 128 bit value: <br/>
 * <code>{ a : ObjectId("4e9d87aa5825b60b637815a6") }</code> or
 * <code>{ a : { $oid : "4e9d87aa5825b60b637815a6" } }</code></dd>
 * <dt>$regex</dt>
 * <dd>Creates an {@link RegularExpressionElement}: <br/>
 * <code>{ a : { $regex : 'cat' , $options : 'i' } }</code></dd>
 * <dt>Timestamp</dt>
 * <dd>Creates a {@link MongoTimestampElement}. The first value is the seconds
 * since the UNIX epoch. The second value is an ordinal: <br/>
 * <code>{ a : Timestamp(0,0) }</code> or
 * <code>{ a : { $timestamp : { t : 0, i : 0 } } }</code></dd>
 * </dl>
 * <p>
 * The following non-standard extensions are also provided. These extensions may
 * be deprecated in future releases if standard extensions are created:
 * </p>
 * <dl>
 * <dt>DBPointer</dt>
 * <dd>Creates a {@link com.allanbank.mongodb.bson.element.DBPointerElement
 * DBPointerElement}:<br/>
 * <code>{ a : DBPointer("db", 'collection', ObjectId("4e9d87aa5825b60b637815a6") ) }</code>
 * <br/>
 * <b>Note</b>: DBPointers are deprecated in favor of the
 * {@link DocumentReference DBRef} convention</dd>
 * </dl>
 * <p>
 * <b>Note</b>: Currently serialization/parsing round trip is not supported for
 * the following {@link Element} types:
 * <ul>
 * <li>{@link com.allanbank.mongodb.bson.element.JavaScriptElement
 * JavaScriptElement}</li>
 * <li>{@link com.allanbank.mongodb.bson.element.JavaScriptWithScopeElement
 * JavaScriptWithScopeElement}</li>
 * </ul>
 * </p>
 * 
 * @see <a
 *      href="http://docs.mongodb.org/manual/reference/mongodb-extended-json/">MongoDB
 *      Extended JSON</a>
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Json {

    /**
     * Parses the document from the reader into a BSON {@link Document}.
     * <p>
     * See the class documentation for important limitations on round trip
     * serialization and parsing.
     * </p>
     * 
     * @param input
     *            The source of the document to read.
     * @return The {@link Document} representation of the JSON document.
     * @throws JsonParseException
     *             On a failure to parse the JSON document.
     */
    public static Document parse(final Reader input) throws JsonParseException {
        final JsonParser parser = new JsonParser();

        try {
            final Object result = parser.parse(input);
            if (result instanceof Document) {
                return (Document) result;
            }

            throw new JsonParseException(
                    "Unknown type returned from the parsed document: " + result);
        }
        catch (final ParseException pe) {
            if (pe.currentToken != null) {
                throw new JsonParseException(pe.getMessage(), pe,
                        pe.currentToken.beginLine, pe.currentToken.beginColumn);
            }
            throw new JsonParseException(pe);
        }
        catch (final RuntimeException re) {
            throw new JsonParseException(re);
        }
    }

    /**
     * Parses the document from the reader into a BSON {@link Document}.
     * <p>
     * This method is equivalent to: <blockquote>
     * 
     * <pre>
     * <code>
     * parse(new StringReader(input));
     * </code>
     * </pre>
     * 
     * </blockquote>
     * </p>
     * <p>
     * See the class documentation for important limitations on round trip
     * serialization and parsing.
     * </p>
     * 
     * @param input
     *            The source of the document to read.
     * @return The {@link Document} representation of the JSON document.
     * @throws JsonParseException
     *             On a failure to parse the JSON document.
     */
    public static Document parse(final String input) throws JsonParseException {
        return parse(new StringReader(input));
    }

    /**
     * Serializes the {@link Document} to an equivalent JSON document.
     * <p>
     * See the class documentation for important limitations on round trip
     * serialization and parsing.
     * </p>
     * 
     * @param document
     *            The document to conver to a JSON document.
     * @return The JSON document text.
     * @throws JsonException
     *             On a failure to write the JSON document.
     */
    public static String serialize(final DocumentAssignable document)
            throws JsonException {
        final StringWriter writer = new StringWriter();

        serialize(document, writer);

        return writer.toString();
    }

    /**
     * Serializes the {@link Document} to an equivalent JSON document.
     * <p>
     * See the class documentation for important limitations on round trip
     * serialization and parsing.
     * </p>
     * 
     * @param document
     *            The document to conver to a JSON document.
     * @param sink
     *            The sink for the JSON document text.
     * @throws JsonException
     *             On a failure to write the JSON document.
     */
    public static void serialize(final DocumentAssignable document,
            final Writer sink) throws JsonException {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                sink, true);
        document.asDocument().accept(visitor);
    }

    /**
     * Creates a new Json onbject - hidden.
     */
    private Json() {
        super();
    }

}
