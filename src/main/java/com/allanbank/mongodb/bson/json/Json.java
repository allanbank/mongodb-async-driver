/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.json;

import java.io.Reader;
import java.io.StringReader;

import com.allanbank.mongodb.bson.Document;

/**
 * Json provides a simplified interface for parsing JSON documents into BSON
 * {@link Document}s.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Json {

    /**
     * Parses the document from the reader into a BSON {@link Document}.
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
     * Creates a new Json onbject - hidden.
     */
    private Json() {
        super();
    }

}
