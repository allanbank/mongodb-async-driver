/*
 * #%L
 * JsonSerializationVisitor.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Pattern;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.error.JsonException;
import com.allanbank.mongodb.util.IOUtils;

/**
 * JsonSerializationVisitor provides a BSON Visitor that generates a JSON
 * document.
 *
 * @see <a
 *      href="http://docs.mongodb.org/manual/reference/mongodb-extended-json/">MongoDB
 *      Extended JSON</a>
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Conublic sulting, Inc., All Rights Reserved
 */
public class JsonSerializationVisitor
        implements Visitor {

    /** The platforms new line string. */
    public static final String NL = System.getProperty("line.separator", "\n");

    /** A pattern to detect valid "symbol" names. */
    public static final Pattern SYMBOL_PATTERN = Pattern
            .compile("\\p{Alpha}\\p{Alnum}*");

    /** The default time zone. */
    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    /** The current indent level. */
    private int myIndentLevel = 0;

    /** If true then the visitor will write the document to 1 line. */
    private final boolean myOneLine;

    /** The Writer to write to. */
    private final Writer mySink;

    /**
     * If true then the visitor will write strict JSON using the strict mode for
     * BSON's types.
     *
     * @see <a
     *      href="http://docs.mongodb.org/manual/reference/mongodb-extended-json/">MongoDB
     *      Extended JSON</a>
     */
    private final boolean myStrict;

    /**
     * If true then the names of the elements should be suppressed because we
     * are in an array.
     */
    private boolean mySuppressNames = false;

    /**
     * Creates a new JsonSerializationVisitor.
     *
     * @param sink
     *            The Writer to write to.
     * @param oneLine
     *            If true then the visitor will write the document to 1 line,
     *            otherwise the visitor will write the document accross multiple
     *            lines with indenting.
     */
    public JsonSerializationVisitor(final Writer sink, final boolean oneLine) {
        this(sink, oneLine, false);
    }

    /**
     * Creates a new JsonSerializationVisitor.
     *
     * @param sink
     *            The Writer to write to.
     * @param oneLine
     *            If true then the visitor will write the document to 1 line,
     *            otherwise the visitor will write the document accross multiple
     *            lines with indenting.
     * @param strict
     *            If true then the visitor will write strict JSON using the
     *            strict format for BSON's types.
     * @see <a
     *      href="http://docs.mongodb.org/manual/reference/mongodb-extended-json/">MongoDB
     *      Extended JSON</a>
     */
    public JsonSerializationVisitor(final Writer sink, final boolean oneLine,
            final boolean strict) {
        mySink = sink;
        myOneLine = oneLine;
        myStrict = strict;
        myIndentLevel = 0;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to create a JSON representation of the document's elements to
     * the writer provided when this object was created.
     * </p>
     */
    @Override
    public void visit(final List<Element> elements) {
        try {
            if (elements.isEmpty()) {
                mySink.write("{}");
            }
            else if ((elements.size() == 1)
                    && !(elements.get(0) instanceof DocumentElement)
                    && !(elements.get(0) instanceof ArrayElement)) {
                mySink.write("{ ");

                final boolean oldSuppress = mySuppressNames;
                mySuppressNames = false;

                elements.get(0).accept(this);

                mySuppressNames = oldSuppress;
                mySink.write(" }");
            }
            else {
                mySink.write('{');
                myIndentLevel += 1;
                final boolean oldSuppress = mySuppressNames;
                mySuppressNames = false;

                boolean first = true;
                for (final Element element : elements) {
                    if (!first) {
                        mySink.write(",");
                    }
                    nl();
                    element.accept(this);
                    first = false;
                }

                mySuppressNames = oldSuppress;
                myIndentLevel -= 1;
                nl();
                mySink.write('}');
            }
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the array's elements to the
     * writer provided when this object was created.
     * </p>
     */
    @Override
    public void visitArray(final String name, final List<Element> elements) {
        try {
            writeName(name);
            if (elements.isEmpty()) {
                mySink.write("[]");
            }
            else if ((elements.size() == 1)
                    && !(elements.get(0) instanceof DocumentElement)
                    && !(elements.get(0) instanceof ArrayElement)) {
                mySink.write("[ ");
                final boolean oldSuppress = mySuppressNames;
                mySuppressNames = true;

                elements.get(0).accept(this);

                mySuppressNames = oldSuppress;
                mySink.write(" ]");
            }
            else {
                mySink.write("[");
                myIndentLevel += 1;
                final boolean oldSuppress = mySuppressNames;
                mySuppressNames = true;

                boolean first = true;
                for (final Element element : elements) {
                    if (!first) {
                        mySink.write(", ");
                    }
                    nl();
                    element.accept(this);
                    first = false;
                }

                mySuppressNames = oldSuppress;
                myIndentLevel -= 1;
                nl();
                mySink.append(']');
            }
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the binary element to the
     * writer provided when this object was created. This method generates the
     * MongoDB standard BinData(...) JSON extension.
     * </p>
     */
    @Override
    public void visitBinary(final String name, final byte subType,
            final byte[] data) {
        try {
            writeName(name);
            if (myStrict) {
                mySink.write("{ ");

                writeInnerName("$binary");
                mySink.write('"');
                mySink.write(IOUtils.toBase64(data));
                mySink.write("\", ");

                writeInnerName("$type");
                mySink.write('"');
                mySink.write(Integer.toHexString(subType));
                mySink.write("\" }");
            }
            else {
                mySink.write("BinData( ");
                mySink.write(Integer.toString(subType));
                mySink.write(", '");
                mySink.write(IOUtils.toBase64(data));
                mySink.write("' )");
            }
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the boolean element to the
     * writer provided when this object was created.
     * </p>
     */
    @Override
    public void visitBoolean(final String name, final boolean value) {
        try {
            writeName(name);
            mySink.write(Boolean.toString(value));
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the DBPointer element to
     * the writer provided when this object was created. This method generates
     * the non-standard DBPointer(...) JSON extension.
     * </p>
     */
    @Override
    public void visitDBPointer(final String name, final String databaseName,
            final String collectionName, final ObjectId id) {
        try {
            writeName(name);
            if (myStrict) {
                mySink.write("{ ");
                writeInnerName("$db");
                writeQuotedString(databaseName);
                mySink.write(", ");
                writeInnerName("$collection");
                writeQuotedString(collectionName);
                mySink.write(", ");
                writeInnerName("$id");
                writeObjectId(id);
                mySink.write(" }");
            }
            else {
                mySink.write("DBPointer( ");
                writeQuotedString(databaseName);
                mySink.write(", ");
                writeQuotedString(collectionName);
                mySink.write(", ");
                writeObjectId(id);
                mySink.write(" )");
            }
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the sub-document element to
     * the writer provided when this object was created.
     * </p>
     */
    @Override
    public void visitDocument(final String name, final List<Element> elements) {
        try {
            writeName(name);
            visit(elements);
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the double element to the
     * writer provided when this object was created.
     * </p>
     */
    @Override
    public void visitDouble(final String name, final double value) {
        try {
            writeName(name);
            mySink.write(Double.toString(value));
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the integer element to the
     * writer provided when this object was created.
     * </p>
     */
    @Override
    public void visitInteger(final String name, final int value) {
        try {
            writeName(name);
            mySink.write(Integer.toString(value));
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the JavaScript element to
     * the writer provided when this object was created. This method writes the
     * elements as a <code>{ $code : &lt;code&gt; }</code> sub-document.
     * </p>
     */
    @Override
    public void visitJavaScript(final String name, final String code) {
        try {
            writeName(name);
            mySink.write("{ $code : ");
            writeQuotedString(code);
            mySink.write(" }");
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the JavaScript element to
     * the writer provided when this object was created. This method writes the
     * elements as a
     * <code>{ $code : &lt;code&gt;, $scope : &lt;scope&gt; }</code>
     * sub-document.
     * </p>
     */
    @Override
    public void visitJavaScript(final String name, final String code,
            final Document scope) {
        try {
            writeName(name);
            mySink.write("{ $code : ");
            writeQuotedString(code);
            mySink.write(", $scope : ");
            scope.accept(this);
            mySink.write(" }");
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the binary element to the
     * writer provided when this object was created. This method generates the
     * MongoDB standard NumberLong(...) JSON extension.
     * </p>
     */
    @Override
    public void visitLong(final String name, final long value) {
        try {
            writeName(name);
            if (myStrict) {
                mySink.write("{ ");
                writeInnerName("$numberLong");
                writeQuotedString(Long.toString(value));
                mySink.write(" }");
            }
            else {
                mySink.write("NumberLong('");
                mySink.write(Long.toString(value));
                mySink.write("')");
            }
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the DBPointer element to
     * the writer provided when this object was created. This method generates
     * the non-standard MaxKey() JSON extension.
     * </p>
     */
    @Override
    public void visitMaxKey(final String name) {
        try {
            writeName(name);
            if (myStrict) {
                mySink.write("{ \"$maxKey\" : 1 }");
            }
            else {
                mySink.write("MaxKey()");
            }
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the DBPointer element to
     * the writer provided when this object was created. This method generates
     * the non-standard MinKey() JSON extension.
     * </p>
     */
    @Override
    public void visitMinKey(final String name) {
        try {
            writeName(name);
            if (myStrict) {
                mySink.write("{ \"$minKey\" : 1 }");
            }
            else {
                mySink.write("MinKey()");
            }
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the binary element to the
     * writer provided when this object was created. This method generates the
     * MongoDB standard Timestamp(...) JSON extension.
     * </p>
     */
    @Override
    public void visitMongoTimestamp(final String name, final long value) {
        try {
            final long time = (value >> Integer.SIZE) & 0xFFFFFFFFL;
            final long increment = value & 0xFFFFFFFFL;

            writeName(name);
            if (myStrict) {
                mySink.write("{ ");

                writeInnerName("$timestamp");

                mySink.write("{ ");
                writeInnerName("t");
                mySink.write(Long.toString(time * 1000));
                mySink.write(", ");
                writeInnerName("i");
                mySink.write(Long.toString(increment));
                mySink.write(" } }");
            }
            else {
                mySink.write("Timestamp(");
                mySink.write(Long.toString(time * 1000));
                mySink.write(", ");
                mySink.write(Long.toString(increment));
                mySink.write(')');
            }
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the null element to the
     * writer provided when this object was created.
     * </p>
     */
    @Override
    public void visitNull(final String name) {
        try {
            writeName(name);
            mySink.write("null");
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the binary element to the
     * writer provided when this object was created. This method generates the
     * MongoDB standard ObjectId(...) JSON extension.
     * </p>
     */
    @Override
    public void visitObjectId(final String name, final ObjectId id) {
        try {
            writeName(name);
            writeObjectId(id);
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the JavaScript element to
     * the writer provided when this object was created. This method writes the
     * elements as a
     * <code>{ $regex : &lt;pattern&gt;, $options : &lt;options&gt; }</code>
     * sub-document.
     * </p>
     */
    @Override
    public void visitRegularExpression(final String name, final String pattern,
            final String options) {
        try {
            writeName(name);
            mySink.write("{ ");
            writeInnerName("$regex");
            writeQuotedString(pattern);
            if (!options.isEmpty()) {
                mySink.write(", ");
                writeInnerName("$options");
                writeQuotedString(options);
            }
            mySink.write(" }");
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the string element to the
     * writer provided when this object was created.
     * </p>
     */
    @Override
    public void visitString(final String name, final String value) {
        try {
            writeName(name);
            writeQuotedString(value);
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the symbol element to the
     * writer provided when this object was created.
     * </p>
     */
    @Override
    public void visitSymbol(final String name, final String symbol) {
        try {
            writeName(name);
            if (SYMBOL_PATTERN.matcher(symbol).matches()) {
                mySink.write(symbol);
            }
            else {
                writeQuotedString(symbol);
            }
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append a JSON representation of the binary element to the
     * writer provided when this object was created. This method generates the
     * MongoDB standard ISODate(...) JSON extension.
     * </p>
     */
    @Override
    public void visitTimestamp(final String name, final long timestamp) {
        final SimpleDateFormat sdf = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        sdf.setTimeZone(UTC);

        try {
            writeName(name);
            if (myStrict) {
                mySink.write("{ ");

                writeInnerName("$date");
                mySink.write('"');
                mySink.write(sdf.format(new Date(timestamp)));
                mySink.write("\" }");
            }
            else {
                mySink.write("ISODate('");
                mySink.write(sdf.format(new Date(timestamp)));
                mySink.write("')");
            }
            mySink.flush();
        }
        catch (final IOException ioe) {
            throw new JsonException(ioe);
        }

    }

    /**
     * Returns if the visitor is currently suppressing the names of elements.
     * This is true when serializing an array.
     *
     * @return If the visitor is currently suppressing the names of elements.
     *         This is true when serializing an array.
     */
    protected boolean isSuppressNames() {
        return mySuppressNames;
    }

    /**
     * Writes a new line if {@link #myOneLine} is false and indents to the
     * {@link #myIndentLevel}.
     *
     * @throws IOException
     *             On a failure to write the new line.
     */
    protected void nl() throws IOException {
        if (!myOneLine) {
            mySink.write(NL);
            for (int i = 0; i < myIndentLevel; ++i) {
                mySink.write("  ");
            }
        }
        else {
            mySink.write(' ');
        }
    }

    /**
     * Sets the value for if the visitor is currently suppressing the names of
     * elements. This is true, for example, when serializing an array.
     *
     * @param suppressNames
     *            The new value for if names should be suppressed.
     */
    protected void setSuppressNames(final boolean suppressNames) {
        mySuppressNames = suppressNames;
    }

    /**
     * Writes the name if {@link #mySuppressNames} is false.
     *
     * @param name
     *            The name to write, if not suppressed.
     * @throws IOException
     *             On a failure to write the new line.
     */
    protected void writeInnerName(final String name) throws IOException {
        if (myStrict) {
            writeQuotedString(name);
        }
        else {
            if (SYMBOL_PATTERN.matcher(name).matches()) {
                mySink.write(name);
            }
            else {
                writeQuotedString(name);
            }
        }
        mySink.write(" : ");
    }

    /**
     * Writes the name if {@link #mySuppressNames} is false.
     *
     * @param name
     *            The name to write, if not suppressed.
     * @throws IOException
     *             On a failure to write the new line.
     */
    protected void writeName(final String name) throws IOException {
        if (!mySuppressNames) {
            writeInnerName(name);
        }
    }

    /**
     * Writes the {@link ObjectId}.
     *
     * @param id
     *            The {@link ObjectId} to write.
     * @throws IOException
     *             On a failure writing to the sink.
     */
    protected void writeObjectId(final ObjectId id) throws IOException {

        final String hexId = id.toHexString();

        if (myStrict) {
            mySink.write("{ ");
            writeInnerName("$oid");
            writeQuotedString(hexId);
            mySink.write(" }");
        }
        else {
            mySink.write("ObjectId(");
            writeQuotedString(hexId);
            mySink.write(")");
        }
    }

    /**
     * Writes the {@code string} as a quoted string.
     *
     * @param string
     *            The String to write.
     * @throws IOException
     *             On a failure writing the String.
     */
    protected void writeQuotedString(final String string) throws IOException {
        if (myStrict) {
            mySink.write('"');
            // Escape any embedded single quotes.
            mySink.write(string.replaceAll("\"", "\\\\\""));
            mySink.write('"');
        }
        else if (string.indexOf('\'') < 0) {
            mySink.write('\'');
            mySink.write(string);
            mySink.write('\'');
        }
        else if (string.indexOf('"') < 0) {
            mySink.write('"');
            mySink.write(string);
            mySink.write('"');
        }
        else {
            mySink.write('\'');
            // Escape any embedded single quotes.
            mySink.write(string.replaceAll("'", "\\\\'"));
            mySink.write('\'');
        }
    }
}
