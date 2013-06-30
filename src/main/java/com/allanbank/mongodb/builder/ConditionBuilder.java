/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.JavaScriptElement;
import com.allanbank.mongodb.bson.element.JavaScriptWithScopeElement;
import com.allanbank.mongodb.bson.element.LongElement;
import com.allanbank.mongodb.bson.element.MaxKeyElement;
import com.allanbank.mongodb.bson.element.MinKeyElement;
import com.allanbank.mongodb.bson.element.MongoTimestampElement;
import com.allanbank.mongodb.bson.element.NullElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.ObjectIdElement;
import com.allanbank.mongodb.bson.element.RegularExpressionElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.SymbolElement;
import com.allanbank.mongodb.bson.element.TimestampElement;
import com.allanbank.mongodb.bson.element.UuidElement;
import com.allanbank.mongodb.builder.expression.Constant;
import com.allanbank.mongodb.error.QueryFailedException;

/**
 * ConditionBuilder provides tracking for the condition of a single field within
 * a query.
 * <p>
 * Use the {@link QueryBuilder#where(String)} method to create a
 * {@link ConditionBuilder}.
 * </p>
 * 
 * @see QueryBuilder#whereField(String)
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ConditionBuilder implements DocumentAssignable {

    /** The equals element. */
    private Element myEqualsComparison;

    /** The name of the field to compare. */
    private final String myFieldName;

    /** The non-equal comparisons. */
    private final Map<Operator, Element> myOtherComparisons;

    /** The parent builder for the condition. */
    private final QueryBuilder myParent;

    /**
     * Creates a new ConditionBuilder.
     * <p>
     * This constructor is protected since generally users will use the
     * {@link QueryBuilder} class to create a condition builder.
     * </p>
     * 
     * @param fieldName
     *            The name for the field to compare.
     * @param parent
     *            The parent builder for this condition.
     */
    protected ConditionBuilder(final String fieldName, final QueryBuilder parent) {
        myFieldName = fieldName;
        myParent = parent;

        myOtherComparisons = new LinkedHashMap<Operator, Element>();
    }

    /**
     * Checks if the value is greater than the specified <tt>dateTime</tt>.
     * <p>
     * This is equivalent to {@link #greaterThanTimestamp(long)
     * greaterThanTimestamp(dateTime.getTime())}.
     * </p>
     * <p>
     * Only a single {@link #greaterThan(int) greaterThan(...)} comparison can
     * be used. Calling multiple {@link #greaterThan(byte[]) greaterThan(...)}
     * methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param dateTime
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     * @see #greaterThanTimestamp(long)
     */
    public ConditionBuilder after(final Date dateTime) {
        return greaterThanTimestamp(dateTime.getTime());
    }

    /**
     * Specify the values that must <em>all</em> be in the fields array.
     * <p>
     * Only a single {@link #all(ArrayBuilder)} comparison can be used. Calling
     * multiple <tt>all(...)</tt> methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param elements
     *            A builder for the values for the comparison. Any changes to
     *            the {@link ArrayBuilder} after this method is called are not
     *            reflected in the comparison.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder all(final ArrayBuilder elements) {
        return all(elements.build());
    }

    /**
     * Specify the values that must <em>all</em> be in the fields array.
     * <p>
     * Only a single {@link #all(Element[])} comparison can be used. Calling
     * multiple <tt>all(...)</tt> methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param values
     *            The values for the comparison.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder all(final Constant... values) {
        myEqualsComparison = null;

        final List<Element> elements = new ArrayList<Element>(values.length);
        for (int i = 0; i < values.length; ++i) {
            elements.add(values[i].toElement(ArrayElement.nameFor(i)));
        }
        myOtherComparisons.put(MiscellaneousOperator.ALL, new ArrayElement(
                MiscellaneousOperator.ALL.getToken(), elements));

        return this;
    }

    /**
     * Specify the values that must <em>all</em> be in the fields array.
     * <p>
     * Only a single {@link #all(Element[])} comparison can be used. Calling
     * multiple <tt>all(...)</tt> methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param elements
     *            The element values for the comparison.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder all(final Element... elements) {
        myEqualsComparison = null;
        myOtherComparisons.put(MiscellaneousOperator.ALL, new ArrayElement(
                MiscellaneousOperator.ALL.getToken(), elements));

        return this;
    }

    /**
     * Starts a logical conjunction with this condition builder. If the
     * <tt>fieldName</tt> is equal to this builder's {@link #getFieldName()
     * field name} then this builder will be returned. Otherwise a different
     * builder will be returned sharing the same parent {@link QueryBuilder}.
     * 
     * @param fieldName
     *            The name of the field to create a conjunction with.
     * @return The {@link ConditionBuilder} to use to construct the conjunction.
     */
    public ConditionBuilder and(final String fieldName) {
        return myParent.whereField(fieldName);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the result of {@link #build()}.
     * </p>
     * 
     * @see #build()
     */
    @Override
    public Document asDocument() {
        return build();
    }

    /**
     * Checks if the value is less than the specified <tt>dateTime</tt>.
     * <p>
     * This is equivalent to {@link #lessThanTimestamp(long)
     * lessThanTimestamp(dateTime.getTime())}.
     * </p>
     * <p>
     * Only a single {@link #lessThan(int) lessThan(...)} comparison can be
     * used. Calling multiple {@link #lessThan(byte[]) lessThan(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param dateTime
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     * @see #lessThanTimestamp(long)
     */
    public ConditionBuilder before(final Date dateTime) {
        return lessThanTimestamp(dateTime.getTime());
    }

    /**
     * Returns the results of building the parent {@link QueryBuilder}.
     * 
     * @return The results of building the parent {@link QueryBuilder}.
     * 
     * @see QueryBuilder#build()
     */
    public Document build() {
        return myParent.build();
    }

    /**
     * Query to match a single element in the array field.
     * <p>
     * Only a single {@link #elementMatches(DocumentAssignable)} comparison can
     * be used. Calling multiple <tt>elementMatches(...)</tt> methods overwrites
     * previous values. In addition any {@link #equals(boolean) equals(...)}
     * condition is removed since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param arrayElementQuery
     *            A builder for the query to match a sub element. Any changes to
     *            the {@link QueryBuilder} after this method is called are not
     *            reflected in the comparison.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder elementMatches(
            final DocumentAssignable arrayElementQuery) {
        myEqualsComparison = null;
        myOtherComparisons.put(
                MiscellaneousOperator.ELEMENT_MATCH,
                new DocumentElement(MiscellaneousOperator.ELEMENT_MATCH
                        .getToken(), arrayElementQuery.asDocument()));
        return this;
    }

    /**
     * Checks if the value equals the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equals(final boolean value) {
        myOtherComparisons.clear();
        myEqualsComparison = new BooleanElement(getFieldName(), value);
        return this;
    }

    /**
     * Checks if the value equals the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param subType
     *            The binary values subtype.
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equals(final byte subType, final byte[] value) {
        myOtherComparisons.clear();
        myEqualsComparison = new BinaryElement(getFieldName(), subType, value);
        return this;
    }

    /**
     * Checks if the value equals the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equals(final byte[] value) {
        myOtherComparisons.clear();
        myEqualsComparison = new BinaryElement(getFieldName(), value);
        return this;
    }

    /**
     * Checks if the value equals the specified <tt>dateTime</tt>.
     * <p>
     * This is equivalent to {@link #equalsTimestamp(long)
     * equalsTimestamp(dateTime.getTime())}.
     * </p>
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param dateTime
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     * @see #equalsTimestamp(long)
     */
    public ConditionBuilder equals(final Date dateTime) {
        return equalsTimestamp(dateTime.getTime());
    }

    /**
     * Checks if the value equals the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equals(final DocumentAssignable value) {
        myOtherComparisons.clear();
        myEqualsComparison = new DocumentElement(getFieldName(),
                value.asDocument());
        return this;
    }

    /**
     * Checks if the value equals the specified <tt>value</tt>.
     * <p>
     * <b>NOTE:</b> Queries for matching a double value that closely
     * approximates an integer value (e.g., 1.00) will compare equal to a stored
     * integer or long value.
     * </p>
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equals(final double value) {
        myOtherComparisons.clear();
        myEqualsComparison = new DoubleElement(getFieldName(), value);
        return this;
    }

    /**
     * Checks if the value equals the specified <tt>value</tt>.
     * <p>
     * <b>NOTE:</b> Queries for matching a integer value will compare equals to
     * an equivalent stored long or closely matching double value.
     * </p>
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equals(final int value) {
        myOtherComparisons.clear();
        myEqualsComparison = new IntegerElement(getFieldName(), value);
        return this;
    }

    /**
     * Checks if the value equals the specified <tt>value</tt>.
     * <p>
     * <b>NOTE:</b> Queries for matching a long value will compare equals to an
     * equivalent stored integer or closely matching double value.
     * </p>
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equals(final long value) {
        myOtherComparisons.clear();
        myEqualsComparison = new LongElement(getFieldName(), value);
        return this;
    }

    /**
     * Checks if the value equals the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equals(final ObjectId value) {
        myOtherComparisons.clear();
        myEqualsComparison = new ObjectIdElement(getFieldName(), value);
        return this;
    }

    /**
     * Checks if the value equals the specified <tt>value</tt>.
     * <p>
     * <b>NOTE:</b> This checks if the value <b>is</b> a regular expression
     * <b>or</b> if it is a string or symbol that matches the regular
     * expression. It is functionally equivalent to {@link #matches(Pattern)}.
     * </p>
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     * 
     * @see #matches(Pattern)
     */
    public ConditionBuilder equals(final Pattern value) {
        myOtherComparisons.clear();
        myEqualsComparison = new RegularExpressionElement(getFieldName(), value);
        return this;
    }

    /**
     * Checks if the value equals the specified <tt>value</tt>.
     * <p>
     * <b>NOTE:</b> This method will match against a string or a symbol type
     * element.
     * </p>
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equals(final String value) {
        myOtherComparisons.clear();
        myEqualsComparison = new StringElement(getFieldName(), value);
        return this;
    }

    /**
     * Checks if the value equals the specified <tt>uuid</tt> using the standard
     * byte ordering.
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param uuid
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equals(final UUID uuid) {
        myOtherComparisons.clear();
        myEqualsComparison = new UuidElement(getFieldName(), uuid);
        return this;
    }

    /**
     * Checks if the value equals the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equalsJavaScript(final String value) {
        myOtherComparisons.clear();
        myEqualsComparison = new JavaScriptElement(getFieldName(), value);
        return this;
    }

    /**
     * Checks if the value equals the specified <tt>value</tt>.
     * <p>
     * <b>NOTE:</b> Testing has shown that the <tt>scope</tt> is ignored when
     * performing the comparison.
     * </p>
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @param scope
     *            The stored scope value.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equalsJavaScript(final String value,
            final DocumentAssignable scope) {
        myOtherComparisons.clear();
        myEqualsComparison = new JavaScriptWithScopeElement(getFieldName(),
                value, scope.asDocument());
        return this;
    }

    /**
     * Checks if the value equals the specified <tt>value</tt> using the legacy
     * Java byte ordering.
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param uuid
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equalsLegacy(final UUID uuid) {
        myOtherComparisons.clear();
        myEqualsComparison = new UuidElement(getFieldName(),
                UuidElement.LEGACY_UUID_SUBTTYPE, uuid);
        return this;
    }

    /**
     * Checks if the value is a max key element.
     * <p>
     * <b>WARNING:</b> Testing has shown that this query matches all documents
     * even if they do not contain any value for the field.
     * </p>
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equalsMaxKey() {
        myOtherComparisons.clear();
        myEqualsComparison = new MaxKeyElement(getFieldName());
        return this;
    }

    /**
     * Checks if the value is a min key element.
     * <p>
     * <b>WARNING:</b> Testing has shown that this query matches all documents
     * even if they do not contain any value for the field.
     * </p>
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equalsMinKey() {
        myOtherComparisons.clear();
        myEqualsComparison = new MinKeyElement(getFieldName());
        return this;
    }

    /**
     * Checks if the value equals the specified <tt>value</tt>.
     * <p>
     * <b>WARNING:</b> Testing has shown that this query will throw a
     * {@link QueryFailedException} if a document contains a field with the same
     * name but of type {@link ElementType#UTC_TIMESTAMP}.
     * </p>
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equalsMongoTimestamp(final long value) {
        myOtherComparisons.clear();
        myEqualsComparison = new MongoTimestampElement(getFieldName(), value);
        return this;
    }

    /**
     * Checks if the value is a null value.
     * <p>
     * <b>Note:</b> A field is considered to be <code>null</code> if it is a
     * literal <code>null</code> value in the document <b>OR</b> it does not
     * contain the field.
     * </p>
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equalsNull() {
        myOtherComparisons.clear();
        myEqualsComparison = new NullElement(getFieldName());
        return this;
    }

    /**
     * Checks if the value equals the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equalsSymbol(final String value) {
        myOtherComparisons.clear();
        myEqualsComparison = new SymbolElement(getFieldName(), value);
        return this;
    }

    /**
     * Checks if the value equals the specified <tt>value</tt>.
     * <p>
     * <b>NOTE:</b> Queries for matching a timestamp value will compare equals
     * to an equivalent stored MongoTimestamp value.
     * </p>
     * <p>
     * Only a single {@link #equals(boolean) equals(...)} comparison can be
     * used. Calling multiple {@link #equals(byte[]) equals(...)} methods
     * overwrites previous values. In addition <tt>equals(...)</tt> removes all
     * other conditions from the builder since there is no equal operator
     * supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder equalsTimestamp(final long value) {
        myOtherComparisons.clear();
        myEqualsComparison = new TimestampElement(getFieldName(), value);
        return this;
    }

    /**
     * Checks if the field exists (or not) in the document.
     * <p>
     * Only a single {@link #exists(boolean) exists(...)} comparison can be
     * used. Calling multiple {@link #exists() exists(...)} methods overwrites
     * previous values. In addition any {@link #equals(boolean) equals(...)}
     * condition is removed since no equality operator is supported by MongoDB.
     * </p>
     * <p>
     * This method is equivalent to calling {@link #exists(boolean)
     * exists(true)}.
     * </p>
     * 
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder exists() {
        return exists(true);
    }

    /**
     * Checks if the field exists (or not) in the document.
     * <p>
     * Only a single {@link #exists(boolean) exists(...)} comparison can be
     * used. Calling multiple {@link #exists() exists(...)} methods overwrites
     * previous values. In addition any {@link #equals(boolean) equals(...)}
     * condition is removed since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param value
     *            If true the field must exist. If false the field must not
     *            exist.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder exists(final boolean value) {
        myEqualsComparison = null;
        myOtherComparisons.put(MiscellaneousOperator.EXISTS,
                new BooleanElement(MiscellaneousOperator.EXISTS.getToken(),
                        value));
        return this;
    }

    /**
     * Geospatial query for documents whose field intersects the specified
     * {@link GeoJson GeoJSON} specified geometry.
     * <p>
     * This method is designed to be use with a GeoJSON document constructed
     * with the {@link GeoJson} class<blockquote>
     * 
     * <pre>
     * <code>
     * {@link QueryBuilder#where where}("geo").intersects({@link GeoJson#lineString GeoJson.lineString}( {@link GeoJson#p GeoJson.p}(1,2),{@link GeoJson#p GeoJson.p}(10,11) ) );
     * </code>
     * </pre>
     * 
     * </blockquote>
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $geoIntersects} operator requires a
     * {@link Index#geo2dSphere(String) 2dsphere} index.
     * </p>
     * <p>
     * Only a single {@link #intersects} comparison can be used. Calling
     * multiple <tt>intersects(...)</tt> methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param geoJsonDoc
     *            The GeoJSON document describing the geometry.
     * @return The condition builder for chaining method calls.
     * 
     * @since MongoDB 2.4
     */
    public ConditionBuilder geoWithin(final DocumentAssignable geoJsonDoc) {
        myEqualsComparison = null;

        myOtherComparisons.put(GeospatialOperator.GEO_WITHIN,
                new DocumentElement(GeospatialOperator.GEO_WITHIN.getToken(),
                        new DocumentElement(GeospatialOperator.GEOMETRY,
                                geoJsonDoc.asDocument())));

        return this;
    }

    /**
     * Returns the fieldName value.
     * 
     * @return The fieldName value.
     */
    public String getFieldName() {
        return myFieldName;
    }

    /**
     * Checks if the value is greater than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThan(int) greaterThan(...)} comparison can
     * be used. Calling multiple {@link #greaterThan(byte[]) greaterThan(...)}
     * methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param subType
     *            The binary values subtype.
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThan(final byte subType, final byte[] value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GT, new BinaryElement(
                ComparisonOperator.GT.getToken(), subType, value));
        return this;
    }

    /**
     * Checks if the value is greater than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThan(int) greaterThan(...)} comparison can
     * be used. Calling multiple {@link #greaterThan(byte[]) greaterThan(...)}
     * methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThan(final byte[] value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GT, new BinaryElement(
                ComparisonOperator.GT.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is greater than the specified <tt>dateTime</tt>.
     * <p>
     * This is equivalent to {@link #greaterThanTimestamp(long)
     * greaterThanTimestamp(dateTime.getTime())}.
     * </p>
     * <p>
     * Only a single {@link #greaterThan(int) greaterThan(...)} comparison can
     * be used. Calling multiple {@link #greaterThan(byte[]) greaterThan(...)}
     * methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param dateTime
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     * @see #greaterThanTimestamp(long)
     */
    public ConditionBuilder greaterThan(final Date dateTime) {
        return greaterThanTimestamp(dateTime.getTime());
    }

    /**
     * Checks if the value is greater than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThan(int) greaterThan(...)} comparison can
     * be used. Calling multiple {@link #greaterThan(byte[]) greaterThan(...)}
     * methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThan(final double value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GT, new DoubleElement(
                ComparisonOperator.GT.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is greater than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThan(int) greaterThan(...)} comparison can
     * be used. Calling multiple {@link #greaterThan(byte[]) greaterThan(...)}
     * methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThan(final int value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GT, new IntegerElement(
                ComparisonOperator.GT.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is greater than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThan(int) greaterThan(...)} comparison can
     * be used. Calling multiple {@link #greaterThan(byte[]) greaterThan(...)}
     * methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThan(final long value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GT, new LongElement(
                ComparisonOperator.GT.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is greater than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThan(int) greaterThan(...)} comparison can
     * be used. Calling multiple {@link #greaterThan(byte[]) greaterThan(...)}
     * methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThan(final ObjectId value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GT, new ObjectIdElement(
                ComparisonOperator.GT.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is greater than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThan(int) greaterThan(...)} comparison can
     * be used. Calling multiple {@link #greaterThan(byte[]) greaterThan(...)}
     * methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThan(final String value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GT, new StringElement(
                ComparisonOperator.GT.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is greater than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThan(int) greaterThan(...)} comparison can
     * be used. Calling multiple {@link #greaterThan(byte[]) greaterThan(...)}
     * methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThanMongoTimestamp(final long value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GT,
                new MongoTimestampElement(ComparisonOperator.GT.getToken(),
                        value));
        return this;
    }

    /**
     * Checks if the value is greater than or equals the specified
     * <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThanOrEqualTo(int)
     * greaterThanOrEqualTo(...)} comparison can be used. Calling multiple
     * {@link #greaterThanOrEqualTo(byte[]) greaterThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param subType
     *            The binary values subtype.
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThanOrEqualTo(final byte subType,
            final byte[] value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GTE, new BinaryElement(
                ComparisonOperator.GTE.getToken(), subType, value));
        return this;
    }

    /**
     * Checks if the value is greater than or equals the specified
     * <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThanOrEqualTo(int)
     * greaterThanOrEqualTo(...)} comparison can be used. Calling multiple
     * {@link #greaterThanOrEqualTo(byte[]) greaterThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThanOrEqualTo(final byte[] value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GTE, new BinaryElement(
                ComparisonOperator.GTE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is greater than or equals the specified
     * <tt>dateTime</tt>.
     * <p>
     * This is equivalent to {@link #greaterThanOrEqualToTimestamp(long)
     * greaterThanOrEqualToTimestamp(dateTime.getTime())}.
     * </p>
     * <p>
     * Only a single {@link #greaterThanOrEqualTo(int)
     * greaterThanOrEqualTo(...)} comparison can be used. Calling multiple
     * {@link #greaterThanOrEqualTo(byte[]) greaterThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param dateTime
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     * @see #greaterThanOrEqualToTimestamp(long)
     */
    public ConditionBuilder greaterThanOrEqualTo(final Date dateTime) {
        return greaterThanOrEqualToTimestamp(dateTime.getTime());
    }

    /**
     * Checks if the value is greater than or equals the specified
     * <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThanOrEqualTo(int)
     * greaterThanOrEqualTo(...)} comparison can be used. Calling multiple
     * {@link #greaterThanOrEqualTo(byte[]) greaterThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThanOrEqualTo(final double value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GTE, new DoubleElement(
                ComparisonOperator.GTE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is greater than or equals the specified
     * <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThanOrEqualTo(int)
     * greaterThanOrEqualTo(...)} comparison can be used. Calling multiple
     * {@link #greaterThanOrEqualTo(byte[]) greaterThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThanOrEqualTo(final int value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GTE, new IntegerElement(
                ComparisonOperator.GTE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is greater than or equals the specified
     * <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThanOrEqualTo(int)
     * greaterThanOrEqualTo(...)} comparison can be used. Calling multiple
     * {@link #greaterThanOrEqualTo(byte[]) greaterThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThanOrEqualTo(final long value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GTE, new LongElement(
                ComparisonOperator.GTE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is greater than or equals the specified
     * <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThanOrEqualTo(int)
     * greaterThanOrEqualTo(...)} comparison can be used. Calling multiple
     * {@link #greaterThanOrEqualTo(byte[]) greaterThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThanOrEqualTo(final ObjectId value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GTE, new ObjectIdElement(
                ComparisonOperator.GTE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is greater than or equals the specified
     * <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThanOrEqualTo(int)
     * greaterThanOrEqualTo(...)} comparison can be used. Calling multiple
     * {@link #greaterThanOrEqualTo(byte[]) greaterThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThanOrEqualTo(final String value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GTE, new StringElement(
                ComparisonOperator.GTE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is greater than or equals the specified
     * <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThanOrEqualTo(int)
     * greaterThanOrEqualTo(...)} comparison can be used. Calling multiple
     * {@link #greaterThanOrEqualTo(byte[]) greaterThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThanOrEqualToMongoTimestamp(final long value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GTE,
                new MongoTimestampElement(ComparisonOperator.GTE.getToken(),
                        value));
        return this;
    }

    /**
     * Checks if the value is greater than or equals the specified
     * <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThanOrEqualTo(int)
     * greaterThanOrEqualTo(...)} comparison can be used. Calling multiple
     * {@link #greaterThanOrEqualTo(byte[]) greaterThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThanOrEqualToSymbol(final String value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GTE, new SymbolElement(
                ComparisonOperator.GTE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is greater than or equals the specified
     * <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThanOrEqualTo(int)
     * greaterThanOrEqualTo(...)} comparison can be used. Calling multiple
     * {@link #greaterThanOrEqualTo(byte[]) greaterThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThanOrEqualToTimestamp(final long value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GTE, new TimestampElement(
                ComparisonOperator.GTE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is greater than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThan(int) greaterThan(...)} comparison can
     * be used. Calling multiple {@link #greaterThan(byte[]) greaterThan(...)}
     * methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThanSymbol(final String value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GT, new SymbolElement(
                ComparisonOperator.GT.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is greater than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #greaterThan(int) greaterThan(...)} comparison can
     * be used. Calling multiple {@link #greaterThan(byte[]) greaterThan(...)}
     * methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder greaterThanTimestamp(final long value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.GT, new TimestampElement(
                ComparisonOperator.GT.getToken(), value));
        return this;
    }

    /**
     * Specify the values that one must match the fields value.
     * <p>
     * Only a single {@link #in(ArrayBuilder)} comparison can be used. Calling
     * multiple <tt>in(...)</tt> methods overwrites previous values. In addition
     * any {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param elements
     *            A builder for the values for the comparison. Any changes to
     *            the {@link ArrayBuilder} after this method is called are not
     *            reflected in the comparison.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder in(final ArrayBuilder elements) {
        return in(elements.build());
    }

    /**
     * Specify the values that one must match the fields value.
     * <p>
     * Only a single {@link #in(Element[])} comparison can be used. Calling
     * multiple <tt>in(...)</tt> methods overwrites previous values. In addition
     * any {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param values
     *            The values for the comparison.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder in(final Constant... values) {
        myEqualsComparison = null;

        final List<Element> elements = new ArrayList<Element>(values.length);
        for (int i = 0; i < values.length; ++i) {
            elements.add(values[i].toElement(ArrayElement.nameFor(i)));
        }
        myOtherComparisons.put(MiscellaneousOperator.IN, new ArrayElement(
                MiscellaneousOperator.IN.getToken(), elements));

        return this;
    }

    /**
     * Specify the values that one must match the fields value.
     * <p>
     * Only a single {@link #in(Element[])} comparison can be used. Calling
     * multiple <tt>in(...)</tt> methods overwrites previous values. In addition
     * any {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param elements
     *            The element values for the comparison.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder in(final Element... elements) {
        myEqualsComparison = null;
        myOtherComparisons.put(MiscellaneousOperator.IN, new ArrayElement(
                MiscellaneousOperator.IN.getToken(), elements));

        return this;
    }

    /**
     * Checks if the value's type matches the specified <tt>type</tt>.
     * <p>
     * Only a single {@link #instanceOf(ElementType)} comparison can be used.
     * Calling multiple {@link #instanceOf(ElementType)} methods overwrites
     * previous values. In addition any {@link #equals(boolean) equals(...)}
     * condition is removed since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param type
     *            The expected type for the value.
     * 
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder instanceOf(final ElementType type) {
        myEqualsComparison = null;
        myOtherComparisons.put(MiscellaneousOperator.TYPE, new IntegerElement(
                MiscellaneousOperator.TYPE.getToken(), type.getToken()));
        return this;
    }

    /**
     * Geospatial query for documents whose field intersects the specified
     * {@link GeoJson GeoJSON} specified geometry.
     * <p>
     * This method is designed to be use with a GeoJSON document constructed
     * with the {@link GeoJson} class<blockquote>
     * 
     * <pre>
     * <code>
     * {@link QueryBuilder#where where}("geo").intersects({@link GeoJson#lineString GeoJson.lineString}( {@link GeoJson#p GeoJson.p}(1,2),{@link GeoJson#p GeoJson.p}(10,11) ) );
     * </code>
     * </pre>
     * 
     * </blockquote>
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $geoIntersects} operator requires a
     * {@link Index#geo2dSphere(String) 2dsphere} index.
     * </p>
     * <p>
     * Only a single {@link #intersects} comparison can be used. Calling
     * multiple <tt>intersects(...)</tt> methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param geoJsonDoc
     *            The GeoJSON document describing the geometry.
     * @return The condition builder for chaining method calls.
     * 
     * @since MongoDB 2.4
     */
    public ConditionBuilder intersects(final DocumentAssignable geoJsonDoc) {
        myEqualsComparison = null;

        myOtherComparisons.put(GeospatialOperator.INTERSECT,
                new DocumentElement(GeospatialOperator.INTERSECT.getToken(),
                        new DocumentElement(GeospatialOperator.GEOMETRY,
                                geoJsonDoc.asDocument())));

        return this;
    }

    /**
     * Checks if the value is less than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThan(int) lessThan(...)} comparison can be
     * used. Calling multiple {@link #lessThan(byte[]) lessThan(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param subType
     *            The binary values subtype.
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThan(final byte subType, final byte[] value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LT, new BinaryElement(
                ComparisonOperator.LT.getToken(), subType, value));
        return this;
    }

    /**
     * Checks if the value is less than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThan(int) lessThan(...)} comparison can be
     * used. Calling multiple {@link #lessThan(byte[]) lessThan(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThan(final byte[] value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LT, new BinaryElement(
                ComparisonOperator.LT.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is less than the specified <tt>dateTime</tt>.
     * <p>
     * This is equivalent to {@link #lessThanTimestamp(long)
     * lessThanTimestamp(dateTime.getTime())}.
     * </p>
     * <p>
     * Only a single {@link #lessThan(int) lessThan(...)} comparison can be
     * used. Calling multiple {@link #lessThan(byte[]) lessThan(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param dateTime
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     * @see #lessThanTimestamp(long)
     */
    public ConditionBuilder lessThan(final Date dateTime) {
        return lessThanTimestamp(dateTime.getTime());
    }

    /**
     * Checks if the value is less than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThan(int) lessThan(...)} comparison can be
     * used. Calling multiple {@link #lessThan(byte[]) lessThan(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThan(final double value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LT, new DoubleElement(
                ComparisonOperator.LT.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is less than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThan(int) lessThan(...)} comparison can be
     * used. Calling multiple {@link #lessThan(byte[]) lessThan(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThan(final int value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LT, new IntegerElement(
                ComparisonOperator.LT.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is less than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThan(int) lessThan(...)} comparison can be
     * used. Calling multiple {@link #lessThan(byte[]) lessThan(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThan(final long value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LT, new LongElement(
                ComparisonOperator.LT.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is less than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThan(int) lessThan(...)} comparison can be
     * used. Calling multiple {@link #lessThan(byte[]) lessThan(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThan(final ObjectId value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LT, new ObjectIdElement(
                ComparisonOperator.LT.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is less than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThan(int) lessThan(...)} comparison can be
     * used. Calling multiple {@link #lessThan(byte[]) lessThan(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThan(final String value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LT, new StringElement(
                ComparisonOperator.LT.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is less than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThan(int) lessThan(...)} comparison can be
     * used. Calling multiple {@link #lessThan(byte[]) lessThan(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThanMongoTimestamp(final long value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LT,
                new MongoTimestampElement(ComparisonOperator.LT.getToken(),
                        value));
        return this;
    }

    /**
     * Checks if the value is less than or equals the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThanOrEqualTo(int) lessThanOrEqualTo(...)}
     * comparison can be used. Calling multiple
     * {@link #lessThanOrEqualTo(byte[]) lessThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param subType
     *            The binary values subtype.
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThanOrEqualTo(final byte subType,
            final byte[] value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LTE, new BinaryElement(
                ComparisonOperator.LTE.getToken(), subType, value));
        return this;
    }

    /**
     * Checks if the value is less than or equals the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThanOrEqualTo(int) lessThanOrEqualTo(...)}
     * comparison can be used. Calling multiple
     * {@link #lessThanOrEqualTo(byte[]) lessThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThanOrEqualTo(final byte[] value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LTE, new BinaryElement(
                ComparisonOperator.LTE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is less than or equals the specified
     * <tt>dateTime</tt>.
     * <p>
     * This is equivalent to {@link #lessThanOrEqualToTimestamp(long)
     * lessThanOrEqualToTimestamp(dateTime.getTime())}.
     * </p>
     * <p>
     * Only a single {@link #lessThanOrEqualTo(int) lessThanOrEqualTo(...)}
     * comparison can be used. Calling multiple
     * {@link #lessThanOrEqualTo(byte[]) lessThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param dateTime
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     * @see #lessThanOrEqualToTimestamp(long)
     */
    public ConditionBuilder lessThanOrEqualTo(final Date dateTime) {
        return lessThanOrEqualToTimestamp(dateTime.getTime());
    }

    /**
     * Checks if the value is less than or equals the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThanOrEqualTo(int) lessThanOrEqualTo(...)}
     * comparison can be used. Calling multiple
     * {@link #lessThanOrEqualTo(byte[]) lessThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThanOrEqualTo(final double value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LTE, new DoubleElement(
                ComparisonOperator.LTE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is less than or equals the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThanOrEqualTo(int) lessThanOrEqualTo(...)}
     * comparison can be used. Calling multiple
     * {@link #lessThanOrEqualTo(byte[]) lessThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThanOrEqualTo(final int value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LTE, new IntegerElement(
                ComparisonOperator.LTE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is less than or equals the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThanOrEqualTo(int) lessThanOrEqualTo(...)}
     * comparison can be used. Calling multiple
     * {@link #lessThanOrEqualTo(byte[]) lessThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThanOrEqualTo(final long value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LTE, new LongElement(
                ComparisonOperator.LTE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is less than or equals the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThanOrEqualTo(int) lessThanOrEqualTo(...)}
     * comparison can be used. Calling multiple
     * {@link #lessThanOrEqualTo(byte[]) lessThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThanOrEqualTo(final ObjectId value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LTE, new ObjectIdElement(
                ComparisonOperator.LTE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is less than or equals the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThanOrEqualTo(int) lessThanOrEqualTo(...)}
     * comparison can be used. Calling multiple
     * {@link #lessThanOrEqualTo(byte[]) lessThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThanOrEqualTo(final String value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LTE, new StringElement(
                ComparisonOperator.LTE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is less than or equals the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThanOrEqualTo(int) lessThanOrEqualTo(...)}
     * comparison can be used. Calling multiple
     * {@link #lessThanOrEqualTo(byte[]) lessThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThanOrEqualToMongoTimestamp(final long value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LTE,
                new MongoTimestampElement(ComparisonOperator.LTE.getToken(),
                        value));
        return this;
    }

    /**
     * Checks if the value is less than or equals the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThanOrEqualTo(int) lessThanOrEqualTo(...)}
     * comparison can be used. Calling multiple
     * {@link #lessThanOrEqualTo(byte[]) lessThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThanOrEqualToSymbol(final String value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LTE, new SymbolElement(
                ComparisonOperator.LTE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is less than or equals the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThanOrEqualTo(int) lessThanOrEqualTo(...)}
     * comparison can be used. Calling multiple
     * {@link #lessThanOrEqualTo(byte[]) lessThanOrEqualTo(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThanOrEqualToTimestamp(final long value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LTE, new TimestampElement(
                ComparisonOperator.LTE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is less than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThan(int) lessThan(...)} comparison can be
     * used. Calling multiple {@link #lessThan(byte[]) lessThan(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThanSymbol(final String value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LT, new SymbolElement(
                ComparisonOperator.LT.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is less than the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #lessThan(int) lessThan(...)} comparison can be
     * used. Calling multiple {@link #lessThan(byte[]) lessThan(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder lessThanTimestamp(final long value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.LT, new TimestampElement(
                ComparisonOperator.LT.getToken(), value));
        return this;
    }

    /**
     * Checks if the value matches the specified <tt>pattern</tt>.
     * <p>
     * NOTE: This checks if the value <b>is</b> a regular expression <b>or</b>
     * if it is a string or symbol that matches the regular expression. It is
     * functionally equivalent to {@link #equals(Pattern)}.
     * <p>
     * Only a single {@link #matches(Pattern)} comparison can be used. Calling
     * multiple {@link #matches(Pattern)} methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * <p>
     * This method is equivalent to calling {@link #exists(boolean)
     * exists(true)}.
     * </p>
     * 
     * @param pattern
     *            The pattern to match the value against.
     * 
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder matches(final Pattern pattern) {
        myEqualsComparison = null;
        myOtherComparisons.put(
                MiscellaneousOperator.REG_EX,
                new RegularExpressionElement(MiscellaneousOperator.REG_EX
                        .getToken(), pattern));
        return this;
    }

    /**
     * Checks if the modulo of the documents value and <tt>divisor</tt> equals
     * the <tt>remainder</tt>.
     * <p>
     * Only a single {@link #mod(int,int)} comparison can be used. Calling
     * multiple {@link #mod(long,long)} methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param divisor
     *            The divisor for the modulo operation.
     * @param remainder
     *            The desired remainder from the modulo operation.
     * 
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder mod(final int divisor, final int remainder) {
        myEqualsComparison = null;

        final ArrayBuilder builder = BuilderFactory.startArray();
        builder.addInteger(divisor);
        builder.addInteger(remainder);

        myOtherComparisons.put(MiscellaneousOperator.MOD, new ArrayElement(
                MiscellaneousOperator.MOD.getToken(), builder.build()));
        return this;
    }

    /**
     * Checks if the modulo of the documents value and <tt>divisor</tt> equals
     * the <tt>remainder</tt>.
     * <p>
     * Only a single {@link #mod(int,int)} comparison can be used. Calling
     * multiple {@link #mod(long,long)} methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param divisor
     *            The divisor for the modulo operation.
     * @param remainder
     *            The desired remainder from the modulo operation.
     * 
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder mod(final long divisor, final long remainder) {
        myEqualsComparison = null;

        final ArrayBuilder builder = BuilderFactory.startArray();
        builder.addLong(divisor);
        builder.addLong(remainder);

        myOtherComparisons.put(MiscellaneousOperator.MOD, new ArrayElement(
                MiscellaneousOperator.MOD.getToken(), builder.build()));
        return this;
    }

    /**
     * Geospatial query for documents whose field is near the specified
     * {@link GeoJson GeoJSON} specified geometry.
     * <p>
     * This method is designed to be use with a GeoJSON document constructed
     * with the {@link GeoJson} class<blockquote>
     * 
     * <pre>
     * <code>
     * {@link QueryBuilder#where where}("geo").near({@link GeoJson#lineString GeoJson.lineString}( {@link GeoJson#p GeoJson.p}(1,2),{@link GeoJson#p GeoJson.p}(10,11) ) );
     * </code>
     * </pre>
     * 
     * </blockquote>
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $near} operator is not supported with sharded
     * clusters.
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $near} operator with a GeoJSON document requires
     * a {@link Index#geo2dSphere(String) 2dsphere} index.
     * </p>
     * <p>
     * Only a single {@link #near} comparison can be used. Calling multiple
     * <tt>near(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param geoJsonDoc
     *            The GeoJSON document describing the geometry.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder near(final DocumentAssignable geoJsonDoc) {
        myEqualsComparison = null;
        myOtherComparisons.put(GeospatialOperator.NEAR, new DocumentElement(
                GeospatialOperator.NEAR.getToken(), new DocumentElement(
                        GeospatialOperator.GEOMETRY, geoJsonDoc.asDocument())));
        myOtherComparisons.remove(GeospatialOperator.MAX_DISTANCE_MODIFIER);

        return this;
    }

    /**
     * Geospatial query for documents whose field is near the specified
     * {@link GeoJson GeoJSON} specified geometry.
     * <p>
     * This method is designed to be use with a GeoJSON document constructed
     * with the {@link GeoJson} class<blockquote>
     * 
     * <pre>
     * <code>
     * {@link QueryBuilder#where where}("geo").near({@link GeoJson#lineString GeoJson.lineString}( {@link GeoJson#p GeoJson.p}(1,2),{@link GeoJson#p GeoJson.p}(10,11) ), 42 );
     * </code>
     * </pre>
     * 
     * </blockquote>
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $near} operator is not supported with sharded
     * clusters.
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $near} operator with a GeoJSON document requires
     * a {@link Index#geo2dSphere(String) 2dsphere} index.
     * </p>
     * <p>
     * Only a single {@link #near} comparison can be used. Calling multiple
     * <tt>near(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param geoJsonDoc
     *            The GeoJSON document describing the geometry.
     * @param maxDistance
     *            Limits to documents returned to those within the specified
     *            maximum distance.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder near(final DocumentAssignable geoJsonDoc,
            final double maxDistance) {
        myEqualsComparison = null;
        myOtherComparisons.put(
                GeospatialOperator.NEAR,
                new DocumentElement(GeospatialOperator.NEAR.getToken(),
                        new DocumentElement(GeospatialOperator.GEOMETRY,
                                geoJsonDoc.asDocument()), new DoubleElement(
                                GeospatialOperator.MAX_DISTANCE_MODIFIER
                                        .getToken(), maxDistance)));
        myOtherComparisons.remove(GeospatialOperator.MAX_DISTANCE_MODIFIER);

        return this;
    }

    /**
     * Geospatial query for documents whose field is near the specified [
     * <tt>x</tt>, <tt>y</tt>] coordinates.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $near} operator is not supported with sharded
     * clusters.
     * </p>
     * <p>
     * Only a single {@link #near} comparison can be used. Calling multiple
     * <tt>near(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate to find documents near.
     * @param y
     *            The Y coordinate to find documents near.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder near(final double x, final double y) {
        myEqualsComparison = null;
        myOtherComparisons.put(GeospatialOperator.NEAR, new ArrayElement(
                GeospatialOperator.NEAR.getToken(), new DoubleElement("0", x),
                new DoubleElement("1", y)));
        myOtherComparisons.remove(GeospatialOperator.MAX_DISTANCE_MODIFIER);

        return this;
    }

    /**
     * Geospatial query for documents whose field is near the specified [
     * <tt>x</tt>, <tt>y</tt>] coordinates.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $near} operator is not supported with sharded
     * clusters.
     * </p>
     * <p>
     * Only a single {@link #near} comparison can be used. Calling multiple
     * <tt>near(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate to find documents near.
     * @param y
     *            The Y coordinate to find documents near.
     * @param maxDistance
     *            Limits to documents returned to those within the specified
     *            maximum distance.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder near(final double x, final double y,
            final double maxDistance) {
        myEqualsComparison = null;
        myOtherComparisons.put(GeospatialOperator.NEAR, new ArrayElement(
                GeospatialOperator.NEAR.getToken(), new DoubleElement("0", x),
                new DoubleElement("1", y)));
        myOtherComparisons.put(
                GeospatialOperator.MAX_DISTANCE_MODIFIER,
                new DoubleElement(GeospatialOperator.MAX_DISTANCE_MODIFIER
                        .getToken(), maxDistance));

        return this;
    }

    /**
     * Geospatial query for documents whose field is near the specified [
     * <tt>x</tt>, <tt>y</tt>] coordinates.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $near} operator is not supported with sharded
     * clusters.
     * </p>
     * <p>
     * Only a single {@link #near} comparison can be used. Calling multiple
     * <tt>near(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate to find documents near.
     * @param y
     *            The Y coordinate to find documents near.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder near(final int x, final int y) {
        myEqualsComparison = null;
        myOtherComparisons.put(GeospatialOperator.NEAR, new ArrayElement(
                GeospatialOperator.NEAR.getToken(), new IntegerElement("0", x),
                new IntegerElement("1", y)));
        myOtherComparisons.remove(GeospatialOperator.MAX_DISTANCE_MODIFIER);

        return this;
    }

    /**
     * Geospatial query for documents whose field is near the specified [
     * <tt>x</tt>, <tt>y</tt>] coordinates.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $near} operator is not supported with sharded
     * clusters.
     * </p>
     * <p>
     * Only a single {@link #near} comparison can be used. Calling multiple
     * <tt>near(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate to find documents near.
     * @param y
     *            The Y coordinate to find documents near.
     * @param maxDistance
     *            Limits to documents returned to those within the specified
     *            maximum distance.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder near(final int x, final int y, final int maxDistance) {
        myEqualsComparison = null;
        myOtherComparisons.put(GeospatialOperator.NEAR, new ArrayElement(
                GeospatialOperator.NEAR.getToken(), new IntegerElement("0", x),
                new IntegerElement("1", y)));
        myOtherComparisons.put(
                GeospatialOperator.MAX_DISTANCE_MODIFIER,
                new IntegerElement(GeospatialOperator.MAX_DISTANCE_MODIFIER
                        .getToken(), maxDistance));

        return this;
    }

    /**
     * Geospatial query for documents whose field is near the specified [
     * <tt>x</tt>, <tt>y</tt>] coordinates.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $near} operator is not supported with sharded
     * clusters.
     * </p>
     * <p>
     * Only a single {@link #near} comparison can be used. Calling multiple
     * <tt>near(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate to find documents near.
     * @param y
     *            The Y coordinate to find documents near.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder near(final long x, final long y) {
        myEqualsComparison = null;
        myOtherComparisons.put(GeospatialOperator.NEAR, new ArrayElement(
                GeospatialOperator.NEAR.getToken(), new LongElement("0", x),
                new LongElement("1", y)));
        myOtherComparisons.remove(GeospatialOperator.MAX_DISTANCE_MODIFIER);

        return this;
    }

    /**
     * Geospatial query for documents whose field is near the specified [
     * <tt>x</tt>, <tt>y</tt>] coordinates.
     * <p>
     * <b>NOTE:</b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $near} operator is not supported with sharded
     * clusters.
     * </p>
     * <p>
     * Only a single {@link #near} comparison can be used. Calling multiple
     * <tt>near(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate to find documents near.
     * @param y
     *            The Y coordinate to find documents near.
     * @param maxDistance
     *            Limits to documents returned to those within the specified
     *            maximum distance.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder near(final long x, final long y,
            final long maxDistance) {
        myEqualsComparison = null;
        myOtherComparisons.put(GeospatialOperator.NEAR, new ArrayElement(
                GeospatialOperator.NEAR.getToken(), new LongElement("0", x),
                new LongElement("1", y)));
        myOtherComparisons.put(
                GeospatialOperator.MAX_DISTANCE_MODIFIER,
                new LongElement(GeospatialOperator.MAX_DISTANCE_MODIFIER
                        .getToken(), maxDistance));

        return this;
    }

    /**
     * Geospatial query for documents whose field is near the specified
     * {@link GeoJson GeoJSON} specified geometry on a sphere.
     * <p>
     * This method is designed to be use with a GeoJSON document constructed
     * with the {@link GeoJson} class<blockquote>
     * 
     * <pre>
     * <code>
     * {@link QueryBuilder#where where}("geo").nearSphere({@link GeoJson#lineString GeoJson.lineString}( {@link GeoJson#p GeoJson.p}(1,2),{@link GeoJson#p GeoJson.p}(10,11) ) );
     * </code>
     * </pre>
     * 
     * </blockquote>
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $nearSphere} operator is not supported with
     * sharded clusters.
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $nearSphere} operator with a GeoJSON document
     * requires a {@link Index#geo2dSphere(String) 2dsphere} index.
     * </p>
     * <p>
     * Only a single {@link #nearSphere} comparison can be used. Calling
     * multiple <tt>near(...)</tt> methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param geoJsonDoc
     *            The GeoJSON document describing the geometry.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder nearSphere(final DocumentAssignable geoJsonDoc) {
        myEqualsComparison = null;
        myOtherComparisons.put(GeospatialOperator.NEAR_SPHERE,
                new DocumentElement(GeospatialOperator.NEAR_SPHERE.getToken(),
                        new DocumentElement(GeospatialOperator.GEOMETRY,
                                geoJsonDoc.asDocument())));
        myOtherComparisons.remove(GeospatialOperator.MAX_DISTANCE_MODIFIER);

        return this;
    }

    /**
     * Geospatial query for documents whose field is near the specified
     * {@link GeoJson GeoJSON} specified geometry on a sphere.
     * <p>
     * This method is designed to be use with a GeoJSON document constructed
     * with the {@link GeoJson} class<blockquote>
     * 
     * <pre>
     * <code>
     * {@link QueryBuilder#where where}("geo").nearSphere({@link GeoJson#lineString GeoJson.lineString}( {@link GeoJson#p GeoJson.p}(1,2),{@link GeoJson#p GeoJson.p}(10,11) ), 42 );
     * </code>
     * </pre>
     * 
     * </blockquote>
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $nearSphere} operator is not supported with
     * sharded clusters.
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $nearSphere} operator with a GeoJSON document
     * requires a {@link Index#geo2dSphere(String) 2dsphere} index.
     * </p>
     * <p>
     * Only a single {@link #nearSphere} comparison can be used. Calling
     * multiple <tt>near(...)</tt> methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param geoJsonDoc
     *            The GeoJSON document describing the geometry.
     * @param maxDistance
     *            Limits to documents returned to those within the specified
     *            maximum distance.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder nearSphere(final DocumentAssignable geoJsonDoc,
            final double maxDistance) {
        myEqualsComparison = null;
        myOtherComparisons.put(
                GeospatialOperator.NEAR_SPHERE,
                new DocumentElement(GeospatialOperator.NEAR_SPHERE.getToken(),
                        new DocumentElement(GeospatialOperator.GEOMETRY,
                                geoJsonDoc.asDocument()), new DoubleElement(
                                GeospatialOperator.MAX_DISTANCE_MODIFIER
                                        .getToken(), maxDistance)));
        myOtherComparisons.remove(GeospatialOperator.MAX_DISTANCE_MODIFIER);

        return this;
    }

    /**
     * Geospatial query for documents whose field is near the specified [
     * <tt>x</tt>, <tt>y</tt>] coordinates on a sphere.
     * <p>
     * <b>NOTE:</b> The <tt>x</tt> must be within the range [-180, 180) and the
     * <tt>y</tt> values must be in the range (-90, 90) or the query will throw
     * a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $nearSphere} operator is not supported with
     * sharded clusters.
     * </p>
     * <p>
     * Only a single {@link #nearSphere} comparison can be used. Calling
     * multiple <tt>nearSphere(...)</tt> methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate to find documents near.
     * @param y
     *            The Y coordinate to find documents near.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder nearSphere(final double x, final double y) {
        myEqualsComparison = null;
        myOtherComparisons.put(GeospatialOperator.NEAR_SPHERE,
                new ArrayElement(GeospatialOperator.NEAR_SPHERE.getToken(),
                        new DoubleElement("0", x), new DoubleElement("1", y)));
        myOtherComparisons.remove(GeospatialOperator.MAX_DISTANCE_MODIFIER);

        return this;
    }

    /**
     * Geospatial query for documents whose field is near the specified [
     * <tt>x</tt>, <tt>y</tt>] coordinates on a sphere.
     * <p>
     * <b>NOTE:</b> The <tt>x</tt> must be within the range [-180, 180) and the
     * <tt>y</tt> values must be in the range (-90, 90) or the query will throw
     * a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $nearSphere} operator is not supported with
     * sharded clusters.
     * </p>
     * <p>
     * Only a single {@link #nearSphere} comparison can be used. Calling
     * multiple <tt>nearSphere(...)</tt> methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate to find documents near.
     * @param y
     *            The Y coordinate to find documents near.
     * @param maxDistance
     *            Limits to documents returned to those within the specified
     *            maximum distance.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder nearSphere(final double x, final double y,
            final double maxDistance) {
        myEqualsComparison = null;
        myOtherComparisons.put(GeospatialOperator.NEAR_SPHERE,
                new ArrayElement(GeospatialOperator.NEAR_SPHERE.getToken(),
                        new DoubleElement("0", x), new DoubleElement("1", y)));
        myOtherComparisons.put(
                GeospatialOperator.MAX_DISTANCE_MODIFIER,
                new DoubleElement(GeospatialOperator.MAX_DISTANCE_MODIFIER
                        .getToken(), maxDistance));

        return this;
    }

    /**
     * Geospatial query for documents whose field is near the specified [
     * <tt>x</tt>, <tt>y</tt>] coordinates on a sphere.
     * <p>
     * <b>NOTE:</b> The <tt>x</tt> must be within the range [-180, 180) and the
     * <tt>y</tt> values must be in the range (-90, 90) or the query will throw
     * a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $nearSphere} operator is not supported with
     * sharded clusters.
     * </p>
     * <p>
     * Only a single {@link #nearSphere} comparison can be used. Calling
     * multiple <tt>nearSphere(...)</tt> methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate to find documents near.
     * @param y
     *            The Y coordinate to find documents near.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder nearSphere(final int x, final int y) {
        myEqualsComparison = null;
        myOtherComparisons
                .put(GeospatialOperator.NEAR_SPHERE, new ArrayElement(
                        GeospatialOperator.NEAR_SPHERE.getToken(),
                        new IntegerElement("0", x), new IntegerElement("1", y)));
        myOtherComparisons.remove(GeospatialOperator.MAX_DISTANCE_MODIFIER);

        return this;
    }

    /**
     * Geospatial query for documents whose field is near the specified [
     * <tt>x</tt>, <tt>y</tt>] coordinates on a sphere.
     * <p>
     * <b>NOTE:</b> The <tt>x</tt> must be within the range [-180, 180) and the
     * <tt>y</tt> values must be in the range (-90, 90) or the query will throw
     * a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $nearSphere} operator is not supported with
     * sharded clusters.
     * </p>
     * <p>
     * Only a single {@link #nearSphere} comparison can be used. Calling
     * multiple <tt>nearSphere(...)</tt> methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate to find documents near.
     * @param y
     *            The Y coordinate to find documents near.
     * @param maxDistance
     *            Limits to documents returned to those within the specified
     *            maximum distance.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder nearSphere(final int x, final int y,
            final int maxDistance) {
        myEqualsComparison = null;
        myOtherComparisons
                .put(GeospatialOperator.NEAR_SPHERE, new ArrayElement(
                        GeospatialOperator.NEAR_SPHERE.getToken(),
                        new IntegerElement("0", x), new IntegerElement("1", y)));
        myOtherComparisons.put(
                GeospatialOperator.MAX_DISTANCE_MODIFIER,
                new IntegerElement(GeospatialOperator.MAX_DISTANCE_MODIFIER
                        .getToken(), maxDistance));

        return this;
    }

    /**
     * Geospatial query for documents whose field is near the specified [
     * <tt>x</tt>, <tt>y</tt>] coordinates on a sphere.
     * <p>
     * <b>NOTE:</b> The <tt>x</tt> must be within the range [-180, 180) and the
     * <tt>y</tt> values must be in the range (-90, 90) or the query will throw
     * a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $nearSphere} operator is not supported with
     * sharded clusters.
     * </p>
     * <p>
     * Only a single {@link #nearSphere} comparison can be used. Calling
     * multiple <tt>nearSphere(...)</tt> methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate to find documents near.
     * @param y
     *            The Y coordinate to find documents near.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder nearSphere(final long x, final long y) {
        myEqualsComparison = null;
        myOtherComparisons.put(GeospatialOperator.NEAR_SPHERE,
                new ArrayElement(GeospatialOperator.NEAR_SPHERE.getToken(),
                        new LongElement("0", x), new LongElement("1", y)));
        myOtherComparisons.remove(GeospatialOperator.MAX_DISTANCE_MODIFIER);

        return this;
    }

    /**
     * Geospatial query for documents whose field is near the specified [
     * <tt>x</tt>, <tt>y</tt>] coordinates on a sphere.
     * <p>
     * <b>NOTE:</b> The <tt>x</tt> must be within the range [-180, 180) and the
     * <tt>y</tt> values must be in the range (-90, 90) or the query will throw
     * a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $nearSphere} operator is not supported with
     * sharded clusters.
     * </p>
     * <p>
     * Only a single {@link #nearSphere} comparison can be used. Calling
     * multiple <tt>nearSphere(...)</tt> methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate to find documents near.
     * @param y
     *            The Y coordinate to find documents near.
     * @param maxDistance
     *            Limits to documents returned to those within the specified
     *            maximum distance.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder nearSphere(final long x, final long y,
            final long maxDistance) {
        myEqualsComparison = null;
        myOtherComparisons.put(GeospatialOperator.NEAR_SPHERE,
                new ArrayElement(GeospatialOperator.NEAR_SPHERE.getToken(),
                        new LongElement("0", x), new LongElement("1", y)));
        myOtherComparisons.put(
                GeospatialOperator.MAX_DISTANCE_MODIFIER,
                new LongElement(GeospatialOperator.MAX_DISTANCE_MODIFIER
                        .getToken(), maxDistance));

        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualTo(final boolean value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE, new BooleanElement(
                ComparisonOperator.NE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param subType
     *            The binary values subtype.
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualTo(final byte subType, final byte[] value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE, new BinaryElement(
                ComparisonOperator.NE.getToken(), subType, value));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualTo(final byte[] value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE, new BinaryElement(
                ComparisonOperator.NE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>dateTime</tt>.
     * <p>
     * This is equivalent to {@link #notEqualToTimestamp(long)
     * notEqualToTimestamp(dateTime.getTime())}.
     * </p>
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param dateTime
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     * @see #notEqualToTimestamp(long)
     */
    public ConditionBuilder notEqualTo(final Date dateTime) {
        return notEqualToTimestamp(dateTime.getTime());
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualTo(final DocumentAssignable value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE, new DocumentElement(
                ComparisonOperator.NE.getToken(), value.asDocument()));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualTo(final double value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE, new DoubleElement(
                ComparisonOperator.NE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualTo(final int value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE, new IntegerElement(
                ComparisonOperator.NE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualTo(final long value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE, new LongElement(
                ComparisonOperator.NE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualTo(final ObjectId value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE, new ObjectIdElement(
                ComparisonOperator.NE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * <b>WARNING:</b> Testing has shown that this query will throw a
     * {@link QueryFailedException}.
     * </p>
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     * 
     * @see #matches(Pattern)
     */
    public ConditionBuilder notEqualTo(final Pattern value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE,
                new RegularExpressionElement(ComparisonOperator.NE.getToken(),
                        value));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualTo(final String value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE, new StringElement(
                ComparisonOperator.NE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>uuid</tt> using the
     * standard UUID byte ordering.
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param uuid
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualTo(final UUID uuid) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE, new UuidElement(
                ComparisonOperator.NE.getToken(), uuid));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualToJavaScript(final String value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE, new JavaScriptElement(
                ComparisonOperator.NE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * <b>NOTE:</b> Testing has shown that the <tt>scope</tt> is ignored when
     * performing the comparison.
     * </p>
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @param scope
     *            The stored scope value.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualToJavaScript(final String value,
            final DocumentAssignable scope) {
        myEqualsComparison = null;
        myOtherComparisons.put(
                ComparisonOperator.NE,
                new JavaScriptWithScopeElement(
                        ComparisonOperator.NE.getToken(), value, scope
                                .asDocument()));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>uuid</tt> using the
     * legacy Java UUID byte ordering.
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param uuid
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualToLegacy(final UUID uuid) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE, new UuidElement(
                ComparisonOperator.NE.getToken(),
                UuidElement.LEGACY_UUID_SUBTTYPE, uuid));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * <b>WARNING:</b> Testing has shown that this query matches all documents
     * even if the value of the field is a {@link ElementType#MIN_KEY}.
     * </p>
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualToMaxKey() {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE, new MaxKeyElement(
                ComparisonOperator.NE.getToken()));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * <b>WARNING:</b> Testing has shown that this query matches all documents
     * even if the value of the field is a {@link ElementType#MIN_KEY}.
     * </p>
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualToMinKey() {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE, new MinKeyElement(
                ComparisonOperator.NE.getToken()));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualToMongoTimestamp(final long value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE,
                new MongoTimestampElement(ComparisonOperator.NE.getToken(),
                        value));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualToNull() {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE, new NullElement(
                ComparisonOperator.NE.getToken()));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualToSymbol(final String value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE, new SymbolElement(
                ComparisonOperator.NE.getToken(), value));
        return this;
    }

    /**
     * Checks if the value is not equal to the specified <tt>value</tt>.
     * <p>
     * Only a single {@link #notEqualTo(boolean) notEqualTo(...)} comparison can
     * be used. Calling multiple {@link #notEqualTo(byte[]) equals(...)} methods
     * overwrites previous values. In addition any {@link #equals(boolean)
     * equals(...)} condition is removed since no equality operator is supported
     * by MongoDB.
     * </p>
     * 
     * @param value
     *            The value to compare the field against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notEqualToTimestamp(final long value) {
        myEqualsComparison = null;
        myOtherComparisons.put(ComparisonOperator.NE, new TimestampElement(
                ComparisonOperator.NE.getToken(), value));
        return this;
    }

    /**
     * Specify the values that must <em>not</em> must not match the fields
     * value.
     * <p>
     * Only a single {@link #notIn(ArrayBuilder)} comparison can be used.
     * Calling multiple <tt>notIn(...)</tt> methods overwrites previous values.
     * In addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param elements
     *            A builder for the values for the comparison. Any changes to
     *            the {@link ArrayBuilder} after this method is called are not
     *            reflected in the comparison.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notIn(final ArrayBuilder elements) {
        return notIn(elements.build());
    }

    /**
     * Specify the values that must <em>not</em> must not match the fields
     * value.
     * <p>
     * Only a single {@link #notIn(Element[])} comparison can be used. Calling
     * multiple <tt>notIn(...)</tt> methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param values
     *            The values for the comparison.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notIn(final Constant... values) {
        myEqualsComparison = null;

        final List<Element> elements = new ArrayList<Element>(values.length);
        for (int i = 0; i < values.length; ++i) {
            elements.add(values[i].toElement(ArrayElement.nameFor(i)));
        }
        myOtherComparisons.put(MiscellaneousOperator.NIN, new ArrayElement(
                MiscellaneousOperator.NIN.getToken(), elements));

        return this;
    }

    /**
     * Specify the values that must <em>not</em> must not match the fields
     * value.
     * <p>
     * Only a single {@link #notIn(Element[])} comparison can be used. Calling
     * multiple <tt>notIn(...)</tt> methods overwrites previous values. In
     * addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param elements
     *            The element values for the comparison.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder notIn(final Element... elements) {
        myEqualsComparison = null;
        myOtherComparisons.put(MiscellaneousOperator.NIN, new ArrayElement(
                MiscellaneousOperator.NIN.getToken(), elements));

        return this;
    }

    /**
     * Resets the builder back to an empty, no condition, state.
     */
    public void reset() {
        myOtherComparisons.clear();
        myEqualsComparison = null;
    }

    /**
     * Checks if the value is an array of the specified <tt>length</tt>.
     * <p>
     * Only a single {@link #size(int) lessThan(...)} comparison can be used.
     * Calling multiple <tt>size(int)</tt> methods overwrites previous values.
     * In addition any {@link #equals(boolean) equals(...)} condition is removed
     * since no equality operator is supported by MongoDB.
     * </p>
     * 
     * @param length
     *            The value to compare the field's length against.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder size(final int length) {
        myEqualsComparison = null;
        myOtherComparisons.put(MiscellaneousOperator.SIZE, new IntegerElement(
                MiscellaneousOperator.SIZE.getToken(), length));
        return this;
    }

    /**
     * Adds an ad-hoc JavaScript condition to the query.
     * <p>
     * Only a single {@link #where(String)} condition can be used. Calling
     * multiple <tt>where(...)</tt> methods overwrites previous values.
     * </p>
     * 
     * @param javaScript
     *            The javaScript condition to add.
     * @return This builder for call chaining.
     */
    public ConditionBuilder where(final String javaScript) {
        myParent.whereJavaScript(javaScript);

        return this;
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding polygon.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>within(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param uniqueDocs
     *            Controls if documents are returned multiple times for multiple
     *            matching conditions.
     * @param p1
     *            The first point defining the bounds of the polygon.
     * @param p2
     *            The second point defining the bounds of the polygon.
     * @param p3
     *            The third point defining the bounds of the polygon.
     * @param points
     *            The remaining points in the polygon.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder within(final boolean uniqueDocs, final Point2D p1,
            final Point2D p2, final Point2D p3, final Point2D... points) {
        myEqualsComparison = null;

        final DocumentBuilder builder = BuilderFactory.start();
        final ArrayBuilder box = builder.pushArray(GeospatialOperator.POLYGON);

        box.pushArray().addDouble(p1.getX()).addDouble(p1.getY());
        box.pushArray().addDouble(p2.getX()).addDouble(p2.getY());
        box.pushArray().addDouble(p3.getX()).addDouble(p3.getY());
        for (final Point2D p : points) {
            box.pushArray().addDouble(p.getX()).addDouble(p.getY());
        }

        builder.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, uniqueDocs);

        myOtherComparisons.put(GeospatialOperator.WITHIN, new DocumentElement(
                GeospatialOperator.WITHIN.getToken(), builder.build()));

        return this;
    }

    /**
     * Geospatial query for documents whose field is near the specified
     * {@link GeoJson GeoJSON} specified geometry.
     * <p>
     * This method is designed to be use with a GeoJSON document constructed
     * with the {@link GeoJson} class<blockquote>
     * 
     * <pre>
     * <code>
     * {@link QueryBuilder#where where}("geo").near({@link GeoJson#lineString GeoJson.lineString}( {@link GeoJson#p GeoJson.p}(1,2),{@link GeoJson#p GeoJson.p}(10,11) ) );
     * </code>
     * </pre>
     * 
     * </blockquote>
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $near} operator is not supported with sharded
     * clusters.
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $near} operator with a GeoJSON document requires
     * a {@link Index#geo2dSphere(String) 2dsphere} index.
     * </p>
     * <p>
     * Only a single {@link #near} comparison can be used. Calling multiple
     * <tt>near(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param geoJsonDoc
     *            The GeoJSON document describing the geometry.
     * @return The condition builder for chaining method calls.
     */

    /**
     * Geospatial query for documents whose field is within the specified
     * {@link GeoJson GeoJSON} specified geometry.
     * <p>
     * This method is designed to be use with a GeoJSON document constructed
     * with the {@link GeoJson} class<blockquote>
     * 
     * <pre>
     * <code>
     * {@link QueryBuilder#where where}("geo").within({@link GeoJson#lineString GeoJson.lineString}( {@link GeoJson#p GeoJson.p}(1,2),{@link GeoJson#p GeoJson.p}(10,11) ) );
     * </code>
     * </pre>
     * 
     * </blockquote>
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $within} operator with a GeoJSON document
     * requires a {@link Index#geo2dSphere(String) 2dsphere} index.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>within(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param geoJsonDoc
     *            The GeoJSON document describing the geometry.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder within(final DocumentAssignable geoJsonDoc) {
        myEqualsComparison = null;

        myOtherComparisons.put(GeospatialOperator.WITHIN, new DocumentElement(
                GeospatialOperator.WITHIN.getToken(), new DocumentElement(
                        GeospatialOperator.GEOMETRY, geoJsonDoc.asDocument())));

        return this;
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * {@link GeoJson GeoJSON} specified geometry.
     * <p>
     * This method is designed to be use with a GeoJSON document constructed
     * with the {@link GeoJson} class<blockquote>
     * 
     * <pre>
     * <code>
     * {@link QueryBuilder#where where}("geo").within({@link GeoJson#lineString GeoJson.lineString}( {@link GeoJson#p GeoJson.p}(1,2),{@link GeoJson#p GeoJson.p}(10,11) ), true );
     * </code>
     * </pre>
     * 
     * </blockquote>
     * </p>
     * <p>
     * <b>NOTE: </b> The {@code $within} operator with a GeoJSON document
     * requires a {@link Index#geo2dSphere(String) 2dsphere} index.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>within(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param geoJsonDoc
     *            The GeoJSON document describing the geometry.
     * @param uniqueDocs
     *            Controls if documents are returned multiple times for multiple
     *            matching conditions.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder within(final DocumentAssignable geoJsonDoc,
            final boolean uniqueDocs) {
        myEqualsComparison = null;

        myOtherComparisons.put(GeospatialOperator.WITHIN, new DocumentElement(
                GeospatialOperator.WITHIN.getToken(), new DocumentElement(
                        GeospatialOperator.GEOMETRY, geoJsonDoc.asDocument()),
                new BooleanElement(GeospatialOperator.UNIQUE_DOCS_MODIFIER,
                        uniqueDocs)));

        return this;
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding circular region.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>within(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate for the center of the circle.
     * @param y
     *            The Y coordinate for the center of the circle.
     * @param radius
     *            The radius of the circle.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder within(final double x, final double y,
            final double radius) {
        return within(x, y, radius, true);
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding circular region.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>within(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate for the center of the circle.
     * @param y
     *            The Y coordinate for the center of the circle.
     * @param radius
     *            The radius of the circle.
     * @param uniqueDocs
     *            Controls if documents are returned multiple times for multiple
     *            matching conditions.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder within(final double x, final double y,
            final double radius, final boolean uniqueDocs) {
        myEqualsComparison = null;

        final DocumentBuilder builder = BuilderFactory.start();
        final ArrayBuilder box = builder.pushArray(GeospatialOperator.CIRCLE);
        box.pushArray().addDouble(x).addDouble(y);
        box.addDouble(radius);
        builder.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, uniqueDocs);

        myOtherComparisons.put(GeospatialOperator.WITHIN, new DocumentElement(
                GeospatialOperator.WITHIN.getToken(), builder.build()));

        return this;
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding rectangular region.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>within(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x1
     *            The first X coordinate.
     * @param y1
     *            The first Y coordinate.
     * @param x2
     *            The second X coordinate. NOT THE WIDTH.
     * @param y2
     *            The second Y coordinate. NOT THE HEIGHT.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder within(final double x1, final double y1,
            final double x2, final double y2) {
        return within(x1, y1, x2, y2, true);
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding rectangular region.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>within(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x1
     *            The first X coordinate.
     * @param y1
     *            The first Y coordinate.
     * @param x2
     *            The second X coordinate. NOT THE WIDTH.
     * @param y2
     *            The second Y coordinate. NOT THE HEIGHT.
     * @param uniqueDocs
     *            Controls if documents are returned multiple times for multiple
     *            matching conditions.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder within(final double x1, final double y1,
            final double x2, final double y2, final boolean uniqueDocs) {
        myEqualsComparison = null;

        final DocumentBuilder builder = BuilderFactory.start();
        final ArrayBuilder box = builder.pushArray(GeospatialOperator.BOX);
        box.pushArray().addDouble(Math.min(x1, x2)).addDouble(Math.min(y1, y2));
        box.pushArray().addDouble(Math.max(x1, x2)).addDouble(Math.max(y1, y2));
        builder.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, uniqueDocs);

        myOtherComparisons.put(GeospatialOperator.WITHIN, new DocumentElement(
                GeospatialOperator.WITHIN.getToken(), builder.build()));

        return this;
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding circular region.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>within(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate for the center of the circle.
     * @param y
     *            The Y coordinate for the center of the circle.
     * @param radius
     *            The radius of the circle.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder within(final int x, final int y, final int radius) {
        return within(x, y, radius, true);
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding circular region.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>within(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate for the center of the circle.
     * @param y
     *            The Y coordinate for the center of the circle.
     * @param radius
     *            The radius of the circle.
     * @param uniqueDocs
     *            Controls if documents are returned multiple times for multiple
     *            matching conditions.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder within(final int x, final int y, final int radius,
            final boolean uniqueDocs) {
        myEqualsComparison = null;

        final DocumentBuilder builder = BuilderFactory.start();
        final ArrayBuilder box = builder.pushArray(GeospatialOperator.CIRCLE);
        box.pushArray().addInteger(x).addInteger(y);
        box.addInteger(radius);
        builder.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, uniqueDocs);

        myOtherComparisons.put(GeospatialOperator.WITHIN, new DocumentElement(
                GeospatialOperator.WITHIN.getToken(), builder.build()));

        return this;
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding rectangular region.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>within(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x1
     *            The first X coordinate.
     * @param y1
     *            The first Y coordinate.
     * @param x2
     *            The second X coordinate. NOT THE WIDTH.
     * @param y2
     *            The second Y coordinate. NOT THE HEIGHT.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder within(final int x1, final int y1, final int x2,
            final int y2) {
        return within(x1, y1, x2, y2, true);
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding rectangular region.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>within(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x1
     *            The first X coordinate.
     * @param y1
     *            The first Y coordinate.
     * @param x2
     *            The second X coordinate. NOT THE WIDTH.
     * @param y2
     *            The second Y coordinate. NOT THE HEIGHT.
     * @param uniqueDocs
     *            Controls if documents are returned multiple times for multiple
     *            matching conditions.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder within(final int x1, final int y1, final int x2,
            final int y2, final boolean uniqueDocs) {
        myEqualsComparison = null;

        final DocumentBuilder builder = BuilderFactory.start();
        final ArrayBuilder box = builder.pushArray(GeospatialOperator.BOX);
        box.pushArray().addInteger(Math.min(x1, x2))
                .addInteger(Math.min(y1, y2));
        box.pushArray().addInteger(Math.max(x1, x2))
                .addInteger(Math.max(y1, y2));
        builder.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, uniqueDocs);

        myOtherComparisons.put(GeospatialOperator.WITHIN, new DocumentElement(
                GeospatialOperator.WITHIN.getToken(), builder.build()));

        return this;
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding circular region.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>within(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate for the center of the circle.
     * @param y
     *            The Y coordinate for the center of the circle.
     * @param radius
     *            The radius of the circle.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder within(final long x, final long y, final long radius) {
        return within(x, y, radius, true);
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding circular region.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>within(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate for the center of the circle.
     * @param y
     *            The Y coordinate for the center of the circle.
     * @param radius
     *            The radius of the circle.
     * @param uniqueDocs
     *            Controls if documents are returned multiple times for multiple
     *            matching conditions.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder within(final long x, final long y,
            final long radius, final boolean uniqueDocs) {
        myEqualsComparison = null;

        final DocumentBuilder builder = BuilderFactory.start();
        final ArrayBuilder box = builder.pushArray(GeospatialOperator.CIRCLE);
        box.pushArray().addLong(x).addLong(y);
        box.addLong(radius);
        builder.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, uniqueDocs);

        myOtherComparisons.put(GeospatialOperator.WITHIN, new DocumentElement(
                GeospatialOperator.WITHIN.getToken(), builder.build()));

        return this;
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding rectangular region.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>within(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x1
     *            The first X coordinate.
     * @param y1
     *            The first Y coordinate.
     * @param x2
     *            The second X coordinate. NOT THE WIDTH.
     * @param y2
     *            The second Y coordinate. NOT THE HEIGHT.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder within(final long x1, final long y1, final long x2,
            final long y2) {
        return within(x1, y1, x2, y2, true);
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding rectangular region.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>within(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x1
     *            The first X coordinate.
     * @param y1
     *            The first Y coordinate.
     * @param x2
     *            The second X coordinate. NOT THE WIDTH.
     * @param y2
     *            The second Y coordinate. NOT THE HEIGHT.
     * @param uniqueDocs
     *            Controls if documents are returned multiple times for multiple
     *            matching conditions.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder within(final long x1, final long y1, final long x2,
            final long y2, final boolean uniqueDocs) {
        myEqualsComparison = null;

        final DocumentBuilder builder = BuilderFactory.start();
        final ArrayBuilder box = builder.pushArray(GeospatialOperator.BOX);
        box.pushArray().addLong(Math.min(x1, x2)).addLong(Math.min(y1, y2));
        box.pushArray().addLong(Math.max(x1, x2)).addLong(Math.max(y1, y2));
        builder.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, uniqueDocs);

        myOtherComparisons.put(GeospatialOperator.WITHIN, new DocumentElement(
                GeospatialOperator.WITHIN.getToken(), builder.build()));

        return this;
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding polygon.
     * <p>
     * <b>NOTE: </b> The <tt>x</tt> and <tt>y</tt> values must be in the range
     * [-180, 180) or the query will throw a {@link QueryFailedException}.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>within(...)</tt> methods overwrites previous values. In addition any
     * {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param p1
     *            The first point defining the bounds of the polygon.
     * @param p2
     *            The second point defining the bounds of the polygon.
     * @param p3
     *            The third point defining the bounds of the polygon.
     * @param points
     *            The remaining points in the polygon.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder within(final Point2D p1, final Point2D p2,
            final Point2D p3, final Point2D... points) {
        return within(true, p1, p2, p3, points);
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding circular region on a sphere.
     * <p>
     * <b>NOTE:</b> The <tt>x</tt> must be within the range [-180, 180) and the
     * <tt>y</tt> values must be in the range (-90, 90) or the query will throw
     * a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE 2:</b> The <tt>x</tt>, <tt>y</tt> and <tt>radius</tt> must not
     * wrap since that has not been implemented yet within MongoDB.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>withinXXX(...)</tt> methods overwrites previous values. In addition
     * any {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate for the center of the circle.
     * @param y
     *            The Y coordinate for the center of the circle.
     * @param radius
     *            The radius of the circle.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder withinOnSphere(final double x, final double y,
            final double radius) {
        return withinOnSphere(x, y, radius, true);
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding circular region on a sphere.
     * <p>
     * <b>NOTE:</b> The <tt>x</tt> must be within the range [-180, 180) and the
     * <tt>y</tt> values must be in the range (-90, 90) or the query will throw
     * a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE 2:</b> The <tt>x</tt>, <tt>y</tt> and <tt>radius</tt> must not
     * wrap since that has not been implemented yet within MongoDB.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>withinXXX(...)</tt> methods overwrites previous values. In addition
     * any {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate for the center of the circle.
     * @param y
     *            The Y coordinate for the center of the circle.
     * @param radius
     *            The radius of the circle.
     * @param uniqueDocs
     *            Controls if documents are returned multiple times for multiple
     *            matching conditions.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder withinOnSphere(final double x, final double y,
            final double radius, final boolean uniqueDocs) {
        myEqualsComparison = null;

        final DocumentBuilder builder = BuilderFactory.start();
        final ArrayBuilder box = builder
                .pushArray(GeospatialOperator.SPHERICAL_CIRCLE);
        box.pushArray().addDouble(x).addDouble(y);
        box.addDouble(radius);
        builder.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, uniqueDocs);

        myOtherComparisons.put(GeospatialOperator.WITHIN, new DocumentElement(
                GeospatialOperator.WITHIN.getToken(), builder.build()));

        return this;
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding circular region on a sphere.
     * <p>
     * <b>NOTE:</b> The <tt>x</tt> must be within the range [-180, 180) and the
     * <tt>y</tt> values must be in the range (-90, 90) or the query will throw
     * a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE 2:</b> The <tt>x</tt>, <tt>y</tt> and <tt>radius</tt> must not
     * wrap since that has not been implemented yet within MongoDB.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>withinXXX(...)</tt> methods overwrites previous values. In addition
     * any {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate for the center of the circle.
     * @param y
     *            The Y coordinate for the center of the circle.
     * @param radius
     *            The radius of the circle.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder withinOnSphere(final int x, final int y,
            final int radius) {
        return withinOnSphere(x, y, radius, true);
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding circular region on a sphere.
     * <p>
     * <b>NOTE:</b> The <tt>x</tt> must be within the range [-180, 180) and the
     * <tt>y</tt> values must be in the range (-90, 90) or the query will throw
     * a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE 2:</b> The <tt>x</tt>, <tt>y</tt> and <tt>radius</tt> must not
     * wrap since that has not been implemented yet within MongoDB.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>withinXXX(...)</tt> methods overwrites previous values. In addition
     * any {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate for the center of the circle.
     * @param y
     *            The Y coordinate for the center of the circle.
     * @param radius
     *            The radius of the circle.
     * @param uniqueDocs
     *            Controls if documents are returned multiple times for multiple
     *            matching conditions.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder withinOnSphere(final int x, final int y,
            final int radius, final boolean uniqueDocs) {
        myEqualsComparison = null;

        final DocumentBuilder builder = BuilderFactory.start();
        final ArrayBuilder box = builder
                .pushArray(GeospatialOperator.SPHERICAL_CIRCLE);
        box.pushArray().addInteger(x).addInteger(y);
        box.addInteger(radius);
        builder.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, uniqueDocs);

        myOtherComparisons.put(GeospatialOperator.WITHIN, new DocumentElement(
                GeospatialOperator.WITHIN.getToken(), builder.build()));

        return this;
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding circular region on a sphere.
     * <p>
     * <b>NOTE:</b> The <tt>x</tt> must be within the range [-180, 180) and the
     * <tt>y</tt> values must be in the range (-90, 90) or the query will throw
     * a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE 2:</b> The <tt>x</tt>, <tt>y</tt> and <tt>radius</tt> must not
     * wrap since that has not been implemented yet within MongoDB.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>withinXXX(...)</tt> methods overwrites previous values. In addition
     * any {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate for the center of the circle.
     * @param y
     *            The Y coordinate for the center of the circle.
     * @param radius
     *            The radius of the circle.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder withinOnSphere(final long x, final long y,
            final long radius) {
        return withinOnSphere(x, y, radius, true);
    }

    /**
     * Geospatial query for documents whose field is within the specified
     * bounding circular region on a sphere.
     * <p>
     * <b>NOTE:</b> The <tt>x</tt> must be within the range [-180, 180) and the
     * <tt>y</tt> values must be in the range (-90, 90) or the query will throw
     * a {@link QueryFailedException}.
     * </p>
     * <p>
     * <b>NOTE 2:</b> The <tt>x</tt>, <tt>y</tt> and <tt>radius</tt> must not
     * wrap since that has not been implemented yet within MongoDB.
     * </p>
     * <p>
     * Only a single {@link #within} comparison can be used. Calling multiple
     * <tt>withinXXX(...)</tt> methods overwrites previous values. In addition
     * any {@link #equals(boolean) equals(...)} condition is removed since no
     * equality operator is supported by MongoDB.
     * </p>
     * 
     * @param x
     *            The X coordinate for the center of the circle.
     * @param y
     *            The Y coordinate for the center of the circle.
     * @param radius
     *            The radius of the circle.
     * @param uniqueDocs
     *            Controls if documents are returned multiple times for multiple
     *            matching conditions.
     * @return The condition builder for chaining method calls.
     */
    public ConditionBuilder withinOnSphere(final long x, final long y,
            final long radius, final boolean uniqueDocs) {
        myEqualsComparison = null;

        final DocumentBuilder builder = BuilderFactory.start();
        final ArrayBuilder box = builder
                .pushArray(GeospatialOperator.SPHERICAL_CIRCLE);
        box.pushArray().addLong(x).addLong(y);
        box.addLong(radius);
        builder.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, uniqueDocs);

        myOtherComparisons.put(GeospatialOperator.WITHIN, new DocumentElement(
                GeospatialOperator.WITHIN.getToken(), builder.build()));

        return this;
    }

    /**
     * Returns the element representing the current state of this fields
     * condition.
     * 
     * @return The element for the condition which may be <code>null</code> if
     *         no condition has been set.
     */
    /* package */Element buildFieldCondition() {
        if (!myOtherComparisons.isEmpty()) {
            return new DocumentElement(myFieldName, myOtherComparisons.values());
        }

        // Note - This may be null.
        return myEqualsComparison;
    }
}
