/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder.expression;

import java.util.Date;
import java.util.regex.Pattern;

import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.LongElement;
import com.allanbank.mongodb.bson.element.MongoTimestampElement;
import com.allanbank.mongodb.bson.element.NullElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.ObjectIdElement;
import com.allanbank.mongodb.bson.element.RegularExpressionElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.TimestampElement;

/**
 * Expressions provides a collection of static helper method for constructing
 * complex expression.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class Expressions {

    /** The {@value} operator token */
    public static final String ADD = "$add";

    /** The {@value} operator token */
    public static final String ALL_ELEMENTS_TRUE = "$allElementsTrue";

    /** The {@value} operator token */
    public static final String AND = "$and";

    /** The {@value} operator token */
    public static final String ANY_ELEMENT_TRUE = "$anyElementTrue";

    /** The {@value} operator token */
    public static final String COMPARE = "$cmp";

    /** The {@value} operator token */
    public static final String CONCATENATE = "$concat";

    /** The {@value} operator token */
    public static final String CONDITION = "$cond";

    /** The {@value} operator token */
    public static final String DAY_OF_MONTH = "$dayOfMonth";

    /** The {@value} operator token */
    public static final String DAY_OF_WEEK = "$dayOfWeek";

    /** The {@value} operator token */
    public static final String DAY_OF_YEAR = "$dayOfYear";

    /** The {@value} operator token */
    public static final String DIVIDE = "$divide";

    /** The {@value} operator token */
    public static final String EQUAL = "$eq";

    /** The {@value} operator token */
    public static final String GREATER_THAN = "$gt";

    /** The {@value} operator token */
    public static final String GREATER_THAN_OR_EQUAL = "$gte";

    /** The {@value} operator token */
    public static final String HOUR = "$hour";

    /** The {@value} operator token */
    public static final String IF_NULL = "$ifNull";

    /** The {@value} operator token */
    public static final String LESS_THAN = "$lt";

    /** The {@value} operator token */
    public static final String LESS_THAN_OR_EQUAL = "$lte";

    /** The {@value} operator token */
    public static final String LITERAL = "$literal";

    /** The {@value} operator token */
    public static final String MILLISECOND = "$millisecond";

    /** The {@value} operator token */
    public static final String MINUTE = "$minute";

    /** The {@value} operator token */
    public static final String MODULO = "$mod";

    /** The {@value} operator token */
    public static final String MONTH = "$month";

    /** The {@value} operator token */
    public static final String MULTIPLY = "$multiply";

    /** The {@value} operator token */
    public static final String NOT = "$not";

    /** The {@value} operator token */
    public static final String NOT_EQUAL = "$ne";

    /** The {@value} operator token */
    public static final String OR = "$or";

    /** The {@value} operator token */
    public static final String SECOND = "$second";

    /** The {@value} operator token */
    public static final String SET_DIFFERENCE = "$setDifference";

    /** The {@value} operator token */
    public static final String SET_EQUALS = "$setEquals";

    /** The {@value} operator token */
    public static final String SET_INTERSECTION = "$setIntersection";

    /** The {@value} operator token */
    public static final String SET_IS_SUBSET = "$setIsSubset";

    /** The {@value} operator token */
    public static final String SET_UNION = "$setUnion";

    /** The {@value} operator token */
    public static final String SIZE = "$size";

    /** The {@value} operator token */
    public static final String STRING_CASE_INSENSITIVE_COMPARE = "$strcasecmp";

    /** The {@value} operator token */
    public static final String SUB_STRING = "$substr";

    /** The {@value} operator token */
    public static final String SUBTRACT = "$subtract";

    /** The {@value} operator token */
    public static final String TO_LOWER = "$toLower";

    /** The {@value} operator token */
    public static final String TO_UPPER = "$toUpper";

    /** The {@value} operator token */
    public static final String WEEK = "$week";

    /** The {@value} operator token */
    public static final String YEAR = "$year";

    /**
     * Returns an {@link NaryExpression} {@value #ADD} expression.
     * 
     * @param expressions
     *            The sub-expressions.
     * @return The {@link NaryExpression} {@value #ADD} expression.
     */
    public static NaryExpression add(final Expression... expressions) {
        return new NaryExpression(ADD, expressions);
    }

    /**
     * Returns a {@link UnaryExpression} {@value #ALL_ELEMENTS_TRUE} expression.
     * 
     * @param expression
     *            The expression that will be evaluated to create the set to
     *            inspect for a true element.
     * @return The {@link UnaryExpression} {@value #ALL_ELEMENTS_TRUE}
     *         expression.
     */
    public static UnaryExpression allElementsTrue(final Expression expression) {
        return new UnaryExpression(ALL_ELEMENTS_TRUE, expression);
    }

    /**
     * Returns an {@link NaryExpression} {@value #AND} expression.
     * 
     * @param expressions
     *            The sub-expressions.
     * @return The {@link NaryExpression} {@value #AND} expression.
     */
    public static NaryExpression and(final Expression... expressions) {
        return new NaryExpression(AND, expressions);
    }

    /**
     * Returns a {@link UnaryExpression} {@value #ANY_ELEMENT_TRUE} expression.
     * 
     * @param expression
     *            The expression that will be evaluated to create the set to
     *            inspect for a true element.
     * @return The {@link UnaryExpression} {@value #ANY_ELEMENT_TRUE}
     *         expression.
     */
    public static UnaryExpression anyElementTrue(final Expression expression) {
        return new UnaryExpression(ANY_ELEMENT_TRUE, expression);
    }

    /**
     * Returns a {@link NaryExpression} {@value #COMPARE} expression.
     * 
     * @param lhs
     *            The left hand side of the operation.
     * @param rhs
     *            The left hand side of the operation.
     * @return The {@link NaryExpression} {@value #COMPARE} expression.
     */
    public static NaryExpression cmp(final Expression lhs, final Expression rhs) {
        return new NaryExpression(COMPARE, lhs, rhs);
    }

    /**
     * Returns a {@link NaryExpression} {@value #CONCATENATE} expression.
     * 
     * @param expression
     *            The string expressions for the operator.
     * @return The {@link NaryExpression} {@value #CONCATENATE} expression.
     * 
     * @since MongoDB 2.4
     */
    public static NaryExpression concatenate(final Expression... expression) {
        return new NaryExpression(CONCATENATE, expression);
    }

    /**
     * Returns a {@link NaryExpression} {@value #CONDITION} expression.
     * 
     * @param test
     *            The conditions test.
     * @param trueResult
     *            The result if the test is true.
     * @param falseResult
     *            The result if the test is false.
     * @return The {@link NaryExpression} {@value #CONDITION} expression.
     */
    public static NaryExpression cond(final Expression test,
            final Expression trueResult, final Expression falseResult) {
        return new NaryExpression(CONDITION, test, trueResult, falseResult);
    }

    /**
     * Returns a {@link Constant} expression with the provided <tt>value</tt>.
     * 
     * @param value
     *            The constants value.
     * @return The {@link Constant} expression.
     */
    public static Constant constant(final boolean value) {
        return new Constant(new BooleanElement("", value));
    }

    /**
     * Returns a {@link Constant} expression with the provided <tt>value</tt>.
     * 
     * @param value
     *            The constants value.
     * @return The {@link Constant} expression.
     */
    public static Constant constant(final Date value) {
        return constantTimestamp(value.getTime());
    }

    /**
     * Returns a {@link Constant} expression with the provided <tt>value</tt>.
     * 
     * @param value
     *            The constants value.
     * @return The {@link Constant} expression.
     */
    public static Constant constant(final double value) {
        return new Constant(new DoubleElement("", value));
    }

    /**
     * Returns a {@link Constant} expression wrapping the provided
     * <tt>element</tt>.
     * 
     * @param element
     *            The element value.
     * @return The {@link Constant} expression.
     */
    public static Expression constant(final Element element) {
        return new Constant(element);
    }

    /**
     * Returns a {@link Constant} expression with the provided <tt>value</tt>.
     * 
     * @param value
     *            The constants value.
     * @return The {@link Constant} expression.
     */
    public static Constant constant(final int value) {
        return new Constant(new IntegerElement("", value));
    }

    /**
     * Returns a {@link Constant} expression with the provided <tt>value</tt>.
     * 
     * @param value
     *            The constants value.
     * @return The {@link Constant} expression.
     */
    public static Constant constant(final long value) {
        return new Constant(new LongElement("", value));
    }

    /**
     * Returns a {@link Constant} expression with the provided <tt>value</tt>.
     * 
     * @param value
     *            The constants value.
     * @return The {@link Constant} expression.
     */
    public static Constant constant(final ObjectId value) {
        return new Constant(new ObjectIdElement("", value));
    }

    /**
     * Returns a {@link Constant} expression with the provided <tt>value</tt>.
     * 
     * @param value
     *            The constants value.
     * @return The {@link Constant} expression.
     */
    public static Constant constant(final Pattern value) {
        return new Constant(new RegularExpressionElement("", value));
    }

    /**
     * Returns a {@link Constant} expression with the provided <tt>value</tt>.
     * 
     * @param value
     *            The constants value.
     * @return The {@link Constant} expression.
     */
    public static Constant constant(final String value) {
        return new Constant(new StringElement("", value));
    }

    /**
     * Returns a {@link Constant} expression with the provided <tt>value</tt>.
     * 
     * @param value
     *            The constants value.
     * @return The {@link Constant} expression.
     */
    public static Constant constantMongoTimestamp(final long value) {
        return new Constant(new MongoTimestampElement("", value));
    }

    /**
     * Returns a {@link Constant} expression with the provided <tt>value</tt>.
     * 
     * @param value
     *            The constants value.
     * @return The {@link Constant} expression.
     */
    public static Constant constantTimestamp(final long value) {
        return new Constant(new TimestampElement("", value));
    }

    /**
     * Returns a {@link UnaryExpression} {@value #DAY_OF_MONTH} expression.
     * 
     * @param expression
     *            The date for the operator.
     * @return The {@link UnaryExpression} {@value #DAY_OF_MONTH} expression.
     */
    public static UnaryExpression dayOfMonth(final Expression expression) {
        return new UnaryExpression(DAY_OF_MONTH, expression);
    }

    /**
     * Returns a {@link UnaryExpression} {@value #DAY_OF_WEEK} expression.
     * 
     * @param expression
     *            The date for the operator.
     * @return The {@link UnaryExpression} {@value #DAY_OF_WEEK} expression.
     */
    public static UnaryExpression dayOfWeek(final Expression expression) {
        return new UnaryExpression(DAY_OF_WEEK, expression);
    }

    /**
     * Returns a {@link UnaryExpression} {@value #DAY_OF_YEAR} expression.
     * 
     * @param expression
     *            The date for the operator.
     * @return The {@link UnaryExpression} {@value #DAY_OF_YEAR} expression.
     */
    public static UnaryExpression dayOfYear(final Expression expression) {
        return new UnaryExpression(DAY_OF_YEAR, expression);
    }

    /**
     * Returns a {@link NaryExpression} {@value #DIVIDE} expression.
     * 
     * @param numerator
     *            The numerator of the division.
     * @param denominator
     *            The denominator of the division.
     * @return The {@link NaryExpression} {@value #DIVIDE} expression.
     */
    public static NaryExpression divide(final Expression numerator,
            final Expression denominator) {
        return new NaryExpression(DIVIDE, numerator, denominator);
    }

    /**
     * Returns a {@link NaryExpression} {@value #EQUAL} expression.
     * 
     * @param lhs
     *            The left hand side of the equals.
     * @param rhs
     *            The right hand side of the equals.
     * @return The {@link NaryExpression} {@value #EQUAL} expression.
     */
    public static NaryExpression eq(final Expression lhs, final Expression rhs) {
        return new NaryExpression(EQUAL, lhs, rhs);
    }

    /**
     * Returns a {@link Constant} expression with the provided <tt>value</tt>.
     * 
     * @param value
     *            The constants value.
     * @return The {@link Constant} expression.
     */
    public static Constant field(final String value) {
        if (value.startsWith("$")) {
            return new Constant(new StringElement("", value));
        }
        return new Constant(new StringElement("", "$" + value));
    }

    /**
     * Returns a {@link NaryExpression} {@value #GREATER_THAN} expression.
     * 
     * @param lhs
     *            The left hand side of the comparison.
     * @param rhs
     *            The right hand side of the comparison.
     * @return The {@link NaryExpression} {@value #GREATER_THAN} expression.
     */
    public static NaryExpression gt(final Expression lhs, final Expression rhs) {
        return new NaryExpression(GREATER_THAN, lhs, rhs);
    }

    /**
     * Returns a {@link NaryExpression} {@value #GREATER_THAN_OR_EQUAL}
     * expression.
     * 
     * @param lhs
     *            The left hand side of the comparison.
     * @param rhs
     *            The right hand side of the comparison.
     * @return The {@link NaryExpression} {@value #GREATER_THAN_OR_EQUAL}
     *         expression.
     */
    public static NaryExpression gte(final Expression lhs, final Expression rhs) {
        return new NaryExpression(GREATER_THAN_OR_EQUAL, lhs, rhs);
    }

    /**
     * Returns a {@link UnaryExpression} {@value #HOUR} expression.
     * 
     * @param expression
     *            The date for the operator.
     * @return The {@link UnaryExpression} {@value #HOUR} expression.
     */
    public static UnaryExpression hour(final Expression expression) {
        return new UnaryExpression(HOUR, expression);
    }

    /**
     * Returns a {@link NaryExpression} {@value #IF_NULL} expression.
     * 
     * @param first
     *            The first expression.
     * @param second
     *            The second expression.
     * @return The {@link NaryExpression} {@value #IF_NULL} expression.
     */
    public static NaryExpression ifNull(final Expression first,
            final Expression second) {
        return new NaryExpression(IF_NULL, first, second);
    }

    /**
     * Returns a {@link Constant} expression wrapping the <tt>value</tt> in a
     * {@value #LITERAL} sub-document.
     * 
     * @param value
     *            The constants value.
     * @return The {@link Constant} expression.
     */
    public static Constant literal(final String value) {
        return new Constant(new DocumentElement("", new StringElement(
                "$literal", value)));
    }

    /**
     * Returns a {@link NaryExpression} {@value #LESS_THAN} expression.
     * 
     * @param lhs
     *            The left hand side of the comparison.
     * @param rhs
     *            The right hand side of the comparison.
     * @return The {@link NaryExpression} {@value #LESS_THAN} expression.
     */
    public static NaryExpression lt(final Expression lhs, final Expression rhs) {
        return new NaryExpression(LESS_THAN, lhs, rhs);
    }

    /**
     * Returns a {@link NaryExpression} {@value #LESS_THAN_OR_EQUAL} expression.
     * 
     * @param lhs
     *            The left hand side of the comparison.
     * @param rhs
     *            The right hand side of the comparison.
     * @return The {@link NaryExpression} {@value #LESS_THAN} expression.
     */
    public static NaryExpression lte(final Expression lhs, final Expression rhs) {
        return new NaryExpression(LESS_THAN_OR_EQUAL, lhs, rhs);
    }

    /**
     * Returns a {@link UnaryExpression} {@value #MILLISECOND} expression.
     * 
     * @param expression
     *            The date for the operator.
     * @return The {@link UnaryExpression} {@value #MILLISECOND} expression.
     */
    public static UnaryExpression millisecond(final Expression expression) {
        return new UnaryExpression(MILLISECOND, expression);
    }

    /**
     * Returns a {@link UnaryExpression} {@value #MINUTE} expression.
     * 
     * @param expression
     *            The date for the operator.
     * @return The {@link UnaryExpression} {@value #MINUTE} expression.
     */
    public static UnaryExpression minute(final Expression expression) {
        return new UnaryExpression(MINUTE, expression);
    }

    /**
     * Returns a {@link NaryExpression} {@value #MODULO} expression.
     * 
     * @param numerator
     *            The numerator of the modulo operation.
     * @param denominator
     *            The denominator of the modulo operation.
     * @return The {@link NaryExpression} {@value #MODULO} expression.
     */
    public static NaryExpression mod(final Expression numerator,
            final Expression denominator) {
        return new NaryExpression(MODULO, numerator, denominator);
    }

    /**
     * Returns a {@link UnaryExpression} {@value #MONTH} expression.
     * 
     * @param expression
     *            The date for the operator.
     * @return The {@link UnaryExpression} {@value #MONTH} expression.
     */
    public static UnaryExpression month(final Expression expression) {
        return new UnaryExpression(MONTH, expression);
    }

    /**
     * Returns a {@link NaryExpression} {@value #MULTIPLY} expression.
     * 
     * @param lhs
     *            The left hand side of the operator.
     * @param rhs
     *            The right hand side of the operator.
     * @return The {@link NaryExpression} {@value #MULTIPLY} expression.
     */
    public static NaryExpression multiply(final Expression lhs,
            final Expression rhs) {
        return new NaryExpression(MULTIPLY, lhs, rhs);
    }

    /**
     * Returns a {@link NaryExpression} {@value #NOT_EQUAL} expression.
     * 
     * @param lhs
     *            The left hand side of the comparison.
     * @param rhs
     *            The right hand side of the comparison.
     * @return The {@link NaryExpression} {@value #NOT_EQUAL} expression.
     */
    public static NaryExpression ne(final Expression lhs, final Expression rhs) {
        return new NaryExpression(NOT_EQUAL, lhs, rhs);
    }

    /**
     * Returns a {@link UnaryExpression} {@value #NOT} expression.
     * 
     * @param expression
     *            The sub expressions for the $not.
     * @return The {@link UnaryExpression} {@value #NOT} expression.
     */
    public static UnaryExpression not(final Expression expression) {
        return new UnaryExpression(NOT, expression);
    }

    /**
     * Returns a <code>null</code> {@link Constant} expression.
     * 
     * @return The {@link Constant} expression.
     */
    public static Constant nullConstant() {
        return new Constant(new NullElement(""));
    }

    /**
     * Returns an {@link NaryExpression} {@value #OR} expression.
     * 
     * @param expressions
     *            The sub-expressions.
     * @return The {@link NaryExpression} {@value #OR} expression.
     */
    public static NaryExpression or(final Expression... expressions) {
        return new NaryExpression(OR, expressions);
    }

    /**
     * Returns a {@link UnaryExpression} {@value #SECOND} expression.
     * 
     * @param expression
     *            The date for the operator.
     * @return The {@link UnaryExpression} {@value #SECOND} expression.
     */
    public static UnaryExpression second(final Expression expression) {
        return new UnaryExpression(SECOND, expression);
    }

    /**
     * Returns an element to set the value to.
     * 
     * @param name
     *            The name of the field to set.
     * @param document
     *            The document to set the document to.
     * @return The Element to set the value to the document.
     */
    public static Element set(final String name,
            final DocumentAssignable document) {
        return new DocumentElement(name, document.asDocument());
    }

    /**
     * Returns an element to set the value to.
     * 
     * @param name
     *            The name of the field to set.
     * @param expression
     *            The expression to compute the value for the field.
     * @return The Element to set the value to the expression.
     */
    public static Element set(final String name, final Expression expression) {
        return expression.toElement(name);
    }

    /**
     * Returns a {@link NaryExpression} {@value #SET_DIFFERENCE} expression.
     * 
     * @param lhs
     *            The expression that will be evaluated to create the first set.
     * @param rhs
     *            The expression that will be evaluated to create the second
     *            set.
     * @return The {@link NaryExpression} {@value #SET_DIFFERENCE} expression.
     */
    public static NaryExpression setDifference(final Expression lhs,
            final Expression rhs) {
        return new NaryExpression(SET_DIFFERENCE, lhs, rhs);
    }

    /**
     * Returns a {@link NaryExpression} {@value #SET_EQUALS} expression.
     * 
     * @param lhs
     *            The expression that will be evaluated to create the first set.
     * @param rhs
     *            The expression that will be evaluated to create the second
     *            set.
     * @return The {@link NaryExpression} {@value #SET_EQUALS} expression.
     */
    public static NaryExpression setEquals(final Expression lhs,
            final Expression rhs) {
        return new NaryExpression(SET_EQUALS, lhs, rhs);
    }

    /**
     * Returns a {@link NaryExpression} {@value #SET_INTERSECTION} expression.
     * 
     * @param lhs
     *            The expression that will be evaluated to create the first set.
     * @param rhs
     *            The expression that will be evaluated to create the second
     *            set.
     * @return The {@link NaryExpression} {@value #SET_INTERSECTION} expression.
     */
    public static NaryExpression setIntersection(final Expression lhs,
            final Expression rhs) {
        return new NaryExpression(SET_INTERSECTION, lhs, rhs);
    }

    /**
     * Returns a {@link NaryExpression} {@value #SET_IS_SUBSET} expression.
     * 
     * @param subSet
     *            The expression that will be tested to see if it is a subset of
     *            the {@code completeSet}.
     * @param completeSet
     *            The expression that will be evaluated to construct the
     *            complete set of values.
     * @return The {@link NaryExpression} {@value #SET_IS_SUBSET} expression.
     */
    public static NaryExpression setIsSubset(final Expression subSet,
            final Expression completeSet) {
        return new NaryExpression(SET_IS_SUBSET, subSet, completeSet);
    }

    /**
     * Returns a {@link NaryExpression} {@value #SET_UNION} expression.
     * 
     * @param lhs
     *            The expression that will be evaluated to create the first set.
     * @param rhs
     *            The expression that will be evaluated to create the second
     *            set.
     * @return The {@link NaryExpression} {@value #SET_UNION} expression.
     */
    public static NaryExpression setUnion(final Expression lhs,
            final Expression rhs) {
        return new NaryExpression(SET_UNION, lhs, rhs);
    }

    /**
     * Returns a {@link UnaryExpression} {@value #SIZE} expression.
     * 
     * @param expression
     *            The expression that will be evaluated to create the set to
     *            inspect for a true element.
     * @return The {@link UnaryExpression} {@value #SIZE} expression.
     */
    public static UnaryExpression size(final Expression expression) {
        return new UnaryExpression(SIZE, expression);
    }

    /**
     * Returns a {@link NaryExpression}
     * {@value #STRING_CASE_INSENSITIVE_COMPARE} expression.
     * 
     * @param lhs
     *            The left hand side of the comparison.
     * @param rhs
     *            The right hand side of the comparison.
     * @return The {@link NaryExpression}
     *         {@value #STRING_CASE_INSENSITIVE_COMPARE} expression.
     */
    public static NaryExpression strcasecmp(final Expression lhs,
            final Expression rhs) {
        return new NaryExpression(STRING_CASE_INSENSITIVE_COMPARE, lhs, rhs);
    }

    /**
     * Returns a {@link NaryExpression} {@value #SUB_STRING} expression.
     * 
     * @param string
     *            The string to pull a sub-string from.
     * @param skip
     *            The number of characters to skip in the string.
     * @param length
     *            The length of the string to extract.
     * @return The {@link NaryExpression} {@value #SUB_STRING} expression.
     */
    public static NaryExpression substr(final Expression string,
            final Expression skip, final Expression length) {
        return new NaryExpression(SUB_STRING, string, skip, length);
    }

    /**
     * Returns a {@link NaryExpression} {@value #SUBTRACT} expression.
     * 
     * @param lhs
     *            The left hand side of the operator.
     * @param rhs
     *            The right hand side of the operator.
     * @return The {@link NaryExpression} {@value #SUBTRACT} expression.
     */
    public static NaryExpression subtract(final Expression lhs,
            final Expression rhs) {
        return new NaryExpression(SUBTRACT, lhs, rhs);
    }

    /**
     * Returns a {@link UnaryExpression} {@value #TO_LOWER} expression.
     * 
     * @param string
     *            The string to modify.
     * @return The {@link UnaryExpression} {@value #TO_LOWER} expression.
     */
    public static UnaryExpression toLower(final Expression string) {
        return new UnaryExpression(TO_LOWER, string);
    }

    /**
     * Returns a {@link UnaryExpression} {@value #TO_UPPER} expression.
     * 
     * @param string
     *            The string to modify.
     * @return The {@link UnaryExpression} {@value #TO_UPPER} expression.
     */
    public static UnaryExpression toUpper(final Expression string) {
        return new UnaryExpression(TO_UPPER, string);
    }

    /**
     * Returns a {@link UnaryExpression} {@value #WEEK} expression.
     * 
     * @param expression
     *            The date for the operator.
     * @return The {@link UnaryExpression} {@value #WEEK} expression.
     */
    public static UnaryExpression week(final Expression expression) {
        return new UnaryExpression(WEEK, expression);
    }

    /**
     * Returns a {@link UnaryExpression} {@value #YEAR} expression.
     * 
     * @param expression
     *            The date for the operator.
     * @return The {@link UnaryExpression} {@value #YEAR} expression.
     */
    public static UnaryExpression year(final Expression expression) {
        return new UnaryExpression(YEAR, expression);
    }

    /**
     * Creates a new Expressions.
     */
    private Expressions() {
    }
}
