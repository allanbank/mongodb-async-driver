/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * QueryBuilder provides support for constructing queries. Most users are
 * expected to use the static methods of this class to create query
 * {@link Document}s.
 * <p>
 * As an example:<blockquote>
 * 
 * <pre>
 * <code>
 * 
 * import static {@link com.allanbank.mongodb.builder.QueryBuilder#and com.allanbank.mongodb.builder.QueryBuilder.and}
 * import static {@link com.allanbank.mongodb.builder.QueryBuilder#or com.allanbank.mongodb.builder.QueryBuilder.or}
 * import static {@link com.allanbank.mongodb.builder.QueryBuilder#not com.allanbank.mongodb.builder.QueryBuilder.not}
 * import static {@link com.allanbank.mongodb.builder.QueryBuilder#where com.allanbank.mongodb.builder.QueryBuilder.where}
 * 
 * Document query = 
 *           or( 
 *              where("f").greaterThan(23).lessThan(42).and("g").lessThan(3),
 *              and( 
 *                where("f").greaterThanOrEqualTo(42),
 *                not( where("g").lessThan(3) ) 
 *              )
 *           );
 * </code>
 * </pre>
 * 
 * </blockquote>
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class QueryBuilder implements DocumentAssignable {

    /**
     * Creates a single document that is the conjunction of the criteria
     * provided.
     * 
     * @param criteria
     *            The criteria to create a conjunction of.
     * @return The conjunction Document.
     */
    public static Document and(final DocumentAssignable... criteria) {
        if (criteria.length <= 0) {
            return BuilderFactory.start().build();
        }
        else if (criteria.length == 1) {
            return criteria[0].asDocument();
        }
        else {
            // Perform 2 things at once.
            // 1) Build the $and document.
            // 2) Build a flat document to optimize the $and away if none of
            // the nested elements collide.
            final Set<String> seen = new HashSet<String>();
            DocumentBuilder optimized = BuilderFactory.start();
            final DocumentBuilder docBuilder = BuilderFactory.start();
            final ArrayBuilder arrayBuilder = docBuilder
                    .pushArray(LogicalOperator.AND.getToken());

            for (final DocumentAssignable criterion : criteria) {
                final Document subQuery = criterion.asDocument();
                // Make sure at least 1 element.
                final Iterator<Element> iter = subQuery.iterator();
                if (iter.hasNext()) {
                    arrayBuilder.addDocument(subQuery);

                    while ((optimized != null) && iter.hasNext()) {
                        final Element subQueryElement = iter.next();
                        if (seen.add(subQueryElement.getName())) {
                            optimized.add(subQueryElement);
                        }
                        else {
                            optimized = null;
                        }
                    }
                }
            }

            if (optimized != null) {
                return optimized.build();
            }
            return docBuilder.build();
        }
    }

    /**
     * Creates a single document that is the disjunction of the criteria
     * provided.
     * 
     * @param criteria
     *            The criteria to create a disjunction of.
     * @return The disjunction Document.
     */
    public static Document nor(final DocumentAssignable... criteria) {

        final DocumentBuilder docBuilder = BuilderFactory.start();
        final ArrayBuilder arrayBuilder = docBuilder
                .pushArray(LogicalOperator.NOR.getToken());

        for (final DocumentAssignable criterion : criteria) {
            final Document subQuery = criterion.asDocument();
            if (subQuery.iterator().hasNext()) {
                arrayBuilder.addDocument(subQuery);
            }
        }

        return docBuilder.build();
    }

    /**
     * Negate a set of criteria.
     * 
     * @param criteria
     *            The criteria to negate. These will normally be
     *            {@link ConditionBuilder}s or {@link Document}s.
     * @return The negated criteria.
     */
    public static Document not(final DocumentAssignable... criteria) {
        final DocumentBuilder docBuilder = BuilderFactory.start();
        final ArrayBuilder arrayBuilder = docBuilder
                .pushArray(LogicalOperator.NOT.getToken());

        for (final DocumentAssignable criterion : criteria) {
            final Document subQuery = criterion.asDocument();
            if (subQuery.iterator().hasNext()) {
                arrayBuilder.addDocument(subQuery);
            }
        }

        return docBuilder.build();
    }

    /**
     * Creates a single document that is the disjunction of the criteria
     * provided.
     * 
     * @param criteria
     *            The criteria to create a disjunction of.
     * @return The disjunction Document.
     */
    public static Document or(final DocumentAssignable... criteria) {
        if (criteria.length <= 0) {
            return BuilderFactory.start().build();
        }
        else if (criteria.length == 1) {
            return criteria[0].asDocument();
        }
        else {
            final DocumentBuilder docBuilder = BuilderFactory.start();
            final ArrayBuilder arrayBuilder = docBuilder
                    .pushArray(LogicalOperator.OR.getToken());

            for (final DocumentAssignable criterion : criteria) {
                final Document subQuery = criterion.asDocument();
                if (subQuery.iterator().hasNext()) {
                    arrayBuilder.addDocument(subQuery);
                }
            }

            return docBuilder.build();
        }
    }

    /**
     * Start a criteria for a single conjunctions.
     * 
     * @param field
     *            The field to start the criteria against.
     * @return A {@link ConditionBuilder} for constructing the conditions.
     */
    public static ConditionBuilder where(final String field) {
        return new QueryBuilder().whereField(field);
    }

    /** The set of conditions created for the query. */
    private final Map<String, ConditionBuilder> myConditions;

    /** The ad-hoc JavaScript condition. */
    private String myWhere;

    /**
     * Creates a new QueryBuilder.
     */
    public QueryBuilder() {
        myConditions = new LinkedHashMap<String, ConditionBuilder>();
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
     * Construct the final query document.
     * 
     * @return The document containing the constraints specified.
     */
    public Document build() {
        final DocumentBuilder builder = BuilderFactory.start();
        for (final ConditionBuilder condBuilder : myConditions.values()) {
            final Element condElement = condBuilder.buildFieldCondition();

            if (condElement != null) {
                builder.add(condElement);
            }
        }

        if (myWhere != null) {
            builder.addJavaScript(MiscellaneousOperator.WHERE.getToken(),
                    myWhere);
        }

        return builder.build();
    }

    /**
     * Clears the builder's conditions.
     */
    public void reset() {
        myConditions.clear();
    }

    /**
     * Returns a builder for the constraints on a single field.
     * 
     * @param fieldName
     *            The name of the field to constrain.
     * @return A {@link ConditionBuilder} for creation of the conditions of the
     *         field.
     */
    public ConditionBuilder whereField(final String fieldName) {
        ConditionBuilder builder = myConditions.get(fieldName);
        if (builder == null) {
            builder = new ConditionBuilder(fieldName, this);
            myConditions.put(fieldName, builder);
        }
        return builder;
    }

    /**
     * Adds an ad-hoc JavaScript condition to the query.
     * 
     * @param javaScript
     *            The javaScript condition to add.
     * @return This builder for call chaining.
     */
    public QueryBuilder whereJavaScript(final String javaScript) {
        myWhere = javaScript;

        return this;
    }
}
