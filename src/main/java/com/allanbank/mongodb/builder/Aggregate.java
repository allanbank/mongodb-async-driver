/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.builder.expression.Expressions;

/**
 * Aggregate provides support for the <tt>aggregate</tt> command supporting a
 * pipeline of commands to execute.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Aggregate {

    /** The pipeline of operations to be applied. */
    private final List<Element> myPipeline;

    /**
     * Creates a new Aggregate.
     * 
     * @param builder
     *            The builder for the Aggregate instance.
     */
    protected Aggregate(final Builder builder) {
        myPipeline = Collections.unmodifiableList(Arrays
                .asList(builder.myPipeline.build()));
    }

    /**
     * Returns the pipeline of operations to apply.
     * 
     * @return The pipeline of operations to apply.
     */
    public List<Element> getPipeline() {
        return myPipeline;
    }

    /**
     * Builder provides the ability to construct aggregate command pipelines.
     * <p>
     * Methods are provided for all existing pipeline operators and generic
     * {@link #step(String, DocumentAssignable)} methods are provided to support
     * future pipeline operators while in development or before the driver is
     * updated.
     * </p>
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static class Builder {

        /** The pipeline of operations to be applied. */
        protected final ArrayBuilder myPipeline;

        /**
         * Creates a new Builder.
         */
        public Builder() {
            myPipeline = BuilderFactory.startArray();
        }

        /**
         * Constructs a new {@link Aggregate} object from the state of the
         * builder.
         * 
         * @return The new {@link Aggregate} object.
         */
        public Aggregate build() {
            return new Aggregate(this);
        }

        /**
         * Adds a <tt>$group</tt> operation to the pipeline to aggregate
         * documents passing this point in the pipeline into a group of
         * documents.
         * <p>
         * This method is intended to construct groups with simple dynamic or
         * static id documents.
         * </p>
         * <blockquote>
         * 
         * <pre>
         * <code>
         * import static {@link AggregationGroupId#id com.allanbank.mongodb.builder.AggregationGroupId.id};
         * import static {@link AggregationGroupField#set com.allanbank.mongodb.builder.AggregationGroupField.set};
         * 
         * {@link Aggregate.Builder} builder = new Aggregate.Builder();
         * builder.group(
         *           id("$field1"),
         *           set("resultField1").uniqueValuesOf("$field2"),
         *           set("resultField2").max("$field3"),
         *           set("sum").sum("$field4") );
         * </code>
         * </pre>
         * 
         * </blockquote>
         * 
         * @param id
         *            The builder for the <tt>_id</tt> field to specify unique
         *            groups.
         * @param aggregations
         *            The specification for the group id and what fields to
         *            aggregate in the form of a document.
         * @return This builder for chaining method calls.
         */
        public Builder group(final AggregationGroupId id,
                final AggregationGroupField... aggregations) {

            final Element[] elements = new Element[aggregations.length + 1];
            elements[0] = id.toElement();
            for (int i = 0; i < aggregations.length; ++i) {
                elements[i + 1] = aggregations[i].toElement();
            }

            return step("$group", elements);
        }

        /**
         * Adds a <tt>$group</tt> operation to the pipeline to aggregate
         * documents passing this point in the pipeline into a group of
         * documents.
         * <p>
         * This method is intended to construct groups with complex dynamic or
         * static id documents. The {@link AggregationGroupId.Builder}
         * implements the {@link DocumentBuilder} for construction of arbitrary
         * complex _id documents.
         * </p>
         * <blockquote>
         * 
         * <pre>
         * <code>
         * import static {@link AggregationGroupId#id com.allanbank.mongodb.builder.AggregationGroupId.id};
         * import static {@link AggregationGroupField#set com.allanbank.mongodb.builder.AggregationGroupField.set};
         * 
         * {@link Aggregate.Builder} builder = new Aggregate.Builder();
         * builder.group(
         *           id().addField("$field1").addField("$field2"),
         *           set("resultField1").uniqueValuesOf("$field3"),
         *           set("resultField2").first("$field4"),
         *           set("count").count() );
         * </code>
         * </pre>
         * 
         * </blockquote>
         * 
         * @param id
         *            The builder for the <tt>_id</tt> field to specify unique
         *            groups.
         * @param aggregations
         *            The specification for the group id and what fields to
         *            aggregate in the form of a document.
         * @return This builder for chaining method calls.
         */
        public Builder group(final AggregationGroupId.Builder id,
                final AggregationGroupField... aggregations) {
            return group(id.buildId(), aggregations);
        }

        /**
         * Adds a <tt>$group</tt> operation to the pipeline to aggregate
         * documents passing this point in the pipeline into a group of
         * documents.
         * 
         * @param aggregations
         *            The specification for the group id and what fields to
         *            aggregate in the form of a document.
         * @return This builder for chaining method calls.
         */
        public Builder group(final DocumentAssignable aggregations) {
            return step("$group", aggregations);
        }

        /**
         * Adds a <tt>$group</tt> operation to the pipeline to aggregate
         * documents passing this point in the pipeline into a group of
         * documents.
         * <p>
         * This method is intended to construct groups with complex dynamic or
         * static id documents. The {@link AggregationGroupId.Builder}
         * implements the {@link DocumentBuilder} for construction of arbitrary
         * complex _id documents.
         * </p>
         * <blockquote>
         * 
         * <pre>
         * <code>
         * import static {@link AggregationGroupId#id com.allanbank.mongodb.builder.AggregationGroupId.id};
         * import static {@link AggregationGroupField#set com.allanbank.mongodb.builder.AggregationGroupField.set};
         * 
         * {@link Aggregate.Builder} builder = new Aggregate.Builder();
         * builder.group(
         *           id().addInteger("i", 1),
         *           set("resultField1").uniqueValuesOf("$field3"),
         *           set("resultField2").first("$field4"),
         *           set("count").count() );
         * </code>
         * </pre>
         * 
         * </blockquote>
         * 
         * @param id
         *            The builder for the <tt>_id</tt> field to specify unique
         *            groups.
         * @param aggregations
         *            The specification for the group id and what fields to
         *            aggregate in the form of a document.
         * @return This builder for chaining method calls.
         */
        public Builder group(final DocumentAssignable id,
                final AggregationGroupField... aggregations) {
            return group(new AggregationGroupId(id), aggregations);
        }

        /**
         * Adds a <tt>$limit</tt> operation to the pipeline to stop producing
         * documents passing this point in the pipeline once the limit of
         * documents is reached.
         * 
         * @param numberOfDocuments
         *            The number of documents to allow past this point in the
         *            pipeline.
         * @return This builder for chaining method calls.
         * 
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/aggregation/#_S_limit">Aggregation
         *      Framework Operators - $limit</a>
         */
        public Builder limit(final int numberOfDocuments) {
            return step("$limit", numberOfDocuments);
        }

        /**
         * Adds a <tt>$limit</tt> operation to the pipeline to stop producing
         * documents passing this point in the pipeline once the limit of
         * documents is reached.
         * 
         * @param numberOfDocuments
         *            The number of documents to allow past this point in the
         *            pipeline.
         * @return This builder for chaining method calls.
         * 
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/aggregation/#_S_limit">
         *      Aggregation Framework Operators - $limit</a>
         */
        public Builder limit(final long numberOfDocuments) {
            return step("$limit", numberOfDocuments);
        }

        /**
         * Adds a <tt>$match</tt> operation to the pipeline to filter documents
         * passing this point in the pipeline.
         * <p>
         * This method may be used with the {@link QueryBuilder} to easily
         * specify the criteria to match against. <blockquote>
         * 
         * <pre>
         * <code>
         * import static {@link QueryBuilder#where com.allanbank.mongodb.builder.QueryBuilder.where}
         * 
         * Aggregate.Builder builder = new Aggregate.Builder();
         * 
         * builder.match( where("f").greaterThan(23).lessThan(42).and("g").lessThan(3) );
         * ...
         * </code>
         * </pre>
         * 
         * </blockquote>
         * </p>
         * 
         * @param query
         *            The query to match documents against.
         * @return This builder for chaining method calls.
         * 
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/aggregation/#_S_match">
         *      Aggregation Framework Operators - $match</a>
         */
        public Builder match(final DocumentAssignable query) {
            return step("$match", query);
        }

        /**
         * Adds a <tt>$project</tt> operation to the pipeline to create a
         * projection of the documents passing this point in the pipeline.
         * <p>
         * This method is intended to be used with the
         * {@link AggregationProjectFields} and
         * {@link com.allanbank.mongodb.builder.expression.Expressions
         * Expressions} static helper methods.
         * <p>
         * <blockquote>
         * 
         * <pre>
         * <code>
         * import static {@link AggregationProjectFields#include com.allanbank.mongodb.builder.AggregationProjectFields.include};
         * import static {@link com.allanbank.mongodb.builder.expression.Expressions com.allanbank.mongodb.builder.expression.Expressions.*};
         * 
         * 
         * Aggregate.Builder builder = new Aggregate.Builder();
         * ...
         * builder.project(
         *         include("chr", "begin", "end", "calledPloidy"),
         *         set("window",
         *             multiply(
         *                 divide(
         *                     subtract(
         *                         field("begin"), 
         *                         mod(field("begin"), constant(interval))),
         *                     constant(interval)), 
         *                 constant(interval))));
         * ...
         * </code>
         * </pre>
         * 
         * </blockquote>
         * 
         * @param fields
         *            The fields to copy into the projected results.
         * @param elements
         *            The computed elements based on {@link Expressions}.
         * @return This builder for chaining method calls.
         */
        public Builder project(final AggregationProjectFields fields,
                final Element... elements) {
            final List<IntegerElement> fieldElements = fields.toElements();

            final List<Element> allElements = new ArrayList<Element>(
                    fieldElements.size() + elements.length);
            allElements.addAll(fieldElements);
            allElements.addAll(Arrays.asList(elements));

            return step("$project", allElements);
        }

        /**
         * Adds a <tt>$project</tt> operation to the pipeline to create a
         * projection of the documents passing this point in the pipeline.
         * 
         * @param projection
         *            The specification for the projection to perform.
         * @return This builder for chaining method calls.
         */
        public Builder project(final DocumentAssignable projection) {
            return step("$project", projection);
        }

        /**
         * Resets the builder back to an empty pipeline.
         * 
         * @return This builder for chaining method calls.
         */
        public Builder reset() {
            myPipeline.reset();
            return this;
        }

        /**
         * Adds a <tt>$skip</tt> operation to the pipeline to skip the specified
         * number of documents before allowing any document past this point in
         * the pipeline.
         * 
         * @param numberOfDocuments
         *            The number of documents to skip past before allowing any
         *            documents to pass this point in the pipeline.
         * @return This builder for chaining method calls.
         * 
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/aggregation/#_S_skip">
         *      Aggregation Framework Operators - $skip</a>
         */
        public Builder skip(final int numberOfDocuments) {
            return step("$skip", numberOfDocuments);
        }

        /**
         * Adds a <tt>$skip</tt> operation to the pipeline to skip the specified
         * number of documents before allowing any document past this point in
         * the pipeline.
         * 
         * @param numberOfDocuments
         *            The number of documents to skip past before allowing any
         *            documents to pass this point in the pipeline.
         * @return This builder for chaining method calls.
         * 
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/aggregation/#_S_skip">
         *      Aggregation Framework Operators - $skip</a>
         */
        public Builder skip(final long numberOfDocuments) {
            return step("$skip", numberOfDocuments);
        }

        /**
         * Adds a <tt>$sort</tt> operation to sort the documents passing this
         * point based on the sort specification provided.
         * <p>
         * This method is intended to be used with the {@link Sort} class's
         * static methods: <blockquote>
         * 
         * <pre>
         * <code>
         * import static {@link Sort#asc(String) com.allanbank.mongodb.builder.Sort.asc};
         * import static {@link Sort#desc(String) com.allanbank.mongodb.builder.Sort.desc};
         * 
         * Aggregate.Builder builder = new Aggregate.Builder();
         * 
         * builder.setSort( asc("f"), desc("g") );
         * ...
         * </code>
         * </pre>
         * 
         * </blockquote>
         * 
         * @param sortFields
         *            The sort fields to use.
         * @return This builder for chaining method calls.
         * 
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/aggregation/#_S_sort">
         *      Aggregation Framework Operators - $sort</a>
         */
        public Builder sort(final IntegerElement... sortFields) {
            return step("$sort", sortFields);
        }

        /**
         * Adds a <tt>$sort</tt> operation to sort the documents passing this
         * point based on the sort fields provides in ascending order.
         * 
         * @param sortFields
         *            The sort fields to use in ascending order.
         * @return This builder for chaining method calls.
         * 
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/aggregation/#_S_sort">
         *      Aggregation Framework Operators - $sort</a>
         */
        public Builder sort(final String... sortFields) {
            final IntegerElement[] elements = new IntegerElement[sortFields.length];
            for (int i = 0; i < sortFields.length; ++i) {
                elements[i] = Sort.asc(sortFields[i]);
            }
            return sort(elements);
        }

        /**
         * Adds a generic step to the builder's pipeline.
         * 
         * @param operator
         *            The operator to add to the pipeline.
         * @param stepDocument
         *            The document containing the details of the step to apply.
         * @return This builder for chaining method calls.
         */
        public Builder step(final String operator,
                final DocumentAssignable stepDocument) {
            myPipeline.push().addDocument(operator, stepDocument.asDocument());
            return this;
        }

        /**
         * Adds a generic step to the builder's pipeline.
         * 
         * @param operator
         *            The operator to add to the pipeline.
         * @param value
         *            The value for the operator.
         * @return This builder for chaining method calls.
         */
        public Builder step(final String operator, final double value) {
            myPipeline.push().addDouble(operator, value);
            return this;
        }

        /**
         * Adds a generic step to the builder's pipeline.
         * 
         * @param operator
         *            The operator to add to the pipeline.
         * @param elements
         *            The elements containing the details of the step to apply.
         * @return This builder for chaining method calls.
         */
        public Builder step(final String operator, final Element... elements) {
            return step(operator, Arrays.asList(elements));
        }

        /**
         * Adds a generic step to the builder's pipeline.
         * 
         * @param operator
         *            The operator to add to the pipeline.
         * @param value
         *            The value for the operator.
         * @return This builder for chaining method calls.
         */
        public Builder step(final String operator, final int value) {
            myPipeline.push().addInteger(operator, value);
            return this;
        }

        /**
         * Adds a generic step to the builder's pipeline.
         * 
         * @param operator
         *            The operator to add to the pipeline.
         * @param elements
         *            The elements containing the details of the step to apply.
         * @return This builder for chaining method calls.
         */
        public Builder step(final String operator, final List<Element> elements) {
            final DocumentBuilder operatorBuilder = myPipeline.push().push(
                    operator);
            for (final Element element : elements) {
                operatorBuilder.add(element);
            }
            return this;
        }

        /**
         * Adds a generic step to the builder's pipeline.
         * 
         * @param operator
         *            The operator to add to the pipeline.
         * @param value
         *            The value for the operator.
         * @return This builder for chaining method calls.
         */
        public Builder step(final String operator, final long value) {
            myPipeline.push().addLong(operator, value);
            return this;
        }

        /**
         * Adds a generic step to the builder's pipeline.
         * 
         * @param operator
         *            The operator to add to the pipeline.
         * @param value
         *            The value for the operator.
         * @return This builder for chaining method calls.
         */
        public Builder step(final String operator, final String value) {
            myPipeline.push().addString(operator, value);
            return this;
        }

        /**
         * Adds a <tt>$unwind</tt> operation generate a document for each
         * element of the specified array field with the array replaced with the
         * value of the element.
         * 
         * @param fieldName
         *            The name of the array field within the document to unwind.
         *            This name must start with a '$'. If it does not a '$' will
         *            be prepended to the field name..
         * @return This builder for chaining method calls.
         * 
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/aggregation/#_S_unwind">
         *      Aggregation Framework Operators - $unwind</a>
         */
        public Builder unwind(final String fieldName) {
            if (fieldName.startsWith("$")) {
                step("$unwind", fieldName);
            }
            else {
                step("$unwind", "$" + fieldName);
            }
            return this;
        }
    }
}
