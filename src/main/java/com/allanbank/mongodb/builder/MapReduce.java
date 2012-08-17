/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.IntegerElement;

/**
 * Represents the state of a single {@link MongoCollection#mapReduce} command.
 * Objects of this class are created using the nested {@link Builder}.
 * 
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MapReduce {

    /**
     * The finalize function to apply to the final results of the reduce
     * function.
     */
    private final String myFinalizeFunction;

    /**
     * If true limits the translation of the documents to an from
     * BSON/JavaScript.
     */
    private final boolean myJsMode;

    /**
     * If true then the temporary collections created during the map/reduce
     * should not be dropped.
     */
    private final boolean myKeepTemp;

    /** Limits the number of objects to be used as input to the map/reduce. */
    private final int myLimit;

    /** The map functions to apply to each selected document. */
    private final String myMapFunction;

    /**
     * The name of the output database if the output type is One of
     * {@link OutputType#REPLACE}, {@link OutputType#MERGE}, or
     * {@link OutputType#REDUCE}.
     */
    private final String myOutputDatabase;

    /**
     * The name of the output collection if the output type is One of
     * {@link OutputType#REPLACE}, {@link OutputType#MERGE}, or
     * {@link OutputType#REDUCE}.
     */
    private final String myOutputName;

    /** The handling for the output of the map/reduce. */
    private final OutputType myOutputType;

    /** The query to select the document to run the map/reduce against. */
    private final Document myQuery;

    /** The reduce function to apply to the emitted output of the map function. */
    private final String myReduceFunction;

    /** The scoped values to expose to the map/reduce/finalize functions. */
    private final Document myScope;

    /**
     * The sort to apply to the input objects. Useful for optimization, like
     * sorting by the emit key for fewer reduces.
     */
    private final Document mySort;

    /** If true emits progress messages in the server logs. */
    private final boolean myVerbose;

    /**
     * Create a new MapReduce.
     * 
     * @param builder
     *            The builder to copy state from.
     */
    protected MapReduce(final Builder builder) {
        assert (builder.myMapFunction != null) : "A mapReduce must have a map function.";
        assert (builder.myReduceFunction != null) : "A mapReduce must have a reduce function.";

        assert ((builder.myOutputType == OutputType.INLINE) || (builder.myOutputName != null)) : "A mapReduce output type must be INLINE or an output collection must be specified.";

        myMapFunction = builder.myMapFunction;
        myReduceFunction = builder.myReduceFunction;
        myFinalizeFunction = builder.myFinalizeFunction;
        myQuery = builder.myQuery;
        mySort = builder.mySort;
        myScope = builder.myScope;
        myLimit = builder.myLimit;
        myOutputName = builder.myOutputName;
        myOutputDatabase = builder.myOutputDatabase;
        myOutputType = builder.myOutputType;
        myKeepTemp = builder.myKeepTemp;
        myJsMode = builder.myJsMode;
        myVerbose = builder.myVerbose;
    }

    /**
     * Returns the finalize function to apply to the final results of the reduce
     * function.
     * 
     * @return The finalize function to apply to the final results of the reduce
     *         function.
     */
    public String getFinalizeFunction() {
        return myFinalizeFunction;
    }

    /**
     * Returns the limit for the number of objects to be used as input to the
     * map/reduce.
     * 
     * @return The limit for the number of objects to be used as input to the
     *         map/reduce.
     */
    public int getLimit() {
        return myLimit;
    }

    /**
     * Returns the map functions to apply to each selected document.
     * 
     * @return The map functions to apply to each selected document.
     */
    public String getMapFunction() {
        return myMapFunction;
    }

    /**
     * Returns the name of the output database if the output type is One of
     * {@link OutputType#REPLACE}, {@link OutputType#MERGE}, or
     * {@link OutputType#REDUCE}.
     * 
     * @return The name of the output database if the output type is One of
     *         {@link OutputType#REPLACE}, {@link OutputType#MERGE}, or
     *         {@link OutputType#REDUCE}.
     */
    public String getOutputDatabase() {
        return myOutputDatabase;
    }

    /**
     * Returns the name of the output collection if the output type is One of
     * {@link OutputType#REPLACE}, {@link OutputType#MERGE}, or
     * {@link OutputType#REDUCE}.
     * 
     * @return The name of the output collection if the output type is One of
     *         {@link OutputType#REPLACE}, {@link OutputType#MERGE}, or
     *         {@link OutputType#REDUCE}.
     */
    public String getOutputName() {
        return myOutputName;
    }

    /**
     * Returns the handling for the output of the map/reduce.
     * 
     * @return The handling for the output of the map/reduce.
     */
    public OutputType getOutputType() {
        return myOutputType;
    }

    /**
     * Returns the query to select the documents to run the map/reduce against.
     * 
     * @return The query to select the documents to run the map/reduce against.
     */
    public Document getQuery() {
        return myQuery;
    }

    /**
     * Returns the reduce function to apply to the emitted output of the map
     * function.
     * 
     * @return The reduce function to apply to the emitted output of the map
     *         function.
     */
    public String getReduceFunction() {
        return myReduceFunction;
    }

    /**
     * Returns the scoped values to expose to the map/reduce/finalize functions.
     * 
     * @return The scoped values to expose to the map/reduce/finalize functions.
     */
    public Document getScope() {
        return myScope;
    }

    /**
     * Returns the sort to apply to the input objects. Useful for optimization,
     * like sorting by the emit key for fewer reduces.
     * 
     * @return The sort to apply to the input objects. Useful for optimization,
     *         like sorting by the emit key for fewer reduces.
     */
    public Document getSort() {
        return mySort;
    }

    /**
     * Returns true to limit the translation of the documents to an from
     * BSON/JavaScript.
     * 
     * @return True to limit the translation of the documents to an from
     *         BSON/JavaScript.
     */
    public boolean isJsMode() {
        return myJsMode;
    }

    /**
     * Returns true to drop the temporary collections created during the
     * map/reduce.
     * 
     * @return True to drop the temporary collections created during the
     *         map/reduce.
     */
    public boolean isKeepTemp() {
        return myKeepTemp;
    }

    /**
     * Returns true to emit progress messages in the server logs.
     * 
     * @return True to emit progress messages in the server logs.
     */
    public boolean isVerbose() {
        return myVerbose;
    }

    /**
     * Helper for creating immutable {@link MapReduce} commands.
     * 
     * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static class Builder {
        /**
         * The finalize function to apply to the final results of the reduce
         * function.
         */
        protected String myFinalizeFunction = null;

        /**
         * If true limits the translation of the documents to an from
         * BSON/JavaScript.
         */
        protected boolean myJsMode = false;

        /**
         * If true then the temporary collections created during the map/reduce
         * should not be dropped.
         */
        protected boolean myKeepTemp = false;

        /** Limits the number of objects to be used as input to the map/reduce. */
        protected int myLimit = 0;

        /** The map functions to apply to each selected document. */
        protected String myMapFunction = null;

        /**
         * The name of the output database if the output type is One of
         * {@link OutputType#REPLACE}, {@link OutputType#MERGE}, or
         * {@link OutputType#REDUCE}.
         */
        protected String myOutputDatabase = null;

        /**
         * The name of the output collection if the output type is One of
         * {@link OutputType#REPLACE}, {@link OutputType#MERGE}, or
         * {@link OutputType#REDUCE}.
         */
        protected String myOutputName = null;

        /** The handling for the output of the map/reduce. */
        protected OutputType myOutputType = OutputType.INLINE;

        /** The query to select the document to run the map/reduce against. */
        protected Document myQuery = null;

        /**
         * The reduce function to apply to the emitted output of the map
         * function.
         */
        protected String myReduceFunction = null;

        /** The scoped values to expose to the map/reduce/finalize functions. */
        protected Document myScope = null;

        /**
         * The sort to apply to the input objects. Useful for optimization, like
         * sorting by the emit key for fewer reduces.
         */
        protected Document mySort = null;

        /** If true emits progress messages in the server logs. */
        protected boolean myVerbose = false;

        /**
         * Constructs a new {@link FindAndModify} object from the state of the
         * builder.
         * 
         * @return The new {@link FindAndModify} object.
         */
        public MapReduce build() {
            return new MapReduce(this);
        }

        /**
         * Sets the finalize function to apply to the final results of the
         * reduce function.
         * 
         * @param finalize
         *            The finalize function to apply to the final results of the
         *            reduce function.
         * @return This builder for chaining method calls.
         */
        public Builder setFinalizeFunction(final String finalize) {
            myFinalizeFunction = finalize;
            return this;
        }

        /**
         * Sets to true to limit the translation of the documents to an from
         * BSON/JavaScript.
         * 
         * @param jsMode
         *            True to limit the translation of the documents to an from
         *            BSON/JavaScript.
         * @return This builder for chaining method calls.
         */
        public Builder setJsMode(final boolean jsMode) {
            myJsMode = jsMode;
            return this;
        }

        /**
         * Sets to true to drop the temporary collections created during the
         * map/reduce.
         * 
         * @param keepTemp
         *            True to drop the temporary collections created during the
         *            map/reduce.
         * @return This builder for chaining method calls.
         */
        public Builder setKeepTemp(final boolean keepTemp) {
            myKeepTemp = keepTemp;
            return this;
        }

        /**
         * Sets the limit for the number of objects to be used as input to the
         * map/reduce.
         * 
         * @param limit
         *            The limit for the number of objects to be used as input to
         *            the map/reduce.
         * @return This builder for chaining method calls.
         */
        public Builder setLimit(final int limit) {
            myLimit = limit;
            return this;
        }

        /**
         * Sets the map functions to apply to each selected document.
         * 
         * @param map
         *            The map functions to apply to each selected document.
         * @return This builder for chaining method calls.
         */
        public Builder setMapFunction(final String map) {
            myMapFunction = map;
            return this;
        }

        /**
         * Sets the name of the output database if the output type is One of
         * {@link OutputType#REPLACE}, {@link OutputType#MERGE}, or
         * {@link OutputType#REDUCE}.
         * 
         * @param outputDatabase
         *            The name of the output database if the output type is One
         *            of {@link OutputType#REPLACE}, {@link OutputType#MERGE},
         *            or {@link OutputType#REDUCE}.
         * @return This builder for chaining method calls.
         */
        public Builder setOutputDatabase(final String outputDatabase) {
            myOutputDatabase = outputDatabase;
            return this;
        }

        /**
         * Sets the name of the output collection if the output type is One of
         * {@link OutputType#REPLACE}, {@link OutputType#MERGE}, or
         * {@link OutputType#REDUCE}.
         * 
         * @param outputName
         *            The name of the output collection if the output type is
         *            One of {@link OutputType#REPLACE},
         *            {@link OutputType#MERGE}, or {@link OutputType#REDUCE}.
         * @return This builder for chaining method calls.
         */
        public Builder setOutputName(final String outputName) {
            myOutputName = outputName;
            return this;
        }

        /**
         * Sets the handling for the output of the map/reduce.
         * 
         * @param outputType
         *            The handling for the output of the map/reduce.
         * @return This builder for chaining method calls.
         */
        public Builder setOutputType(final OutputType outputType) {
            myOutputType = outputType;
            return this;
        }

        /**
         * Sets the query to select the documents to run the map/reduce against.
         * 
         * @param query
         *            The query to select the documents to run the map/reduce
         *            against.
         * @return This builder for chaining method calls.
         */
        public Builder setQuery(final DocumentAssignable query) {
            myQuery = query.asDocument();
            return this;
        }

        /**
         * Sets the reduce function to apply to the emitted output of the map
         * function.
         * 
         * @param reduce
         *            The reduce function to apply to the emitted output of the
         *            map function.
         * @return This builder for chaining method calls.
         */
        public Builder setReduceFunction(final String reduce) {
            myReduceFunction = reduce;
            return this;
        }

        /**
         * Sets the scoped values to expose to the map/reduce/finalize
         * functions.
         * 
         * @param scope
         *            The scoped values to expose to the map/reduce/finalize
         *            functions.
         * @return This builder for chaining method calls.
         */
        public Builder setScope(final DocumentAssignable scope) {
            myScope = scope.asDocument();
            return this;
        }

        /**
         * Sets the sort to apply to the input objects. Useful for optimization,
         * like sorting by the emit key for fewer reduces.
         * 
         * @param sort
         *            The sort to apply to the input objects. Useful for
         *            optimization, like sorting by the emit key for fewer
         *            reduces.
         * @return This builder for chaining method calls.
         */
        public Builder setSort(final DocumentAssignable sort) {
            mySort = sort.asDocument();
            return this;
        }

        /**
         * Sets the sort to apply to the input objects. Useful for optimization,
         * like sorting by the emit key for fewer reduces.
         * <p>
         * This method is intended to be used with the {@link Sort} class's
         * static methods: <blockquote>
         * 
         * <pre>
         * <code>
         * import static {@link Sort#asc(String) com.allanbank.mongodb.builder.Sort.asc};
         * import static {@link Sort#desc(String) com.allanbank.mongodb.builder.Sort.desc};
         * 
         * MapReduce.Builder builder = new Find.Builder();
         * 
         * builder.setSort( asc("f"), desc("g") );
         * ...
         * </code>
         * </pre>
         * 
         * </blockquote>
         * 
         * @param sortFields
         *            The sort to apply to the input objects. Useful for
         *            optimization, like sorting by the emit key for fewer
         *            reduces.
         * @return This builder for chaining method calls.
         */
        public Builder setSort(final IntegerElement... sortFields) {
            final DocumentBuilder builder = BuilderFactory.start();
            for (final IntegerElement sortField : sortFields) {
                builder.add(sortField);
            }
            mySort = builder.build();
            return this;
        }

        /**
         * Sets to true to emit progress messages in the server logs.
         * 
         * @param verbose
         *            True to emit progress messages in the server logs.
         * @return This builder for chaining method calls.
         */
        public Builder setVerbose(final boolean verbose) {
            myVerbose = verbose;
            return this;
        }
    }

    /**
     * Enumeration of the possible output types.
     * 
     * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
     */
    public enum OutputType {
        /** Returns the results inline to the reply to the map/reduce command. */
        INLINE,

        /**
         * Merges the results of the output collections and the map/reduce
         * results.
         */
        MERGE,

        /**
         * Runs a second reduce phase across the output collection and the
         * map/reduce results.
         */
        REDUCE,

        /**
         * Replaces the contents of the output collection with the map/reduce
         * results.
         */
        REPLACE;
    }
}
