/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.builder;

import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.impl.ArrayBuilderImpl;
import com.allanbank.mongodb.bson.builder.impl.DocumentBuilderImpl;

/**
 * Helper class for getting started with a builder. The use of this class is
 * encouraged to avoid direct references to the builder implementations.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BuilderFactory {

    /**
     * Creates a new {@link DocumentBuilder}.
     * 
     * @return The root level document builder.
     */
    public static DocumentBuilder start() {
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
    public static DocumentBuilder start(final DocumentAssignable seedDocument) {
        return new DocumentBuilderImpl(seedDocument);
    }

    /**
     * Creates a new {@link ArrayBuilder}.
     * 
     * @return The root level array builder.
     */
    public static ArrayBuilder startArray() {
        return new ArrayBuilderImpl();
    }

    /**
     * Creates a new builder factory.
     */
    private BuilderFactory() {
        // Nothing to do.
    }
}
