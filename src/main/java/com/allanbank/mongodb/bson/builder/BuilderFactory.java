/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.builder;

import com.allanbank.mongodb.bson.builder.impl.ArrayBuilderImpl;
import com.allanbank.mongodb.bson.builder.impl.DocumentBuilderImpl;

/**
 * Helper class for getting started with a builder. The use of this class is
 * encouraged to avoid direct references to the builder implementations.
 * 
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
