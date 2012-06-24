/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

/**
 * Provides the interfaces for builders to aid in constructing BSON 
 * {@link com.allanbank.mongodb.bson.Document}s.
 * 
 * <h2>Usage</h2>
 * <p>
 * The {@link com.allanbank.mongodb.bson.builder.BuilderFactory} provides methods 
 * to start a {@link com.allanbank.mongodb.bson.builder.DocumentBuilder} or 
 * {@link com.allanbank.mongodb.bson.builder.ArrayBuilder}. 
 * </p>
 * <h2>Sub-Packages</h2>
 * <p>
 * The <a href="impl/package-summary.html">com.allanbank.mongodb.bson.builder.impl</a> package the builder 
 * implementations.  Direct use of these classes is discouraged.  Instead use the interfaces in this package and
 * the {@link com.allanbank.mongodb.bson.builder.BuilderFactory} to start a document. 
 * </p>
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
package com.allanbank.mongodb.bson.builder;