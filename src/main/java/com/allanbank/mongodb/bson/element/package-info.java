/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

/**
 * Provides the complete set of BSON {@link com.allanbank.mongodb.bson.Element}s used to construct 
 * {@link com.allanbank.mongodb.bson.Document}s.
 *
 * <h2>Usage</h2>
 * <p>
 * <tt>Elements</tt> are normally constructed using a builder from the 
 * {@link com.allanbank.mongodb.bson.builder} package.  The
 * {@link com.allanbank.mongodb.bson.Visitor} interface provides a convenient method to 
 * traversing a <tt>Document</tt> without directly accessing these classes.  The 
 * {@link com.allanbank.mongodb.bson.NumericElement} interface provides an easy way 
 * to coerce a numeric element value into an expected type.
 * </p>
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;