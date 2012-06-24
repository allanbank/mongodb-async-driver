/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

/**
 * Strongly typed, immutable implementation of the 
 * <a href="http://bsonspec.org/">BSON Specification</a>.
 *
 * <h2>Usage</h2>
 * <p>
 * A {@link com.allanbank.mongodb.bson.Document} represents a single BSON document.  It is composed of 0-N 
 * {@link com.allanbank.mongodb.bson.Element}s. 
 * </p>
 * <p>
 * The {@link com.allanbank.mongodb.bson.Visitor} pattern is used internally for traversing a
 * <tt>Document</tt>'s structure. 
 * </p>
 *
 * <h2>Sub-Packages</h2>
 * <p>
 * The <a href="builder/package-summary.html">com.allanbank.mongodb.bson.builder</a> package contains
 * builder interfaces to aid in constructing BSON <tt>Document</tt>s.
 * </p>
 * <p>
 * The <a href="element/package-summary.html">com.allanbank.mongodb.bson.element</a> package contains all
 * of the type specific BSON element types.  Most users will never have to directly interface with
 * these classes by using the <tt>Visitor</tt> pattern.
 * </p>
 * <p>
 * The <a href="impl/package-summary.html">com.allanbank.mongodb.bson.impl</a> package contains the
 * concrete <tt>Document</tt> implementation.
 * </p>
 * <p>
 * In the <a href="impl/package-summary.html">com.allanbank.mongodb.bson.io</a> package is a collection 
 * of classes to read and write BSON <tt>Document</tt>s.
 * </p>
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
package com.allanbank.mongodb.bson;