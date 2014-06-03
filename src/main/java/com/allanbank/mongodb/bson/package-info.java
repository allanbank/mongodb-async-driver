/*
 * #%L
 * package-info.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
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
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
package com.allanbank.mongodb.bson;