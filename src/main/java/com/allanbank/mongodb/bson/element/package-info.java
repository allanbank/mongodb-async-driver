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