/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

/**
 * Provides the ability to serialize and deserialize BSON {@link com.allanbank.mongodb.bson.Document}s.
 *
 * <h2>Usage</h2>
 * <p>
 * Two implementations of stream are provided:
 * <ul>
 * <li>{@link com.allanbank.mongodb.bson.io.BsonInputStream}/{@link com.allanbank.mongodb.bson.io.BsonOutputStream}
 * provide the ability to read and write BSON documents directly to a stream without the need to buffer
 * document contents to determine the length prefix for the document.  They require 2 scans over the document.  
 * The first determines the size of the document and the second writes the document's contents.</li>
 * <li>{@link com.allanbank.mongodb.bson.io.BufferingBsonOutputStream}
 * provide the ability to write BSON documents using an intermediate re-used set of buffers for 
 * the document's contents.</li>
 * </p>
 * <p>
 * Empirical testing has shown that the un-buffered <tt>BsonInputStream</tt> and 
 * <tt>BsonOutputStream</tt> perform better on most BSON documents.
 * </p>
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;