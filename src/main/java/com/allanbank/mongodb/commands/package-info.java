/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

/**
 * Provides domain classes and builders for the common MongoDB commands.
 *
 * <h2>Usage</h2>
 * <p>
 * Each class is immutable but has an inner static Builder class that can be used to
 * construct the command object.  
 * </p>
 * <p>
 * For Example:<blockquote><pre><code>
 * GroupBy.Builder builder = new GroupBy.Builder();
 * builder.setKeys(Collections.singletonSet("key"));
 *
 * GroupBy command = builder.build(); 
 * </code></pre><blockquote>
 * </p>
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
package com.allanbank.mongodb.commands;

