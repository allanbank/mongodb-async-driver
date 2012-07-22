/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

/**
 * Provides domain classes and builders for queries and the common MongoDB commands.
 *
 * <h2>Usage</h2>
 * <p>
 * To construct a query using the builder use the static methods of the QueryBuilder 
 * class and then add conditions for fields. As an example:<blockquote>
 * <pre>
 * <code>
 * 
 * import static {@link com.allanbank.mongodb.builder.QueryBuilder#and com.allanbank.mongodb.builder.QueryBuilder.and}
 * import static {@link com.allanbank.mongodb.builder.QueryBuilder#or com.allanbank.mongodb.builder.QueryBuilder.or}
 * import static {@link com.allanbank.mongodb.builder.QueryBuilder#not com.allanbank.mongodb.builder.QueryBuilder.not}
 * import static {@link com.allanbank.mongodb.builder.QueryBuilder#where com.allanbank.mongodb.builder.QueryBuilder.where}
 * 
 * Document query = 
 *           or( 
 *              where("f").greaterThan(23).lessThan(42).and("g").lessThan(3),
 *              and( 
 *                where("f").greaterThanOrEqualTo(42),
 *                not( where("g").lessThan(3) ) 
 *              )
 *           );
 * </code>
 * </pre>
 * <blockquote>
 * </p>
 * <p>
 * Each of the commands classes is immutable but has an inner static Builder class that can be used to
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
package com.allanbank.mongodb.builder;

