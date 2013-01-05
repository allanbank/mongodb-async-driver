/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

/**
 * Support for connections to Sharded Configurations.
 *
 * <h2>Usage</h2>
 * <p>
 * Users should not need to directly use the classes within this package.
 * </p>
 * 
 * <h2>Route Around  the <tt>mongos</tt></h2>
 * <p>
 * The driver out performs the legacy driver when talking directly to either a
 * stand-alone <tt>mongod</tt> process or when talking to a replica-set of 
 * <tt>mongod</tt> processes since it can effectively remove the wait time 
 * between requests to zero.  For a sharded configuration the <tt>mongos</tt>
 * still uses blocking requests which limits the performance capabilities of the
 * driver.
 * </p>
 * <p>
 * To overcome this limitation we provide a feature to route requests around the 
 * <tt>mongos</tt> when a request can be targeted at a particular shard in the cluster.
 * The request is never handled by the mongos and is sent directly to the shard servers.
 * The initial version of this capability only works for "exact-match" queries on 
 * the shard key.  All other requests are routed through the mongos for normal processing.
 * In the case of a failure the request is forwarded to the mongos for normal processing.
 * </p>
 * <p>
 * In order to send requests to a <tt>mongod</tt> process directly in a sharded
 * we need to ensure that the clusters <tt>chunk</tt> state matches the state 
 * used to determine where to send the command.  This is accomplished using the same
 * mechanism used by the <tt>mongos</tt>, namely the <tt>setShardConfig</tt> and 
 * stale configuration exceptions.
 * <p>
 * <p>
 * When a <tt>mongos</tt> connects to a <tt>mongod</tt> for the first time they perform
 * a handshake to synchronize what they both see as the state of the cluster's <tt>chunk</tt>s.
 * The handshake is started with the <tt>mongos</tt> sending a <tt>setShardVersion</tt>
 * similar to: <blockquote><pre><code>
 * {
 *     setShardVersion : "",
 *     init            : true,
 *     configdb        : &lt;configdb_url(s)&gt;,
 *     serverID        : &lt;ObjectID_for_server&gt;
 *     authoritative   : true
 * }
 * </code></pre></blockquote>
 * The <tt>mongod</tt> responds with: <blockquote><pre><code>
 * {
 *     initialized : true,
 *     ok          : 1.0
 * }
 * </code></pre></blockquote>
 * </p>
 * <p>
 * The second stage of the handshake is performed for each database/collection as they are 
 * accessed for the first time or after a version change is detected.  Again, the 
 * <tt>mongos</tt> initiates the exchange:<blockquote><pre><code>
 * {
 *     setShardVersion : "&lt;database&gt;.&lt;collection&gt;",
 *     configdb        : &lt;configdb_url(s)&gt;,
 *     version         : &lt;version_of_oldest_chunk_from_config_servers&gt;,
 *     serverID        : &lt;ObjectID_for_server&gt;,
 *     shard           : &lt;shard_name&gt;,
 *     shardHost       : &lt;shard_host&gt:&lt;shard_port&gt;
 * }
 * </code></pre></blockquote>
 * The <tt>mongod</tt> responds with: <blockquote><pre><code>
 * {
 *     oldVersion : &lt;previous_set_version&gt;,
 *     ok         : 1.0
 * }
 * </code></pre></blockquote>
 * Once this exchange is complete the <tt>mongos</tt> can interact with the <tt>mongod</tt> normally
 * for the specified database/collection.
 * </p>
 * <p>
 * The <tt>mongos</tt> and <tt>mongod</tt> query the config servers to obtain the map of
 * what chunks are resident on which shards.  {@link com.allanbank.mongodb.bson.element.MongoTimestampElement}s
 * are used to track the versions of each chunks configuration and to determine when a 
 * <tt>mongos</tt>'s state has become stale.  The <tt>mongos</tt> appears to issue three types of
 * queries to the config servers:
 * </p>
 * <ul>
 * <li> Query for databases: <tt>config.databases</tt>
 * <blockquote><pre><code>
 * {
 *     _id : "&lt;database&gt;"
 * }
 * </code></pre></blockquote></li>
 * <li> Query for collections: <tt>config.collections</tt>
 * <blockquote><pre><code>
 * {
 *     _id : /^&lt;database&gt;\./
 * }
 * </code></pre></blockquote></li>
 * <li> Query for Chunks: <tt>config.chunks</tt>
 * <blockquote><pre><code>
 * {
 *     query : {
 *         ns : "&lt;database&gt;.&lt;collection&gt;"
 *     },
 *     orderby : {
 *         lastmod : -1
 *     }
 * }
 * </code></pre></blockquote></li>
 * </ul>
 * <p>
 * The last query will also be used with a limit of 1 to return the newest chunk 
 * (by {@link com.allanbank.mongodb.bson.element.MongoTimestampElement}) for a collection.
 * </p>
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
package com.allanbank.mongodb.connection.sharded;

