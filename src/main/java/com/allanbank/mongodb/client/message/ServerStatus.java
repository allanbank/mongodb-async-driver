/*
 * #%L
 * ServerStatus.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.message;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.impl.ImmutableDocument;

/**
 * Provides a convenient mechanism for creating a <a
 * href="http://docs.mongodb.org/manual/reference/command/serverStatus/"
 * >serverStatus</a> command.
 * <p>
 * This is a helper class for retrieving the status of a MongoDB server. The
 * results of this command will look like: <blockquote>
 *
 * <pre>
 * <code>
 * {
 *     "host" : "mongos.example.com",
 *     "version" : "1.8.2",
 *     "process" : "mongos",
 *     "uptime" : 1403,
 *     "localTime" : ISODate("2011-12-06T00:11:56.822Z"),
 *     "mem" : {
 *         "resident" : 5,
 *         "virtual" : 621,
 *         "supported" : true
 *     },
 *     "connections" : {
 *         "current" : 1,
 *         "available" : 818
 *     },
 *     "extra_info" : {
 *         "note" : "fields vary by platform",
 *         "heap_usage_bytes" : 94560,
 *         "page_faults" : 0
 *     },
 *     "opcounters" : {
 *         "insert" : 0,
 *         "query" : 9,
 *         "update" : 0,
 *         "delete" : 0,
 *         "getmore" : 0,
 *         "command" : 33
 *     },
 *     "ops" : {
 *         "sharded" : {
 *             "insert" : 0,
 *             "query" : 0,
 *             "update" : 0,
 *             "delete" : 0,
 *             "getmore" : 0,
 *             "command" : 0
 *         },
 *         "notSharded" : {
 *             "insert" : 0,
 *             "query" : 9,
 *             "update" : 0,
 *             "delete" : 0,
 *             "getmore" : 0,
 *             "command" : 33
 *         }
 *     },
 *     "shardCursorType" : {
 * 
 *     },
 *     "asserts" : {
 *         "regular" : 0,
 *         "warning" : 0,
 *         "msg" : 0,
 *         "user" : 3,
 *         "rollovers" : 0
 *     },
 *     "network" : {
 *         "bytesIn" : 3205,
 *         "bytesOut" : 6698,
 *         "numRequests" : 45
 *     },
 *     "ok" : 1
 * }
 * </code>
 * </pre>
 *
 * </blockquote>
 * </p>
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerStatus
        extends AdminCommand {

    /** The serverStatus "query" document. */
    public static final Document SERVER_STATUS;

    static {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.addInteger("serverStatus", 1);
        SERVER_STATUS = new ImmutableDocument(builder);
    }

    /**
     * Create a new ServerStatus command.
     */
    public ServerStatus() {
        super(SERVER_STATUS);
    }
}
