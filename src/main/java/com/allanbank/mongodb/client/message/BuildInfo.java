/*
 * #%L
 * BuildInfo.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
 * Provides a convenient mechanism for creating a <a href=
 * "http://docs.mongodb.org/manual/reference/command/buildInfo/" >buildinfo</a>
 * command.
 * <p>
 * This is a helper class for retrieving the version of the server:
 * </p>
 * <blockquote>
 * 
 * <pre>
 * <code>
 * {
 *     "version" : "2.2.6",
 *     "gitVersion" : "nogitversion",
 *     "sysInfo" : "Linux buildvm-09.phx2.fedoraproject.org 2.6.32-358.14.1.el6.x86_64 #1 SMP Mon Jun 17 15:54:20 EDT 2013 x86_64 BOOST_LIB_VERSION=1_50",
 *     "versionArray" : [
 *         2,
 *         2,
 *         6,
 *         0
 *     ],
 *     "bits" : 64,
 *     "debug" : false,
 *     "maxBsonObjectSize" : 16777216,
 *     "ok" : 1
 * }
 * </code>
 * </pre>
 * 
 * </blockquote>
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BuildInfo extends AdminCommand {

    /** The ismaster "query" document. */
    public static final Document BUILD_INFO;

    static {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.addInteger("buildinfo", 1);
        BUILD_INFO = new ImmutableDocument(builder);
    }

    /**
     * Create a new IsMaster command.
     */
    public BuildInfo() {
        super(BUILD_INFO);
    }
}
