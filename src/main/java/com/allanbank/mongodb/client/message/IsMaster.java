/*
 * Copyright 2011-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.impl.ImmutableDocument;

/**
 * Provides a convenient mechanism for creating a <a href=
 * "http://docs.mongodb.org/manual/reference/command/isMaster/" >ismaster</a>
 * command.
 * <p>
 * This is a helper class for retrieving the status of replica sets. The results
 * of this command will look like:
 * </p>
 * <blockquote>
 * 
 * <pre>
 * <code>
 * {
 *         "ismaster" : false,
 *         "secondary" : true,
 *         "hosts" : [
 *                 "ny1.acme.com",
 *                 "ny2.acme.com",
 *                 "sf1.acme.com"
 *         ],
 *         "passives" : [
 *              "ny3.acme.com",
 *              "sf3.acme.com"
 *         ],
 *         "arbiters" : [
 *             "sf2.acme.com",
 *         ]
 *         "primary" : "ny2.acme.com",
 *         "ok" : true
 * }
 * </code>
 * </pre>
 * 
 * </blockquote>
 * 
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class IsMaster extends AdminCommand {

    /** The ismaster "query" document. */
    public static final Document IS_MASTER;

    static {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.addInteger("isMaster", 1);
        IS_MASTER = new ImmutableDocument(builder);
    }

    /**
     * Create a new IsMaster command.
     */
    public IsMaster() {
        super(IS_MASTER);
    }
}
