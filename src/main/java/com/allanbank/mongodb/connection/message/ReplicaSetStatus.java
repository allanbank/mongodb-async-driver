/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.message;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * Provides a convenient mechanism for creating a <a href=
 * "http://www.mongodb.org/display/DOCS/Replica+Set+Commands#ReplicaSetCommands-replSetGetStatus"
 * >replSetGetStatus</a> command.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetStatus extends AdminCommand {

    /** The serverStatus "query" document. */
    public static final Document REPLICA_SET_STATUS;

    static {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.addInteger("replSetGetStatus", 1);
        REPLICA_SET_STATUS = builder.build();
    }

    /**
     * Create a new ReplicaSetStatus command.
     */
    public ReplicaSetStatus() {
        super(REPLICA_SET_STATUS);
    }
}