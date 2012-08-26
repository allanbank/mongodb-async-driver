/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection;

/**
 * ClusterType provides an enumeration of the types of cluster configurations
 * supported.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum ClusterType {

    /** The replica set type cluster. */
    REPLICA_SET,

    /** The sharded type cluster using 'mongos' servers. */
    SHARDED,

    /** A single 'mongod' server. */
    STAND_ALONE;

}
