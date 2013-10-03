/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection;

/**
 * ClusterType provides an enumeration of the types of cluster configurations
 * supported.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum ClusterType {

    /** The replica set type cluster. */
    REPLICA_SET,

    /** The sharded type cluster using 'mongos' servers. */
    SHARDED,

    /** A single 'mongod' server. */
    STAND_ALONE;

}
