/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.sharded;

/**
 * Shard contains the runtime state for a single shard within the cluster.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Shard {

    /**
     * The shard server id string from the configuration database. This is could
     * be a simple host:port or for a replica set of the form
     * name/host1:port1,host2:port2,...
     */
    private final String myShardId;

    // TODO - need a chunk/split version.

    /**
     * Creates a new Shard.
     * 
     * @param shardServers
     *            The shard servers string from the configuration database. This
     *            is could be a simple host:port or for a replica set of the
     *            form name/host1:port1,host2:port2,...
     */
    public Shard(final String shardServers) {
        super();

        myShardId = shardServers;
    }

    /**
     * Determines if the passed object is of this same type as this object and
     * if so that its shard id strings are the same.
     * 
     * @param object
     *            The object to compare to.
     * 
     * @see Object#equals(Object)
     */
    @Override
    public boolean equals(final Object object) {
        boolean result = false;
        if (this == object) {
            result = true;
        }
        else if ((object != null) && (getClass() == object.getClass())) {
            final Shard other = (Shard) object;

            result = myShardId.equals(other.myShardId);
        }
        return result;
    }

    /**
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        return myShardId.hashCode();
    }

    /**
     * Returns the shard server id string from the configuration database. This
     * is could be a simple host:port or for a replica set of the form
     * name/host1:port1,host2:port2,...
     * 
     * @return The shard server id string from the configuration database. This
     *         is could be a simple host:port or for a replica set of the form
     *         name/host1:port1,host2:port2,...
     * 
     * @see Object#toString()
     */
    @Override
    public String toString() {
        return myShardId;
    }
}
