/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import com.allanbank.mongodb.client.Message;

/**
 * CursorableMessage provides a common interface for messages that can start a
 * cursor.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface CursorableMessage extends Message {
    /**
     * Returns the number of documents to be returned in each batch of results.
     *
     * @return The number of documents to be returned in each batch of results.
     */
    public int getBatchSize();

    /**
     * Returns the name of the collection.
     *
     * @return The name of the collection.
     */
    public String getCollectionName();

    /**
     * Returns the total number of documents to be returned.
     *
     * @return The total number of documents to be returned.
     */
    public int getLimit();

}
