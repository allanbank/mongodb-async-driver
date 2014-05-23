/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

/**
 * Receiver provides an interface for a class that receives messages from the
 * server so that blocking futures can try and periodically receive messages.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Receiver {
    /**
     * Tries to receive a message from the server. Every attempt should be made
     * to not block.
     */
    public void tryReceive();
}
