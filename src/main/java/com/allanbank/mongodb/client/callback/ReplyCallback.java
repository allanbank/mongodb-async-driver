/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.client.message.Reply;

/**
 * ReplyCallback provides an interface for the receive processing to determine
 * is a callback can be safely called on the receiver's thread.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface ReplyCallback extends Callback<Reply> {

    /**
     * Returns true if the callback is lightweight and can be safely performed
     * in the receive thread.
     * 
     * @return True if the callback is lightweight and can be safely performed
     *         in the receive thread.
     */
    public boolean isLightWeight();
}
