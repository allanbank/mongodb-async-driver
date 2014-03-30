/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import com.allanbank.mongodb.client.FutureCallback;
import com.allanbank.mongodb.client.message.Reply;

/**
 * FutureReplyCallback provides a {@link FutureCallback} that also implements
 * the {@link ReplyCallback} interface.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class FutureReplyCallback extends FutureCallback<Reply> implements
        ReplyCallback {

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return true.
     * </p>
     */
    @Override
    public boolean isLightWeight() {
        return true;
    }
}
