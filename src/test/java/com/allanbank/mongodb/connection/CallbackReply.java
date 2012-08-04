/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection;

import java.util.ArrayList;
import java.util.List;

import org.easymock.Capture;
import org.easymock.EasyMock;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * CallbackReply provides the ability to trigger the callback when called from
 * an {@link EasyMock} mock.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CallbackReply extends Capture<Callback<Reply>> {

    /** Serialization version for the class. */
    private static final long serialVersionUID = 7524739505179001121L;

    /**
     * Creates a new CallbackReply.
     * 
     * @param builders
     *            The reply to provide to the callback.
     * @return The CallbackReply.
     */
    public static Callback<Reply> cb(final DocumentBuilder... builders) {
        return cb(reply(builders));
    }

    /**
     * Creates a new CallbackReply.
     * 
     * @param reply
     *            The reply to provide to the callback.
     * @return The CallbackReply.
     */
    public static Callback<Reply> cb(final Reply reply) {
        EasyMock.capture(new CallbackReply(reply));
        return null;
    }

    /**
     * Creates a new CallbackReply.
     * 
     * @param error
     *            The error to provide to the callback.
     * @return The CallbackReply.
     */
    public static Callback<Reply> cb(final Throwable error) {
        EasyMock.capture(new CallbackReply(error));
        return null;
    }

    /**
     * Creates a new CallbackReply.
     * 
     * @return The CallbackReply.
     */
    public static Callback<Reply> cbError() {
        EasyMock.capture(new CallbackReply(new Throwable("Injected")));
        return null;
    }

    /**
     * Creates a reply with the specified document.
     * 
     * @param builders
     *            The builder for the reply document.
     * @return The Repy.
     */
    public static Reply reply(final DocumentBuilder... builders) {
        final List<Document> docs = new ArrayList<Document>(builders.length);
        for (final DocumentBuilder builder : builders) {
            docs.add(builder.build());
        }
        return new Reply(0, 0, 0, docs, false, false, false, false);
    }

    /** The error to provide to the callback. */
    private final Throwable myError;

    /** The reply to provide to the callback. */
    private final Reply myReply;

    /**
     * Creates a new CallbackReply.
     * 
     * @param reply
     *            The reply to provide to the callback.
     */
    public CallbackReply(final Reply reply) {
        myReply = reply;
        myError = null;
    }

    /**
     * Creates a new CallbackReply.
     * 
     * @param error
     *            The error to provide to the callback.
     */
    public CallbackReply(final Throwable error) {
        myReply = null;
        myError = error;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call super and then provide the reply or error to the
     * callback.
     * </p>
     */
    @Override
    public void setValue(final Callback<Reply> value) {
        super.setValue(value);

        if (myReply != null) {
            value.callback(myReply);
        }
        value.exception(myError);
    }
}
