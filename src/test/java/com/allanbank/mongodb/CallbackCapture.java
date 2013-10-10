/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import org.easymock.Capture;
import org.easymock.EasyMock;

/**
 * CallbackCapture provides the ability to trigger the callback when called from
 * an {@link EasyMock} mock.
 * 
 * @param <T>
 *            The type of value expected by the callback.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CallbackCapture<T> extends Capture<Callback<T>> {

    /** Serialization version for the class. */
    private static final long serialVersionUID = 7524739505179001121L;

    /**
     * Creates a new CallbackCapture.
     * 
     * @param value
     *            The value to provide to the callback.
     * @return The CallbackCapture.
     */
    public static <T> Callback<T> callback(final T value) {
        EasyMock.capture(new CallbackCapture<T>(value));
        return null;
    }

    /**
     * Creates a new CallbackCapture.
     * 
     * @param error
     *            The error to provide to the callback.
     * @return The CallbackCapture.
     */
    public static <T> Callback<T> callback(final Throwable error) {
        EasyMock.capture(new CallbackCapture<T>(error));
        return null;
    }

    /**
     * Creates a new CallbackCapture.
     * 
     * @return The CallbackCapture.
     */
    public static <T> Callback<T> callbackError() {
        EasyMock.capture(new CallbackCapture<T>(new Throwable("Injected")));
        return null;
    }

    /** The error to provide to the callback. */
    private final Throwable myError;

    /** The reply to provide to the callback. */
    private final T myReply;

    /**
     * Creates a new CallbackCapture.
     * 
     * @param reply
     *            The reply to provide to the callback.
     */
    public CallbackCapture(final T reply) {
        myReply = reply;
        myError = null;
    }

    /**
     * Creates a new CallbackCapture.
     * 
     * @param error
     *            The error to provide to the callback.
     */
    public CallbackCapture(final Throwable error) {
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
    public void setValue(final Callback<T> value) {
        super.setValue(value);

        if (myReply != null) {
            value.callback(myReply);
        }
        else {
            value.exception(myError);
        }
    }

}
