/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;

/**
 * AnswerCallback provides the ability to provide replies to callbacks.
 * 
 * @param <R>
 *            The type for the reply callback.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AnswerCallback<R> implements IArgumentMatcher {
    /**
     * Helper for matching callbacks and triggering them at method invocation
     * time.
     * 
     * @param clazz
     *            The class for the callback reply.
     * @return <code>null</code>
     */
    public static <T> Callback<T> callback(final Class<T> clazz) {
        EasyMock.reportMatcher(new AnswerCallback<T>());
        return null;
    }

    /**
     * Helper for matching callbacks and triggering them at method invocation
     * time.
     * 
     * @param clazz
     *            The class for the callback reply.
     * @param error
     *            The error to provide the callback.
     * @return <code>null</code>
     */
    public static <T> Callback<T> callback(final Class<T> clazz,
            final Throwable error) {
        EasyMock.reportMatcher(new AnswerCallback<T>(error));
        return null;
    }

    /**
     * Helper for matching callbacks and triggering them at method invocation
     * time.
     * 
     * @param reply
     *            The reply to give the callback when matching.
     * @return <code>null</code>
     */
    public static <T> Callback<T> callback(final T reply) {
        EasyMock.reportMatcher(new AnswerCallback<T>(reply));
        return null;
    }

    /** The reply to provide the callbacks. */
    private final Throwable myError;

    /** The reply to provide the callbacks. */
    private final R myReply;

    /**
     * Creates a new AnswerCallback.
     */
    public AnswerCallback() {
        myReply = null;
        myError = null;
    }

    /**
     * Creates a new AnswerCallback.
     * 
     * @param reply
     *            The reply to provide the callbacks.
     */
    public AnswerCallback(final R reply) {
        myReply = reply;
        myError = null;
    }

    /**
     * Creates a new AnswerCallback.
     * 
     * @param error
     *            The reply to provide the callbacks.
     */
    public AnswerCallback(final Throwable error) {
        myError = error;
        myReply = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void appendTo(final StringBuffer buffer) {
        if (myError != null) {
            buffer.append("Callback<" + myError.getClass().getSimpleName()
                    + ">");
        }
        else if (myReply != null) {
            buffer.append("Callback<" + myReply.getClass().getSimpleName()
                    + ">");
        }
        else {
            buffer.append("Callback<<interrupt>>");
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to check if the argument is a {@link Callback}, if so provide
     * it the reply and return true.
     * </p>
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean matches(final Object argument) {
        if (argument instanceof Callback<?>) {
            if (myError != null) {
                ((Callback<R>) argument).exception(myError);
            }
            else if (myReply != null) {
                ((Callback<R>) argument).callback(myReply);
            }
            else {
                Thread.currentThread().interrupt();
            }
            return true;
        }
        return false;
    }
}