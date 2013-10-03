/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.element.NullElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.client.connection.message.Reply;
import com.allanbank.mongodb.error.CursorNotFoundException;
import com.allanbank.mongodb.error.DuplicateKeyException;
import com.allanbank.mongodb.error.QueryFailedException;
import com.allanbank.mongodb.error.ReplyException;
import com.allanbank.mongodb.error.ShardConfigStaleException;

/**
 * Helper class for constructing callbacks that convert a {@link Reply} message
 * into a different type of result.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractValidatingReplyCallback implements
        Callback<Reply> {

    /** The fields that may contain the error code. */
    public static final String ERROR_CODE_FIELD = "code";

    /** The fields that may contain the error message. */
    public static final List<String> ERROR_MESSAGE_FIELDS;

    static {
        final List<String> fields = new ArrayList<String>(3);
        fields.add("$err");
        fields.add("errmsg");
        fields.add("err");

        ERROR_MESSAGE_FIELDS = Collections.unmodifiableList(fields);
    }

    /**
     * Creates a new AbstractValidatingReplyCallback.
     */
    public AbstractValidatingReplyCallback() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to {@link #verify(Reply) verify} the reply and then
     * {@link #handle(Reply) handle} it.
     * </p>
     * 
     * @see Callback#callback
     */
    @Override
    public void callback(final Reply result) {

        try {
            verify(result);
            handle(result);
        }
        catch (final MongoDbException error) {
            exception(error);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract void exception(final Throwable thrown);

    /**
     * Creates an exception from the {@link Reply}.
     * 
     * @param reply
     *            The raw reply.
     * @return The exception created.
     */
    protected MongoDbException asError(final Reply reply) {
        return asError(reply, false);
    }

    /**
     * Creates an exception from the {@link Reply}.
     * 
     * @param reply
     *            The raw reply.
     * @param knownError
     *            If true then the reply is assumed to be an error reply.
     * @return The exception created.
     */
    protected MongoDbException asError(final Reply reply,
            final boolean knownError) {
        final List<Document> results = reply.getResults();
        if (results.size() == 1) {
            final Document doc = results.get(0);
            final Element okElem = doc.get("ok");
            final Element errorNumberElem = doc.get(ERROR_CODE_FIELD);

            Element errorMessageElem = null;
            for (int i = 0; (errorMessageElem == null)
                    && (i < ERROR_MESSAGE_FIELDS.size()); ++i) {
                errorMessageElem = doc.get(ERROR_MESSAGE_FIELDS.get(i));
            }

            if (okElem != null) {
                final int okValue = toInt(okElem);
                if (okValue != 1) {
                    return asError(reply, okValue, toInt(errorNumberElem),
                            asString(errorMessageElem));
                }
                else if ((errorMessageElem != null)
                        && !(errorMessageElem instanceof NullElement)) {
                    return asError(reply, okValue, toInt(errorNumberElem),
                            asString(errorMessageElem));
                }
            }
            else if (knownError) {
                return asError(reply, -1, toInt(errorNumberElem),
                        asString(errorMessageElem));

            }
        }
        return null;
    }

    /**
     * Creates an exception from the parsed reply fields.
     * 
     * @param reply
     *            The raw reply.
     * @param okValue
     *            The 'ok' field.
     * @param errorNumber
     *            The 'errno' field.
     * @param errorMessage
     *            The 'errmsg' field.
     * @return The exception created.
     */
    protected MongoDbException asError(final Reply reply, final int okValue,
            final int errorNumber, final String errorMessage) {
        if ((errorNumber == 11000) || (errorNumber == 11001)
                || errorMessage.startsWith("E11000")
                || errorMessage.startsWith("E11001")) {
            return new DuplicateKeyException(okValue, errorNumber,
                    errorMessage, reply);
        }
        return new ReplyException(okValue, errorNumber, errorMessage, reply);
    }

    /**
     * Converts the {@link Element} to a string. If a {@link StringElement} the
     * value of the element is returned. In all other cases the toString()
     * result for the element is returned.
     * 
     * @param errorMessageElem
     *            The element to convert to a string.
     * @return The {@link Element}'s string value.
     */
    protected String asString(final Element errorMessageElem) {
        if (errorMessageElem instanceof StringElement) {
            return ((StringElement) errorMessageElem).getValue();
        }
        return String.valueOf(errorMessageElem);
    }

    /**
     * Checks for a non-flag error in the reply.
     * 
     * @param reply
     *            The reply to check.
     * @throws MongoDbException
     *             On an error represented in the reply.
     */
    protected void checkForError(final Reply reply) throws MongoDbException {
        final MongoDbException exception = asError(reply);
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Called once the {@link Reply} has been validated.
     * 
     * @param reply
     *            The {@link Reply} to be handled.
     */
    protected abstract void handle(Reply reply);

    /**
     * Converts a {@link NumericElement}into an <tt>int</tt> value. If not a
     * {@link NumericElement} then -1 is returned.
     * 
     * @param element
     *            The element to convert.
     * @return The element's integer value or -1.
     */
    protected int toInt(final Element element) {
        if (element instanceof NumericElement) {
            return ((NumericElement) element).getIntValue();
        }

        return -1;
    }

    /**
     * Checks the reply for an error message.
     * 
     * @param reply
     *            The Reply to verify is successful.
     * @throws MongoDbException
     *             On a failure message in the reply.
     */
    protected void verify(final Reply reply) throws MongoDbException {
        if (reply.isCursorNotFound()) {
            throw new CursorNotFoundException(reply, asError(reply, true));
        }
        else if (reply.isQueryFailed()) {
            throw new QueryFailedException(reply, asError(reply, true));
        }
        else if (reply.isShardConfigStale()) {
            throw new ShardConfigStaleException(reply, asError(reply, true));
        }
        else {
            checkForError(reply);
        }
    }

}