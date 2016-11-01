package com.allanbank.mongodb.client.callback;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.element.NullElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.DuplicateKeyException;
import com.allanbank.mongodb.error.DurabilityException;
import com.allanbank.mongodb.error.MaximumTimeLimitExceededException;
import com.allanbank.mongodb.error.QueryFailedException;
import com.allanbank.mongodb.error.ReplyException;

/**
 * ReplyErrorHandler provides common error handling logic.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplyErrorHandler {

    /** The fields that may contain the error code. */
    public static final String ERROR_CODE_FIELD = "code";

    /** The fields that may contain the error message. */
    public static final List<String> ERROR_MESSAGE_FIELDS;

    static {
        final List<String> fields = new ArrayList<String>(3);
        fields.add("jnote");
        fields.add("wnote");
        fields.add("$err");
        fields.add("errmsg");
        fields.add("err");

        ERROR_MESSAGE_FIELDS = Collections.unmodifiableList(fields);
    }

    /**
     * Creates a new ReplyErrorHandler.
     */
    public ReplyErrorHandler() {
        super();
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
    public MongoDbException asException(final Reply reply,
            final boolean knownError) {
        return asError(reply, knownError);
    }

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
        else if (knownError) {
            return new QueryFailedException(reply, new MongoDbException(
                    "Unknown Error."));
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
     * @param knownDurabilityError
     *            Set to true when we know the error is a durability failure.
     * @param errorMessage
     *            The 'errmsg' field.
     * @param message
     *            The message that triggered the error, if known.
     * @return The exception created.
     */
    protected final MongoDbException asError(final Reply reply,
            final int okValue, final int errorNumber,
            final boolean knownDurabilityError, final String errorMessage,
            final Message message) {

        if (isDurabilityFailure(reply, knownDurabilityError, errorMessage)) {
            return new DurabilityException(okValue, errorNumber, errorMessage,
                    message, reply);
        }
        else if ((errorNumber == 11000) || (errorNumber == 11001)
                || errorMessage.startsWith("E11000")
                || errorMessage.startsWith("E11001")) {
            return new DuplicateKeyException(okValue, errorNumber,
                    errorMessage, message, reply);
        }
        else if ((errorNumber == 50) || // Standard
                (errorNumber == 11601) || // errmsg : 'operation was interrupted', code : 11601
                (errorNumber == 13475) || // M/R 2.5-ish
                (errorNumber == 16711)) { // GroupBy 2.5-ish
            return new MaximumTimeLimitExceededException(okValue, errorNumber,
                    errorMessage, message, reply);
        }
        return new ReplyException(okValue, errorNumber, errorMessage, message,
                reply);
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
        return asError(reply, okValue, errorNumber, false, errorMessage, null);
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
     * Check if the failure is a failure of the durability of the write.
     *
     * @param reply
     *            The reply to the message.
     * @param knownDurabilityError
     *            If true then the result is already known to be a durability
     *            failure.
     * @param errorMessage
     *            The error message extracted from the document.
     * @return True if the durability has failed.
     */
    private boolean isDurabilityFailure(final Reply reply,
            final boolean knownDurabilityError, final String errorMessage) {
        boolean durabilityError = knownDurabilityError;

        final List<Document> results = reply.getResults();
        if ((results.size() == 1) && !knownDurabilityError) {
            final Document doc = results.get(0);

            durabilityError = doc.contains("wtimeout")
                    || doc.contains("wnote")
                    || doc.contains("jnote")
                    || doc.contains("badGLE")
                    || errorMessage.startsWith("cannot use 'j' option")
                    || errorMessage
                            .startsWith("could not enforce write concern");
        }
        return durabilityError;
    }

}