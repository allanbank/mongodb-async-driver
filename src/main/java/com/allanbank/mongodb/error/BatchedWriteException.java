/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.allanbank.mongodb.builder.BatchedWrite;
import com.allanbank.mongodb.builder.write.WriteOperation;

/**
 * BatchedWriteException provides a single exception containing the aggregated
 * errors across the batched writes.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BatchedWriteException extends ReplyException {

    /** The serialization version for the class. */
    private static final long serialVersionUID = -7797670480003384909L;

    /**
     * Constructs a single error message from the nested errors.
     * 
     * @param errors
     *            The nested errors.
     * @return An errors message composed of the nested errors.
     */
    private static String createErrorMessage(
            final Map<WriteOperation, Throwable> errors) {
        final StringBuilder builder = new StringBuilder();
        if (errors.size() == 1) {
            builder.append("Error sending a batched write: ");
            builder.append(errors.values().iterator().next().getMessage());
        }
        else {
            builder.append(errors.size());
            builder.append(" errors sending a batched write. Unique error messages:\n\t");

            final Set<String> unique = new HashSet<String>();
            for (final Throwable error : errors.values()) {
                final String message = error.getMessage();
                if (unique.add(message)) {
                    builder.append(message);
                    builder.append("\n\t");
                }
            }
            builder.setLength(builder.length() - 2);
        }

        return builder.toString();
    }

    /** The nested errors. */
    private final Map<WriteOperation, Throwable> myErrors;

    /** The number of touched documents when the error is triggered. */
    private final long myN;

    /** The writes that did not get run by the server. */
    private final List<WriteOperation> mySkipped;

    /** The write that caused the errors. */
    private final BatchedWrite myWrite;

    /**
     * Creates a new BatchedWriteException.
     * 
     * @param write
     *            The write that caused the errors.
     * @param n
     *            The number of touched documents when the error is triggered.
     * @param skipped
     *            The writes that did not get run by the server.
     * @param errors
     *            The nested errors.
     */
    public BatchedWriteException(final BatchedWrite write, final long n,
            final List<WriteOperation> skipped,
            final Map<WriteOperation, Throwable> errors) {
        super(null, createErrorMessage(errors));

        myWrite = write;
        myN = n;
        mySkipped = Collections.unmodifiableList(new ArrayList<WriteOperation>(
                skipped));
        myErrors = Collections
                .unmodifiableMap(new IdentityHashMap<WriteOperation, Throwable>(
                        errors));
    }

    /**
     * Returns the nested errors.
     * 
     * @return The nested errors.
     */
    public Map<WriteOperation, Throwable> getErrors() {
        return myErrors;
    }

    /**
     * Returns the number of touched documents when the error is triggered.
     * 
     * @return The number of touched documents when the error is triggered.
     */
    public long getN() {
        return myN;
    }

    /**
     * Returns the writes that did not get run by the server.
     * 
     * @return The writes that did not get run by the server.
     */
    public List<WriteOperation> getSkipped() {
        return mySkipped;
    }

    /**
     * Returns the write that caused the errors.
     * 
     * @return The write that caused the errors.
     */
    public BatchedWrite getWrite() {
        return myWrite;
    }
}
