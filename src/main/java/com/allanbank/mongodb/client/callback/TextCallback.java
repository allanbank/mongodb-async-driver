/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import java.util.ArrayList;
import java.util.List;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.builder.Text;
import com.allanbank.mongodb.builder.TextResult;

/**
 * TextCallback provides conversion from a {@link Text text} command's result
 * document to a {@link TextResult}.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class TextCallback implements Callback<List<Document>> {

    /** The delegate callback to receive the {@link TextResult}s. */
    private final Callback<List<TextResult>> myDelegate;

    /**
     * Creates a new TextCallback.
     * 
     * @param delegate
     *            The delegate callback to receive the {@link TextResult}s.
     */
    public TextCallback(final Callback<List<TextResult>> delegate) {
        myDelegate = delegate;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to convert each document into a {@link TextResult} and forward
     * to the delegate.
     * </p>
     */
    @Override
    public void callback(final List<Document> result) {
        final List<TextResult> results = new ArrayList<TextResult>(
                result.size());

        for (final Document doc : result) {
            results.add(new TextResult(doc));
        }

        myDelegate.callback(results);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward to the delegate callback.
     * </p>
     */
    @Override
    public void exception(final Throwable thrown) {
        myDelegate.exception(thrown);
    }
}
