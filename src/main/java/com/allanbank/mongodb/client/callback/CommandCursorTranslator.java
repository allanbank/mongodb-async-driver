/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.client.message.Reply;

/**
 * CommandCursorTranslator provides static utility methods to translate cursor
 * documents.
 *
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CommandCursorTranslator {

    /**
     * Translates the reply from a single document command reply into a standard
     * reply with the appropriate bit flips.
     * <p>
     * There are two possible formats we can receive:
     * <ul>
     * <li>
     * Traditional embedded documents under a {@code results} array. For this
     * format all of the Documents are in the single reply and there is no
     * cursor established.<blockquote>
     *
     * <pre>
     * <code>
     * {
     *   results : [
     *      { ... },
     *      ...
     *   ],
     *   ok : 1
     * }
     * </code>
     * </pre>
     *
     * </blockquote></li>
     * <li>
     * A {@code cursor} sub-document containing the cursor's {@code id} and
     * {@code firstBatch}. This reply establishes (possibly) a cursor for the
     * results.<blockquote>
     *
     * <pre>
     * <code>
     * {
     *   cursor : {
     *     id : 1234567,
     *     firstBatch : [
     *       { ... },
     *       ...
     *     ]
     *   }
     *   ok : 1
     * }
     * </code>
     * </pre>
     *
     * </blockquote></li>
     *
     *
     * @param reply
     *            The reply to translate.
     * @return The translated reply.
     */
    /* package */static Reply translate(final Reply reply) {
        Reply result = reply;

        final List<Reply> replies = translateAll(reply);
        if (replies.size() == 1) {
            result = replies.get(0);
        }

        return result;
    }

    /**
     * Translates the reply from a single document command reply into a list of
     * standard replies with the appropriate bit flips.
     * <p>
     * There are three possible formats we can receive:
     * <ul>
     * <li>
     * Traditional embedded documents under a {@code results} array. For this
     * format all of the Documents are in the single reply and there is no
     * cursor established.<blockquote>
     *
     * <pre>
     * <code>
     * {
     *   results : [
     *      { ... },
     *      ...
     *   ],
     *   ok : 1
     * }
     * </code>
     * </pre>
     *
     * </blockquote></li>
     * <li>
     * A {@code cursor} sub-document containing the cursor's {@code id} and
     * {@code firstBatch}. This reply establishes (possibly) a cursor for the
     * results.<blockquote>
     *
     * <pre>
     * <code>
     * {
     *   cursor : {
     *     id : 1234567,
     *     firstBatch : [
     *       { ... },
     *       ...
     *     ]
     *   }
     *   ok : 1
     * }
     * </code>
     * </pre>
     *
     * </blockquote></li>
     * <li>
     * A {@code cursor} sub-array with a sub-document as each element of the
     * array. Each sub-document contains a {@code cursor} document as described
     * above.<blockquote>
     *
     * <pre>
     * <code>
     * {
     *   cursors: [
     *     {
     *       cursor : {
     *         id : 1234567,
     *         firstBatch : [
     *           { ... },
     *           ...
     *         ]
     *       }
     *     },
     *     {
     *       cursor : {
     *         id : 1234568,
     *         firstBatch : [
     *           { ... },
     *           ...
     *         ]
     *       }
     *     },
     *     ...
     *   ]
     *   ok : 1
     * }
     * </code>
     * </pre>
     *
     * </blockquote></li>
     *
     *
     * @param reply
     *            The reply to translate.
     * @return The translated reply.
     */
    /* package */static List<Reply> translateAll(final Reply reply) {
        List<Reply> results = Collections.singletonList(reply);

        // Check for a single Document. All formats to translate are single
        // documents.
        final List<Document> docs = reply.getResults();
        if (docs.size() == 1) {
            final Document replyDoc = docs.get(0);

            List<DocumentElement> resultDocs;

            // Traditional first since it is more probable in the short term.
            final ArrayElement resultArray = replyDoc.get(ArrayElement.class,
                    "result");
            if (resultArray != null) {
                resultDocs = replyDoc.find(DocumentElement.class, "result",
                        ".*");
                results = Collections.singletonList(translate(reply, 0L,
                        resultDocs));
            }
            else {
                final DocumentElement cursor = replyDoc.get(
                        DocumentElement.class, "cursor");
                if (cursor != null) {
                    results = translate(reply,
                            Collections.singletonList(cursor));
                }
                else {
                    final List<DocumentElement> cursors = replyDoc.find(
                            DocumentElement.class, "cursors", ".*", "cursor");
                    if (!cursors.isEmpty()) {
                        results = translate(reply, cursors);
                    }
                }
            }
        }

        return results;
    }

    /**
     * Translates a list of cursor documents into a list of {@link Reply}
     * objects.
     *
     * @param reply
     *            The original reply.
     * @param cursors
     *            The cursor sub documents.
     * @return The translated replies.
     */
    private static List<Reply> translate(final Reply reply,
            final List<DocumentElement> cursors) {

        final List<Reply> results = new ArrayList<Reply>(cursors.size());
        for (final DocumentElement cursor : cursors) {
            final NumericElement id = cursor.get(NumericElement.class, "id");
            final List<DocumentElement> resultDocs = cursor.find(
                    DocumentElement.class, "firstBatch", ".*");

            results.add(translate(reply, (id == null) ? 0 : id.getLongValue(),
                    resultDocs));
        }
        return results;
    }

    /**
     * Creates a new reply based on the original reply and the specified cursor
     * id and document list.
     *
     * @param reply
     *            The original reply to copy from.
     * @param cursorId
     *            The cursor id that has been established.
     * @param results
     *            The results to include in the reply.
     * @return The translated reply.
     */
    private static Reply translate(final Reply reply, final long cursorId,
            final List<DocumentElement> results) {

        // Strip off the element-ness of the documents.
        final List<Document> docs = new ArrayList<Document>(results.size());
        for (final DocumentElement docElement : results) {
            docs.add(BuilderFactory.start(docElement).build());
        }

        return new Reply(reply.getResponseToId(), cursorId,
                reply.getCursorOffset(), docs, reply.isAwaitCapable(),
                reply.isCursorNotFound(), reply.isQueryFailed(),
                reply.isShardConfigStale());
    }

    /**
     * Creates a new CommandCursorTranslator.
     */
    private CommandCursorTranslator() {
        // Static class.
    }

}
