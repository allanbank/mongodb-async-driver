/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.io.Closeable;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.builder.Find;

/**
 * MongoCursorControl provides the controls for a MongoDB cursor interaction.
 * Normally this interface is used via a {@link MongoIterator} but in the case
 * of streaming only the controls are returned.
 *
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface MongoCursorControl extends Closeable {
    /** The batch size field in the cursor document. */
    public static final String BATCH_SIZE_FIELD = "batch_size";

    /** The cursor id field in the cursor document. */
    public static final String CURSOR_ID_FIELD = "cursor_id";

    /** The remaining limit field in the cursor document. */
    public static final String LIMIT_FIELD = "limit";

    /** The name space field in the cursor document. */
    public static final String NAME_SPACE_FIELD = "ns";

    /** The server name field in the cursor document. */
    public static final String SERVER_FIELD = "server";

    /**
     * Returns a {@link Document} that can be used to restart the
     * cursor/iterator.
     * <p>
     * If this iterator is exhausted or closed then the cursor is also closed on
     * the server and this method will return null.
     * </p>
     * <p>
     * If the cursor/{@link Find} was not created with out a timeout then
     * eventually the server will automatically remove the cursor and the
     * restart will fail.
     * </p>
     * <p>
     * Returns the active cursor in the form:<blockquote>
     *
     * <pre>
     * <code>
     * {
     *     {@value #NAME_SPACE_FIELD} : '&lt;database_name&gt;.$lt;collection_name&gt;',
     *     {@value #CURSOR_ID_FIELD} : &lt;cursor_id&gt;,
     *     {@value #SERVER_FIELD} : '&lt;server&gt;',
     *     {@value #LIMIT_FIELD} : &lt;remaining_limit&gt;
     *     {@value #BATCH_SIZE_FIELD} : &lt;batch_size&gt;
     * }</code>
     * </pre>
     *
     * </blockquote>
     *
     * @return A document that can be used to restart the cursor.
     *         <code>null</code> if the server's cursor has been exhausted or
     *         closed.
     */
    public Document asDocument();

    /**
     * Close the iterator and release any resources it is holding.
     */
    @Override
    public void close();

    /**
     * Returns the size for batches of documents that are requested.
     *
     * @return The size of the batches of documents that are requested.
     */
    public int getBatchSize();

    /**
     * Sets the size for future batch sizes.
     *
     * @param batchSize
     *            The size to request for future batch sizes.
     */
    public void setBatchSize(int batchSize);

    /**
     * Stops the iterator after consuming any received and/or requested batches.
     * <p>
     * <b>WARNING</b>: This will leave the cursor open on the server. Even a
     * {@link #close()} on this object will not close the cursor on the server.
     * Users should persist the state of the cursor as returned from
     * {@link #asDocument()} and restart the cursor using one of the
     * {@link MongoClient#restart(com.allanbank.mongodb.bson.DocumentAssignable)}
     * or
     * {@link MongoClient#restart(StreamCallback, com.allanbank.mongodb.bson.DocumentAssignable)}
     * methods. Use {@link #stop()} with extreme caution.
     * </p>
     * <p>
     * The iterator or stream will naturally stop (
     * {@link MongoIterator#hasNext()} will return false or the stream's call
     * back {@link StreamCallback#done()} method will be called) when the
     * current batch and any batches already requested are exhausted.
     * </p>
     */
    public void stop();

}