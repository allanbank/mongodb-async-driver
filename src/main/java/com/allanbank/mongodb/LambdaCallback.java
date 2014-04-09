/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

/**
 * LambdaCallback provides a lambda friendly interface for receiving the
 * asynchronous results of a MongoDB operation.
 * <p>
 * The method name is based on the class (in
 * {@code java.util.function.BiConsumer}) from Java 8. To support earlier JDKs
 * the {@code andThen()} method is not supported.
 * </p>
 * <p>
 * This interface follows the defacto convention that the first argument
 * (left-side) to the method is the exception (error) and the second argument
 * (right-side) is the value. Generally only one of the two arguments will be
 * non-null.
 * </p>
 * 
 * @param <V>
 *            The type of the operation's result.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface LambdaCallback<V> {

    /**
     * Called when the MongoDB operation has completed with the error or result
     * of the operation.
     * 
     * @param thrown
     *            The error encountered when processing the request.
     * @param result
     *            The result of the operation.
     */
    public void accept(Throwable thrown, V result);
}
