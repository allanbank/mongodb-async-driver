/*
 * #%L
 * LambdaCallback.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
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
