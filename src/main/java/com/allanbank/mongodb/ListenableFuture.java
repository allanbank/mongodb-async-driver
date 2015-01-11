/*
 * #%L
 * ListenableFuture.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Enhancement to the {@link Future} interface inspired by the Google Guava's
 * ListenableFuture.
 *
 * @param <V>
 *            The type of the result of the {@link Future}.
 *
 * @see <a
 *      href="https://code.google.com/p/guava-libraries/wiki/ListenableFutureExplained">Listenable
 *      Future Explained</a>
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@ThreadSafe
public interface ListenableFuture<V>
        extends Future<V> {
    /**
     * Add a {@link Runnable} to be executed once the future is completed via
     * the provided executable.
     *
     * <p>
     * The order that {@link Runnable Runnables} are executed is unspecified.
     * </p>
     *
     * @param runnable
     *            The myRunnable to execute.
     * @param executor
     *            Executor to use with the myRunnable.
     * @throws RejectedExecutionException
     *             If the future is already complete and the executor rejects
     *             the request.
     * @throws IllegalArgumentException
     *             On the {@code myRunnable} or {@code executor} being null.
     */
    void addListener(Runnable runnable, Executor executor)
            throws RejectedExecutionException, IllegalArgumentException;
}