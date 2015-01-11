/*
 * #%L
 * Callback.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
 * Interface for a callback with the result of a MongoDB operation.
 * <p>
 * For a lambda friendly version of this interface see {@link LambdaCallback}.
 * </p>
 *
 * @param <V>
 *            The type of the operation's result.
 *
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Callback<V> {

    /**
     * Called when the MongoDB operation has completed with the result of the
     * operation.
     *
     * @param result
     *            The result of the operation.
     */
    public void callback(V result);

    /**
     * Called when the operation fails due to an exception.
     *
     * @param thrown
     *            The thrown exception.
     */
    public void exception(Throwable thrown);

}
