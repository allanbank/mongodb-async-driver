/*
 * #%L
 * ValidatingReplyCallback.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.callback;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.client.message.Reply;

/**
 * ValidatingCallback to expect and extract a single document from the reply.
 * The document is checked for errors but then ignored.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ValidatingReplyCallback
        extends AbstractReplyCallback<Reply> {

    /**
     * Create a new ValidatingReplyCallback.
     *
     * @param delegate
     *            The delegate that we can wait on for the reply.
     */
    public ValidatingReplyCallback(final FutureReplyCallback delegate) {
        super(delegate);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return true as there is no additional processing.
     * </p>
     */
    @Override
    public boolean isLightWeight() {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the original reply.
     * </p>
     */
    @Override
    protected Reply convert(final Reply reply) throws MongoDbException {
        return reply;
    }
}