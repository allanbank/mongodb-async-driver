/*
 * #%L
 * CredentialsCallbackHandler.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.connection.auth;

import java.util.Arrays;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import com.allanbank.mongodb.Credential;

/**
 * CredentialsCallbackHandler provides a {@link CallbackHandler} to provide the
 * user's name and password to the {@link NameCallback} and
 * {@link PasswordCallback}.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the
 *         extensions.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CredentialsCallbackHandler implements CallbackHandler {

    /** The credentials to provide to the callbacks. */
    private final Credential myCredentials;

    /**
     * Creates a new CredentialsCallbackHandler.
     * 
     * @param credentials
     *            The credentials to provide to the callbacks.
     */
    public CredentialsCallbackHandler(final Credential credentials) {
        myCredentials = credentials;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overriden to provide the user's name and password to the
     * {@link NameCallback} and {@link PasswordCallback}. All other callback
     * types will throw a {@link UnsupportedCallbackException}.
     * </p>
     */
    @Override
    public void handle(final Callback[] callbacks)
            throws UnsupportedCallbackException {
        for (final Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                ((NameCallback) callback).setName(myCredentials.getUserName());
            }
            else if (callback instanceof PasswordCallback) {
                final char[] password = myCredentials.getPassword();
                ((PasswordCallback) callback).setPassword(password);
                Arrays.fill(password, ' ');
            }
            else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

}
