/*
 * #%L
 * ScramSha1Authenticator.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.client.FutureCallback;
import com.allanbank.mongodb.client.connection.Connection;

/**
 * ScramSha1Authenticator provides an {@link Authenticator} using
 * {@value #MECHANISM}.
 * <p>
 * This authenticator does not support any credential options.
 * </p>
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the
 *         extensions.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ScramSha1Authenticator extends AbstractSaslAuthenticator implements
        Authenticator {

    /** The SASL mechanism being used: {@value ScramSaslClient#MECHANISM}. */
    public static final String MECHANISM = ScramSaslClient.MECHANISM;

    /**
     * Creates a new ScramSha1Authenticator.
     */
    public ScramSha1Authenticator() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a new {@link ScramSha1Authenticator}.
     * </p>
     */
    @Override
    public ScramSha1Authenticator clone() {
        try {
            final ScramSha1Authenticator newAuth = (ScramSha1Authenticator) super
                    .clone();

            newAuth.myResults = new FutureCallback<Boolean>();

            return newAuth;
        }
        catch (final CloneNotSupportedException e) {
            // Cannot happen!
            return new ScramSha1Authenticator();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to create the {@link ScramSaslClient}.
     * </p>
     */
    @Override
    protected SaslClient createSaslClient(final Credential credentials,
            final Connection connection) throws SaslException {
        return new ScramSaslClient(new CredentialsCallbackHandler(credentials));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return {@value #MECHANISM}.
     * </p>
     */
    @Override
    protected String getMechanism() {
        return MECHANISM;
    }
}
