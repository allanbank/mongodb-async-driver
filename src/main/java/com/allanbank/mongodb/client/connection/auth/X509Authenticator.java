/*
 * #%L
 * X509Authenticator.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.client.FutureCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.message.Command;
import com.allanbank.mongodb.error.MongoDbAuthenticationException;

/**
 * X509Authenticator provides an {@link Authenticator} which will authenticate
 * the user using the {@value #MECHANISM}.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the
 *         extensions.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class X509Authenticator extends AbstractAuthenticator implements
        Authenticator {

    /** The database to authenticate against. */
    public static final String EXTERNAL = SaslResponseCallback.EXTERNAL;

    /** The mechanism being used: {@value} . */
    public static final String MECHANISM = "MONGODB-X509";

    /**
     * Creates a new X509Authenticator.
     */
    public X509Authenticator() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a new {@link X509Authenticator}.
     * </p>
     */
    @Override
    public X509Authenticator clone() {
        X509Authenticator clone;
        try {
            clone = (X509Authenticator) super.clone();
        }
        catch (final CloneNotSupportedException e) {
            clone = new X509Authenticator();
        }

        clone.myResults = new FutureCallback<Boolean>();

        return clone;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send the authentication command using the already establish
     * TLS client certificate.
     * </p>
     */
    @Override
    public void startAuthentication(final Credential credential,
            final Connection connection) throws MongoDbAuthenticationException {
        final DocumentBuilder cmd = BuilderFactory.start();
        cmd.add("authenticate", 1);
        cmd.add("user", credential.getUserName());
        cmd.add("mechanism", MECHANISM);

        connection.send(
                new Command(EXTERNAL, Command.COMMAND_COLLECTION, cmd.build()),
                new X509ResponseCallback(myResults));
    }

}
