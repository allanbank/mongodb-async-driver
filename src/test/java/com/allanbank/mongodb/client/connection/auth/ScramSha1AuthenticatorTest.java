/*
 * #%L
 * ScramSha1AuthenticatorTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import javax.security.sasl.SaslException;

import org.junit.Test;

import com.allanbank.mongodb.Credential;

/**
 * ScramSha1AuthenticatorTest provides tests for the
 * {@link ScramSha1Authenticator}.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ScramSha1AuthenticatorTest {

    /**
     * Test method for {@link ScramSha1Authenticator#clone()}.
     */
    @Test
    public void testClone() {
        final ScramSha1Authenticator auth = new ScramSha1Authenticator();

        assertThat(auth.clone(), not(sameInstance(auth)));
    }

    /**
     * Test method for {@link ScramSha1Authenticator#createSaslClient}.
     *
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testCreateSaslClient() throws SaslException {
        final Credential credential = Credential.builder().userName("user")
                .password("pencil".toCharArray()).build();

        final ScramSha1Authenticator auth = new ScramSha1Authenticator();

        assertThat(auth.createSaslClient(credential, null),
                instanceOf(ScramSaslClient.class));
    }

    /**
     * Test method for {@link ScramSha1Authenticator#getMechanism()}.
     */
    @Test
    public void testGetMechanism() {
        final ScramSha1Authenticator auth = new ScramSha1Authenticator();

        assertThat(auth.getMechanism(), is(ScramSaslClient.MECHANISM));
    }

}
