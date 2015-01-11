/*
 * #%L
 * CredentialsCallbackHandlerTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.fail;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.junit.Test;

import com.allanbank.mongodb.Credential;

/**
 * CredentialsCallbackHandlerTest provides tests for the
 * {@link CredentialsCallbackHandler} class.
 *
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CredentialsCallbackHandlerTest {

    /**
     * Test method for {@link CredentialsCallbackHandler#handle(Callback[])}.
     */
    @Test
    public void testHandleWithNameAndPasswordCallback() {
        final Credential c = Credential.builder().userName("foo")
                .password("bar".toCharArray()).build();

        final NameCallback mockNameCallback = createMock(NameCallback.class);
        final PasswordCallback mockPasswordCallback = createMock(PasswordCallback.class);

        mockNameCallback.setName(c.getUserName());
        expectLastCall();
        mockPasswordCallback.setPassword(aryEq(c.getPassword()));
        expectLastCall();

        replay(mockNameCallback, mockPasswordCallback);

        final CredentialsCallbackHandler handler = new CredentialsCallbackHandler(
                c);

        try {
            handler.handle(new Callback[] { mockNameCallback,
                    mockPasswordCallback });
        }
        catch (final UnsupportedCallbackException e) {
            fail(e.getMessage());
        }

        verify(mockNameCallback, mockPasswordCallback);
    }

    /**
     * Test method for {@link CredentialsCallbackHandler#handle(Callback[])}.
     */
    @Test
    public void testHandleWithNameCallback() {
        final Credential c = Credential.builder().userName("foo")
                .password("bar".toCharArray()).build();

        final NameCallback mockCallback = createMock(NameCallback.class);

        mockCallback.setName(c.getUserName());
        expectLastCall();

        replay(mockCallback);

        final CredentialsCallbackHandler handler = new CredentialsCallbackHandler(
                c);

        try {
            handler.handle(new Callback[] { mockCallback });
        }
        catch (final UnsupportedCallbackException e) {
            fail(e.getMessage());
        }

        verify(mockCallback);
    }

    /**
     * Test method for {@link CredentialsCallbackHandler#handle(Callback[])}.
     */
    @Test
    public void testHandleWithNonPasswordOrNameCallback() {
        final Credential c = Credential.builder().userName("foo")
                .password("bar".toCharArray()).build();

        final Callback mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final CredentialsCallbackHandler handler = new CredentialsCallbackHandler(
                c);

        try {
            handler.handle(new Callback[] { mockCallback });
            fail("Should have thrown a UnsupportedCallbackException");
        }
        catch (final UnsupportedCallbackException e) {
            // Good.
        }

        verify(mockCallback);
    }

    /**
     * Test method for {@link CredentialsCallbackHandler#handle(Callback[])}.
     */
    @Test
    public void testHandleWithPasswordAndNameCallback() {
        final Credential c = Credential.builder().userName("foo")
                .password("bar".toCharArray()).build();

        final NameCallback mockNameCallback = createMock(NameCallback.class);
        final PasswordCallback mockPasswordCallback = createMock(PasswordCallback.class);

        mockNameCallback.setName(c.getUserName());
        expectLastCall();
        mockPasswordCallback.setPassword(aryEq(c.getPassword()));
        expectLastCall();

        replay(mockNameCallback, mockPasswordCallback);

        final CredentialsCallbackHandler handler = new CredentialsCallbackHandler(
                c);

        try {
            handler.handle(new Callback[] { mockPasswordCallback,
                    mockNameCallback });
        }
        catch (final UnsupportedCallbackException e) {
            fail(e.getMessage());
        }

        verify(mockNameCallback, mockPasswordCallback);
    }

    /**
     * Test method for {@link CredentialsCallbackHandler#handle(Callback[])}.
     */
    @Test
    public void testHandleWithPasswordCallback() {
        final Credential c = Credential.builder().userName("foo")
                .password("bar".toCharArray()).build();

        final PasswordCallback mockCallback = createMock(PasswordCallback.class);

        mockCallback.setPassword(aryEq(c.getPassword()));
        expectLastCall();

        replay(mockCallback);

        final CredentialsCallbackHandler handler = new CredentialsCallbackHandler(
                c);

        try {
            handler.handle(new Callback[] { mockCallback });
        }
        catch (final UnsupportedCallbackException e) {
            fail(e.getMessage());
        }

        verify(mockCallback);
    }

}
