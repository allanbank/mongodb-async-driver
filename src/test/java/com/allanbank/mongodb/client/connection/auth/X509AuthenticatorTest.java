/*
 * #%L
 * X509AuthenticatorTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import org.easymock.Capture;
import org.junit.Test;

import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.message.Command;

/**
 * X509AuthenticatorTest provides tests for the {@link X509Authenticator}.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class X509AuthenticatorTest {

    /**
     * Test method for {@link X509Authenticator#clone()} .
     */
    @Test
    public void testClone() {
        final X509Authenticator auth = new X509Authenticator();

        final X509Authenticator clone = auth.clone();

        assertThat(clone, not(sameInstance(auth)));
        assertThat(clone.myResults, not(sameInstance(auth.myResults)));
    }

    /**
     * Test method for
     * {@link X509Authenticator#startAuthentication(Credential, Connection)} .
     */
    @Test
    public void testStartAuthentication() {
        final Credential credential = Credential.builder().userName("CN=foo")
                .build();
        final Connection mockConnection = createMock(Connection.class);
        final Capture<ReplyCallback> capture = new Capture<ReplyCallback>();

        final DocumentBuilder b = BuilderFactory.start();
        b.add("authenticate", 1);
        b.add("user", "CN=foo");
        b.add("mechanism", X509Authenticator.MECHANISM);

        mockConnection.send(eq(new Command(X509Authenticator.EXTERNAL,
                Command.COMMAND_COLLECTION, b.build())), capture(capture));
        expectLastCall();

        replay(mockConnection);

        final X509Authenticator auth = new X509Authenticator();

        auth.startAuthentication(credential, mockConnection);

        verify(mockConnection);

        assertThat(capture.getValue(), instanceOf(X509ResponseCallback.class));
    }

}
