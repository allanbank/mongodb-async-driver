/*
 * #%L
 * ScramSha1SaslAuthenticatorITest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNoException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;

import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.util.IOUtils;

/**
 * ScramSha1SaslAuthenticatorITest provides integration tests for the
 * SCRAM-SHA-1 SASL support.
 * <p>
 * This test requires the {@code centos.&lt;domain&gt;} host to be running the
 * SCRAM-SHA-1 SASL instance of MongoDB or a host be specified via the
 * environment variable {@code MONGODB_SERVER}.
 * </p>
 *
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ScramSha1SaslAuthenticatorITest {

    /** The port the server with Plain SASL support is using. */
    public static final int PORT;

    /** The name of the server running with Plain SASL support enabled. */
    public static final String SERVER;

    /** The name of the test MONGODB. */
    public static final String TEST_MONGODB_HOST;

    /** The URI for connecting to the server with Plain SASL. */
    public static final String URI;

    static {
        String domain = "localdomain";
        try {
            final InetAddress localAddress = InetAddress.getLocalHost();

            final String name = localAddress.getCanonicalHostName();
            final int firstDot = name.indexOf('.');
            if (firstDot > 0) {
                domain = name.substring(firstDot + 1);
            }
        }
        catch (final UnknownHostException e) {
            domain = "localdomain";
        }
        finally {
            TEST_MONGODB_HOST = "centos." + domain;
        }

        final String serverName = System.getenv("MONGODB_SERVER");
        final String port = System.getenv("MONGODB_PORT");
        int portNumber;
        try {
            portNumber = Integer.parseInt(port);
        }
        catch (final NumberFormatException nfe) {
            portNumber = 27017;
        }

        SERVER = (serverName != null) ? serverName : TEST_MONGODB_HOST;
        PORT = portNumber;
        URI = "mongodb://" + SERVER + ":" + PORT;
    }

    /**
     * Test method for {@link ScramSha1Authenticator}.
     *
     * @throws GeneralSecurityException
     *             On a failure to load the credentials.
     */
    @Test
    public void testAuthenticateReadWrite() throws GeneralSecurityException {
        final MongoClientConfiguration config = new MongoClientConfiguration(
                URI);
        config.addCredential(Credential.builder().userName("test_scram")
                .password("test_scram_bad_password".toCharArray()).scramSha1());

        final MongoClient client = MongoFactory.createClient(config);

        final MongoCollection collection = client.getDatabase("test")
                .getCollection("test");

        collection.insert(Durability.ACK, BuilderFactory.start());

        final MongoIterator<Document> iter = collection.find(BuilderFactory
                .start());
        assertTrue(iter.hasNext());

        collection.delete(MongoCollection.ALL);

        IOUtils.close(client);
    }

    /**
     * Starts the standalone server before the tests.
     */
    @Before
    public void verifyServerRunning() {
        Socket socket = null;
        try {
            socket = new Socket(SERVER, PORT);

            // Worked. We are good.
        }
        catch (final IOException error) {
            // Boom.
            assumeNoException("The MongoDB server with plain SASL support ("
                    + SERVER + ") is not running.", error);
        }
        finally {
            try {
                if (socket != null) {
                    socket.close();
                }
            }
            catch (final IOException error) {
                // Ignored.
            }
        }
    }
}
