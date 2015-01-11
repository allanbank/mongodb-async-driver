/*
 * #%L
 * X509AuthenticatorITest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.SocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.junit.After;
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
 * X509AuthenticatorITest provides integration tests for the x.509 support.
 * <p>
 * This test requires the {@code centos.&lt;domain&gt;} host to be running the
 * x.509 instance of MongoDB or a host be specified via the environment variable
 * {@code MONGODB_SERVER}.
 * </p>
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class X509AuthenticatorITest {

    /** Password for the keystores created in the tests. */
    public static final String PASSWORD = "password";

    /** The port the server with x.509 support is using. */
    public static final int PORT;

    /** Key store containing private keys. */
    public static final String PRIVATE_JKS = "src/test/resources/test.jks";

    /** The name of the server running with x.509 support enabled. */
    public static final String SERVER;

    /** The name of the test MONGODB. */
    public static final String TEST_MONGODB_HOST;

    /** The URI for connecting to the server with x.509. */
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

    /** The socket factory for the test. */
    private SocketFactory mySocketFactory = null;

    /**
     * Initializes the SSL Socket Factory.
     */
    @Before
    public void setUp() {
        InputStream stream = null;
        try {
            stream = new FileInputStream(PRIVATE_JKS);

            final KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(stream, PASSWORD.toCharArray());

            final X509TrustManager trust = new TrustAnyManager();
            final KeyManagerFactory keyFactory = KeyManagerFactory
                    .getInstance("PKIX");
            keyFactory.init(keyStore, PASSWORD.toCharArray());

            final SSLContext context = SSLContext.getInstance("SSL");
            context.init(keyFactory.getKeyManagers(),
                    new TrustManager[] { trust }, new SecureRandom());
            mySocketFactory = context.getSocketFactory();
        }
        catch (final NoSuchAlgorithmException e) {
            assumeNoException(e);
        }
        catch (final IOException e) {
            assumeNoException(e);
        }
        catch (final GeneralSecurityException e) {
            assumeNoException(e);
        }
        finally {
            IOUtils.close(stream);
        }
    }

    /**
     * Clears the test state.
     */
    @After
    public void tearDown() {
        mySocketFactory = null;
    }

    /**
     * Test method for {@link X509Authenticator}.
     *
     * @throws GeneralSecurityException
     *             On a failure to load the credentials.
     */
    @Test
    public void testAuthenticateReadWrite() throws GeneralSecurityException {
        final MongoClientConfiguration config = new MongoClientConfiguration(
                URI);
        config.addCredential(Credential.builder()
                .userName("CN=test-rsa.example.com,OU=example,OU=example2")
                .x509());
        config.setSocketFactory(mySocketFactory);

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
            assumeNoException("The MongoDB server with x.509 support ("
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

    /**
     * TrustAnyManager provides a trust manager that trusts everything.
     *
     * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected static final class TrustAnyManager
            implements X509TrustManager {
        /**
         * {@inheritDoc}
         */
        @Override
        public void checkClientTrusted(final X509Certificate[] arg0,
                final String arg1) throws CertificateException {
            // Good.
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void checkServerTrusted(final X509Certificate[] arg0,
                final String arg1) throws CertificateException {
            // Good.
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
