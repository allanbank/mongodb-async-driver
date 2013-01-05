/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.acceptance;

import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.net.URLClassLoader;

import javax.net.SocketFactory;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.util.IOUtils;

/**
 * StandAloneAcceptanceTest performs acceptance tests for the driver against a
 * standalone MongoDB shard server.
 * <p>
 * These are not meant to be exhaustive tests of the driver but instead attempt
 * to demonstrate that interactions with the MongoDB processes work.
 * </p>
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class UnixDomainSocketAcceptanceTest extends BasicAcceptanceTestCases {

    /** The local copy of the jar file release. */
    public static final File JAR_FILE;

    /** The name of the last junixsocket release. */
    public static final String LAST_JUNIXSOCKET_RELEASE;

    /** The name of the last junixsocket release. */
    public static final String LAST_JUNIXSOCKET_RELEASE_FILE;

    /** The local copy of the libraries directory from the release. */
    public static final File LIB_DIR;

    /** The local copy of the tar file release. */
    public static final File TAR_FILE;

    /** The Classloader for the junixsocket library. */
    protected static URLClassLoader ourJUnixSocketLibClassLoader = null;

    /** A socket address for connecting to the MongoDB server. */
    protected static InetSocketAddress ourSocketAddress = null;

    static {
        LAST_JUNIXSOCKET_RELEASE = "junixsocket-1.3";
        LAST_JUNIXSOCKET_RELEASE_FILE = LAST_JUNIXSOCKET_RELEASE
                + "-bin.tar.bz2";
        TAR_FILE = new File("target/" + LAST_JUNIXSOCKET_RELEASE_FILE);
        LIB_DIR = new File("target/" + LAST_JUNIXSOCKET_RELEASE
                + "/lib-native/");
        JAR_FILE = new File("target/" + LAST_JUNIXSOCKET_RELEASE + "/dist/"
                + LAST_JUNIXSOCKET_RELEASE + ".jar");
    }

    /**
     * Starts the standalone server before the tests.
     */
    @BeforeClass
    public static void startServer() {
        startStandAlone();

        FileOutputStream out = null;
        InputStream in = null;
        try {
            if (TAR_FILE.exists()) {
                TAR_FILE.delete();
            }
            final URL url = new URL("http://junixsocket.googlecode.com/files/"
                    + LAST_JUNIXSOCKET_RELEASE_FILE);
            out = new FileOutputStream(TAR_FILE);
            in = url.openStream();

            final byte[] buffer = new byte[4096];
            int read = 0;
            while (0 <= read) {
                out.write(buffer, 0, read);
                read = in.read(buffer);
            }
            out.flush();
            out.close();

            // Now untar the file.
            final ProcessBuilder builder = new ProcessBuilder("tar", "xfj",
                    TAR_FILE.getCanonicalPath());
            builder.directory(TAR_FILE.getParentFile());
            final Process untar = builder.start();
            untar.waitFor();
            assumeTrue(LIB_DIR.isDirectory());
            assumeTrue(JAR_FILE.isFile());

            // Adjust the library path to load the native library.
            System.setProperty("org.newsclub.net.unix.library.path",
                    LIB_DIR.getCanonicalPath());

            // Setup a classloader to load the jar.
            ourJUnixSocketLibClassLoader = new URLClassLoader(
                    new URL[] { JAR_FILE.toURI().toURL() });

            // Create the Socket address to use.
            final Class<?> clazz = ourJUnixSocketLibClassLoader
                    .loadClass("org.newsclub.net.unix.AFUNIXSocketAddress");
            ourSocketAddress = (InetSocketAddress) clazz.getConstructor(
                    File.class)
                    .newInstance(new File("/tmp/mongodb-27017.sock"));

        } catch (final Exception e) {
            // Reflection throws too many types and any exception should abort
            // anyway.
            e.printStackTrace();
            assumeNoException(e);
        } finally {
            IOUtils.close(out);
            IOUtils.close(in);
        }
    }

    /**
     * Stops the servers running in a standalone mode.
     */
    @AfterClass
    public static void stopServer() {
        ourSocketAddress = null;
        ourJUnixSocketLibClassLoader = null;
        System.clearProperty("org.newsclub.net.unix.library.path");

        ourClusterTestSupport.run(TAR_FILE.getParentFile(), "rm", "-rf",
                LAST_JUNIXSOCKET_RELEASE);

        stopStandAlone();

        if (TAR_FILE.exists()) {
            TAR_FILE.delete();
        }

    }

    /**
     * Sets up to create a connection to MongoDB.
     */
    @Override
    @Before
    public void connect() {
        myConfig = new MongoClientConfiguration();
        myConfig.addServer(ourSocketAddress);
        myConfig.setSocketFactory(new UnixDomainSocketFactory());

        super.connect();
    }

    /**
     * UnixDomainSocketFactory provides a {@link SocketFactory} implementation
     * based on reflection into the junixsocket library.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static class UnixDomainSocketFactory extends SocketFactory {

        /**
         * Creates a org.newsclub.net.unix.AFUNIXSocket via reflection.
         */
        @Override
        public Socket createSocket() throws java.io.IOException {
            try {
                final Class<?> clazz = ourJUnixSocketLibClassLoader
                        .loadClass("org.newsclub.net.unix.AFUNIXSocket");

                return (Socket) clazz.getMethod("newInstance").invoke(null);
            } catch (final Exception error) {
                final SocketException socketError = new SocketException(
                        error.getMessage());
                socketError.initCause(error);
                throw socketError;
            }
        }

        /**
         * Always throws a {@link SocketException}.
         */
        @Override
        public Socket createSocket(final InetAddress host, final int port)
                throws SocketException {
            throw new SocketException(
                    "AFUNIX socket does not support connections to a host/port");
        }

        /**
         * Always throws a {@link SocketException}.
         */
        @Override
        public Socket createSocket(final InetAddress address, final int port,
                final InetAddress localAddress, final int localPort)
                throws SocketException {
            throw new SocketException(
                    "AFUNIX socket does not support connections to a host/port");
        }

        /**
         * Always throws a {@link SocketException}.
         */
        @Override
        public Socket createSocket(final String host, final int port)
                throws SocketException {
            throw new SocketException(
                    "AFUNIX socket does not support connections to a host/port");
        }

        /**
         * Always throws a {@link SocketException}.
         */
        @Override
        public Socket createSocket(final String host, final int port,
                final InetAddress localHost, final int localPort)
                throws SocketException {
            throw new SocketException(
                    "AFUNIX socket does not support connections to a host/port");
        }

    }
}
