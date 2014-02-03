/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.hamcrest.Matchers;
import org.junit.Test;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.VersionRange;
import com.allanbank.mongodb.client.message.IsMaster;

/**
 * ServerVersionExceptionTest provides tests for the
 * {@link ServerVersionException} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerVersionExceptionTest {

    /**
     * Test method for {@link MongoClientConfiguration} serialization.
     * 
     * @throws IOException
     *             On a failure reading or writing the config.
     * @throws ClassNotFoundException
     *             On a failure reading the config.
     */
    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        final Version actual = Version.parse("1.2.1");
        final Version required = Version.parse("1.2.3");
        final Message message = new IsMaster();

        final ServerVersionException ex = new ServerVersionException(
                "isMaster", VersionRange.minimum(required), actual, message);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream oout = new ObjectOutputStream(out);
        oout.writeObject(ex);
        oout.close();

        final ByteArrayInputStream in = new ByteArrayInputStream(
                out.toByteArray());
        final ObjectInputStream oin = new ObjectInputStream(in);

        final Object read = oin.readObject();

        assertThat(read, Matchers.instanceOf(ServerVersionException.class));

        final ServerVersionException readEx = (ServerVersionException) read;
        assertNull(readEx.getSentMessage());
    }

    /**
     * Test method for {@link ServerVersionException#ServerVersionException}.
     */
    @Test
    public void testServerVersionException() {
        final Version actual = Version.parse("1.2.1");
        final Version required = Version.parse("1.2.3");
        final Message message = new IsMaster();

        final ServerVersionException ex = new ServerVersionException(
                "isMaster", VersionRange.minimum(required), actual, message);

        assertThat(
                ex.getMessage(),
                is("Attempted to send the 'isMaster' operation to a version "
                        + "1.2.1 server but the operation is only supported after version 1.2.3."));
        assertThat(ex.getActualVersion(), sameInstance(actual));
        assertThat(ex.getOperation(), is("isMaster"));
        assertThat(ex.getRequiredVersion(), sameInstance(required));
        assertThat(ex.getMaximumVersion(), sameInstance(Version.UNKNOWN));
        assertThat(ex.getSentMessage(), sameInstance(message));
    }

    /**
     * Test method for {@link ServerVersionException#ServerVersionException}.
     */
    @Test
    public void testServerVersionNotInRange() {
        final Version actual = Version.parse("1.2.4");
        final Version required = Version.parse("1.2.2");
        final Version max = Version.parse("1.2.3");
        final Message message = new IsMaster();

        final ServerVersionException ex = new ServerVersionException(
                "isMaster", VersionRange.range(required, max), actual, message);

        assertThat(
                ex.getMessage(),
                is("Attempted to send the 'isMaster' operation to a version "
                        + "1.2.4 server but the operation is only supported from version 1.2.2 to 1.2.3."));
        assertThat(ex.getActualVersion(), sameInstance(actual));
        assertThat(ex.getOperation(), is("isMaster"));
        assertThat(ex.getRequiredVersion(), sameInstance(required));
        assertThat(ex.getMaximumVersion(), sameInstance(max));
        assertThat(ex.getSentMessage(), sameInstance(message));
    }

    /**
     * Test method for {@link ServerVersionException#ServerVersionException}.
     */
    @Test
    public void testServerVersionToOld() {
        final Version actual = Version.parse("1.2.4");
        final Version required = Version.parse("1.2.3");
        final Message message = new IsMaster();

        final ServerVersionException ex = new ServerVersionException(
                "isMaster", VersionRange.maximum(required), actual, message);

        assertThat(
                ex.getMessage(),
                is("Attempted to send the 'isMaster' operation to a version "
                        + "1.2.4 server but the operation is only supported before version 1.2.3."));
        assertThat(ex.getActualVersion(), sameInstance(actual));
        assertThat(ex.getOperation(), is("isMaster"));
        assertThat(ex.getRequiredVersion(), sameInstance(Version.VERSION_0));
        assertThat(ex.getMaximumVersion(), sameInstance(required));
        assertThat(ex.getSentMessage(), sameInstance(message));
    }

}
