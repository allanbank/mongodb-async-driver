/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;

/**
 * ServerStatusTest provides tests for the {@link ServerStatus} message.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerStatusTest {

    /**
     * Test method for {@link ServerStatus#ServerStatus()}.
     */
    @Test
    public void testServerStatus() {
        final ServerStatus serverStatus = new ServerStatus();

        assertEquals(AdminCommand.ADMIN_DATABASE,
                serverStatus.getDatabaseName());
        assertEquals(Command.COMMAND_COLLECTION,
                serverStatus.getCollectionName());
        assertEquals(ServerStatus.SERVER_STATUS, serverStatus.getCommand());
        assertSame(ReadPreference.PRIMARY, serverStatus.getReadPreference());
        assertThat(serverStatus.getRequiredVersionRange(), nullValue());
    }
}
