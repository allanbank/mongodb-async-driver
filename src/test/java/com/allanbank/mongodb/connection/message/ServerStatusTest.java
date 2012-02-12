/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.allanbank.mongodb.connection.message.AdminCommand;
import com.allanbank.mongodb.connection.message.Command;
import com.allanbank.mongodb.connection.message.ServerStatus;

/**
 * ServerStatusTest provides tests for the {@link ServerStatus} message.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
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
        assertEquals(1, serverStatus.getNumberToReturn());
        assertEquals(0, serverStatus.getNumberToSkip());
        assertEquals(ServerStatus.SERVER_STATUS, serverStatus.getQuery());
        assertNull(serverStatus.getReturnFields());
        assertFalse(serverStatus.isAwaitData());
        assertFalse(serverStatus.isExhaust());
        assertFalse(serverStatus.isNoCursorTimeout());
        assertFalse(serverStatus.isPartial());
        assertFalse(serverStatus.isReplicaOk());
        assertFalse(serverStatus.isTailable());
    }

}
