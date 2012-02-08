/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.messsage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * IsMasterTest provides tests for the {@link IsMaster} message.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class IsMasterTest {

    /**
     * Test method for {@link IsMaster#IsMaster()}.
     */
    @Test
    public void testIsMaster() {
        final IsMaster isMaster = new IsMaster();

        assertEquals(AdminCommand.ADMIN_DATABASE, isMaster.getDatabaseName());
        assertEquals(Command.COMMAND_COLLECTION, isMaster.getCollectionName());
        assertEquals(1, isMaster.getNumberToReturn());
        assertEquals(0, isMaster.getNumberToSkip());
        assertEquals(IsMaster.IS_MASTER, isMaster.getQuery());
        assertNull(isMaster.getReturnFields());
        assertFalse(isMaster.isAwaitData());
        assertFalse(isMaster.isExhaust());
        assertFalse(isMaster.isNoCursorTimeout());
        assertFalse(isMaster.isPartial());
        assertFalse(isMaster.isReplicaOk());
        assertFalse(isMaster.isTailable());
    }

}
