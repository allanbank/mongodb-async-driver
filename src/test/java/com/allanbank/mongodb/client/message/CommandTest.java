/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * CommandTest provides tests for the {@link Command} class.
 *
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CommandTest {

    /**
     * Test method for {@link Command#getOperationName()}.
     */
    @Test
    public void testGetOperationNameWithEmptyCommandDocument() {
        final Command command = new Command("db", BuilderFactory.start()
                .build());

        assertThat(command.getOperationName(), is("command"));
    }

}
