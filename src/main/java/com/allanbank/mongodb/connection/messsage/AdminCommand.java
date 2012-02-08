/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.messsage;

import com.allanbank.mongodb.bson.Document;

/**
 * Helper class to make generating administration command queries easier.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AdminCommand extends Command {

    /** The administration database name. */
    public static final String ADMIN_DATABASE = "admin";

    /**
     * Create a new Command.
     * 
     * @param commandDocument
     *            The command document containing the command and options.
     */
    public AdminCommand(final Document commandDocument) {
        super(ADMIN_DATABASE, commandDocument);
    }
}
