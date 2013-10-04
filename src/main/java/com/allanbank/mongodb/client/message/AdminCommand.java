/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import com.allanbank.mongodb.bson.Document;

/**
 * Helper class to make generating administration command queries easier.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
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
