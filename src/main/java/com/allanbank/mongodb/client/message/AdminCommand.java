/*
 * #%L
 * AdminCommand.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
        super(ADMIN_DATABASE, COMMAND_COLLECTION, commandDocument);
    }
}
