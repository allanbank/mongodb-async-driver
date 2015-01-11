/*
 * #%L
 * Operation.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client;

/**
 * Enumeration of the possible operations allowed in MongoDB messages.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum Operation {
    /** Delete documents. */
    DELETE(2006),

    /** Get more data from a query. */
    GET_MORE(2005),

    /** Insert new document. */
    INSERT(2002),

    /** Tell database client is done with a cursor. */
    KILL_CURSORS(2007),

    /** Query a collection. */
    QUERY(2004),

    /** Reply to a client request. */
    REPLY(1),

    /** Update a document. */
    UPDATE(2001);

    /**
     * Returns the {@link Operation} for the provided opCode.
     *
     * @param opCode
     *            The operation code for the {@link Operation}.
     * @return The {@link Operation} for the operation code or <code>null</code>
     *         if the operation code is invalid.
     */
    public static Operation fromCode(final int opCode) {
        if (opCode == REPLY.getCode()) {
            return REPLY;
        }
        else if (opCode == DELETE.getCode()) {
            return DELETE;
        }
        else if (opCode == GET_MORE.getCode()) {
            return GET_MORE;
        }
        else if (opCode == INSERT.getCode()) {
            return INSERT;
        }
        else if (opCode == KILL_CURSORS.getCode()) {
            return KILL_CURSORS;
        }
        else if (opCode == QUERY.getCode()) {
            return QUERY;
        }
        else if (opCode == UPDATE.getCode()) {
            return UPDATE;
        }
        return null;
    }

    /** The operation code. */
    private final int myCode;

    /**
     * Creates a new Operation.
     *
     * @param code
     *            The operations code.
     */
    private Operation(final int code) {
        myCode = code;
    }

    /**
     * Returns the Operation's code.
     *
     * @return The operation's code.
     */
    public int getCode() {
        return myCode;
    }
}
