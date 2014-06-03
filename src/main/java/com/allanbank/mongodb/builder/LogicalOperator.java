/*
 * #%L
 * LogicalOperator.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.builder;

import com.allanbank.mongodb.Version;

/**
 * LogicalOperator provides the set of logical operators.
 * 
 * @api.yes This enumeration is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum LogicalOperator implements Operator {

    /** The logical conjunction operator. */
    AND("$and"),

    /** The logical negated disjunction operator. */
    NOR("$nor"),

    /** The logical negation operator. */
    NOT("$not"),

    /** The logical disjunction operator. */
    OR("$or");

    /** The operator's token to use when sending to the server. */
    private final String myToken;

    /** The first MongoDB version to support the operator. */
    private final Version myVersion;

    /**
     * Creates a new LogicalOperator.
     * 
     * @param token
     *            The token to use when sending to the server.
     */
    private LogicalOperator(final String token) {
        this(token, null);
    }

    /**
     * Creates a new LogicalOperator.
     * 
     * @param token
     *            The token to use when sending to the server.
     * @param version
     *            The first MongoDB version to support the operator.
     */
    private LogicalOperator(final String token, final Version version) {
        myToken = token;
        myVersion = version;
    }

    /**
     * The token for the operator that can be sent to the server.
     * 
     * @return The token for the operator.
     */
    @Override
    public String getToken() {
        return myToken;
    }

    /**
     * Returns the first MongoDB version to support the operator.
     * 
     * @return The first MongoDB version to support the operator. If
     *         <code>null</code> then the version is not known and can be
     *         assumed to be all currently supported versions.
     */
    @Override
    public Version getVersion() {
        return myVersion;
    }
}
