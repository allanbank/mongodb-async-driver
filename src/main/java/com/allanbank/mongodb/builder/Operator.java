/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import com.allanbank.mongodb.Version;

/**
 * Operator provides an enumeration of all possible operators.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Operator {

    /**
     * The token for the operator that can be sent to the server.
     * 
     * @return The token for the operator.
     */
    public String getToken();

    /**
     * Returns the first MongoDB version to support the operator.
     * 
     * @return The first MongoDB version to support the operator. If
     *         <code>null</code> then the version is not known and can be
     *         assumed to be all currently supported versions.
     */
    public Version getVersion();
}
