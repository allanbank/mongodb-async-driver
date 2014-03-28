/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder.write;

/**
 * WriteOperationType provides an enumeration of the types of writes.
 * 
 * @api.yes This enumeration is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum WriteOperationType {
    /** A delete operation. */
    DELETE,

    /** An insert operation. */
    INSERT,

    /** An update operation. */
    UPDATE;
}
