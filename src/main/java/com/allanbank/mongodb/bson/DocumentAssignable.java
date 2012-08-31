/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson;

/**
 * DocumentAssignable provides a common interface for objects that can be
 * converted into a document.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface DocumentAssignable {

    /**
     * Converts the object into a document.
     * 
     * @return The object as a Document.
     */
    public Document asDocument();
}
