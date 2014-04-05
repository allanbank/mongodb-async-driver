/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson;

/**
 * ElementAssignable provides a common interface for objects that can be
 * converted into an {@link Element}.
 *
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface ElementAssignable {

    /**
     * Converts the object into an {@link Element}.
     *
     * @return The object as an {@link Element}.
     */
    public Element asElement();
}
