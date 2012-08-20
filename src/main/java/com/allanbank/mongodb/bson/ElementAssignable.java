/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson;

/**
 * ElementAssignable provides a common interface for objects that can be
 * converted into an {@link Element}.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface ElementAssignable {

    /**
     * Converts the object into an {@link Element}.
     * 
     * @return The object as an {@link Element}.
     */
    public Element asElement();
}
