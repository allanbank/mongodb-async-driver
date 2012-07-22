/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson;

/**
 * DocumentAssignable provides a common interface for objects that can be
 * converted into a document.
 * 
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
