/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson;

/**
 * Interface for a document.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Document extends Iterable<Element> {

	/**
	 * Accepts the visitor and calls the appropriate method on the visitor based
	 * on the document type.
	 * 
	 * @param visitor
	 *            THe visitor for the document.
	 */
	public void accept(Visitor visitor);

	/**
	 * Returns true if the document contains an element with the specified name.
	 * 
	 * @param name
	 *            The name of the element to locate.
	 * @return True if the document contains an element with the given name,
	 *         false otherwise.
	 */
	public boolean contains(String name);

	/**
	 * Returns the element with the specified name or null if no element with
	 * that name exists.
	 * 
	 * @param name
	 *            The name of the element to locate.
	 * @return The sub-element in the document with the given name or null if
	 *         element exists with the given name.
	 */
	public Element get(String name);
}
