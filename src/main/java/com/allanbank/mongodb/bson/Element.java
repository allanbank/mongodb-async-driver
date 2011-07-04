/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson;

/**
 * A base class for the basic BSON types.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Element {

	/**
	 * Returns the name for the BSON type.
	 * 
	 * @return The name for the BSON type.
	 */
	public String getName();

	/**
	 * Returns the type for the BSON type.
	 * 
	 * @return The type for the BSON type.
	 */
	public ElementType getType();

	/**
	 * Accepts the visitor and calls the appropriate method on the visitor based
	 * on the element type.
	 * 
	 * @param visitor
	 *            The visitor for the element.
	 */
	public void accept(Visitor visitor);
}
