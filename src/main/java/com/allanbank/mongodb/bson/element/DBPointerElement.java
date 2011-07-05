/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a deprecated BSON DB Pointer element.
 * 
 * @deprecated See BSON Specification.
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
@Deprecated
public class DBPointerElement extends ObjectIdElement {

	/** The BSON type for a Object Id. */
	@SuppressWarnings("hiding")
	public static final ElementType TYPE = ElementType.DB_POINTER;

	/**
	 * Constructs a new {@link DBPointerElement}.
	 * 
	 * @param name
	 *            The name for the BSON Object Id.
	 * @param timestamp
	 *            The timestamp.
	 * @param machineId
	 *            The machine id.
	 */
	public DBPointerElement(final String name, final int timestamp,
			final long machineId) {
		super(TYPE, name, timestamp, machineId);
	}

	/**
	 * Accepts the visitor and calls the {@link Visitor#visitDBPointer} method.
	 * 
	 * @see Element#accept(Visitor)
	 */
	@Override
	public void accept(final Visitor visitor) {
		visitor.visitDBPointer(getName(), getTimestamp(), getMachineId());
	}

	/**
	 * Determines if the passed object is of this same type as this object and
	 * if so that its fields are equal.
	 * 
	 * @param object
	 *            The object to compare to.
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object object) {
		return super.equals(object);
	}

	/**
	 * Computes a reasonable hash code.
	 * 
	 * @return The hash code value.
	 */
	@Override
	public int hashCode() {
		return super.hashCode();
	}

	/**
	 * String form of the object.
	 * 
	 * @return A human readable form of the object.
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return super.toString();
	}
}
