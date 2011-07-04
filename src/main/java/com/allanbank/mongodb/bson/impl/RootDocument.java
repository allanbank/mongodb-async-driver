/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A root level document.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class RootDocument implements Document {

	/** The elements of the document. */
	private final List<Element> myElements;

	/**
	 * Constructed when a user tries to access the elements of the document by
	 * name.
	 */
	private Map<String, Element> myElementMap;

	/**
	 * Constructs a new {@link RootDocument}.
	 * 
	 * @param elements
	 *            The elements for the BSON document.
	 */
	public RootDocument(Element... elements) {
		if (elements.length > 0) {
			myElements = new ArrayList<Element>(Arrays.asList(elements));
		} else {
			myElements = Collections.emptyList();
		}
	}

	/**
	 * Constructs a new {@link RootDocument}.
	 * 
	 * @param elements
	 *            The elements for the BSON document.
	 */
	public RootDocument(List<Element> elements) {
		if ((elements != null) && !elements.isEmpty()) {
			myElements = new ArrayList<Element>(elements);
		} else {
			myElements = Collections.emptyList();
		}

	}

	/**
	 * Returns the elements in the document.
	 * 
	 * @return The elements in the document.
	 */
	public List<Element> getElements() {
		return Collections.unmodifiableList(myElements);
	}

	/**
	 * Returns an iterator over the documents elements.
	 * 
	 * @see Iterable#iterator()
	 */
	@Override
	public Iterator<Element> iterator() {
		return getElements().iterator();
	}

	/**
	 * Returns the element with the specified name or null if no element with
	 * that name exists.
	 * 
	 * @see Document#get(String)
	 */
	@Override
	public Element get(String name) {
		return getElementMap().get(name);
	}

	/**
	 * Returns true if the document contains an element with the specified name.
	 * 
	 * @see Document#contains(String)
	 */
	@Override
	public boolean contains(String name) {
		return getElementMap().containsKey(name);
	}

	/**
	 * Returns a map from the element names to the elements in the document.
	 * Used for faster by-name access.
	 * 
	 * @return The element name to element mapping.
	 */
	private Map<String, Element> getElementMap() {
		if (myElementMap == null) {
			Map<String, Element> mapping = new HashMap<String, Element>(
					myElements.size() + myElements.size());

			for (Element element : myElements) {
				mapping.put(element.getName(), element);
			}

			// Swap the finished map into position.
			myElementMap = mapping;
		}

		return myElementMap;
	}

	/**
	 * Accepts the visitor and calls the {@link Visitor#visitDocument} method.
	 * 
	 * @see Element#accept(Visitor)
	 */
	@Override
	public void accept(Visitor visitor) {
		visitor.visit(getElements());
	}

	/**
	 * Computes a reasonable hash code.
	 * 
	 * @return The hash code value.
	 */
	@Override
	public int hashCode() {
		int result = 1;
		result = 31 * result + super.hashCode();
		result = 31 * result
				+ ((myElements == null) ? 0 : myElements.hashCode());
		return result;
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
	public boolean equals(Object object) {
		boolean result = false;
		if (this == object) {
			result = true;
		} else if ((object != null) && (getClass() == object.getClass())) {
			RootDocument other = (RootDocument) object;

			result = myElements.equals(other.myElements);
		}
		return result;
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
		StringBuilder builder = new StringBuilder();

		builder.append("{ ");

		boolean first = true;
		for (Element element : myElements) {
			if (!first) {
				builder.append(",\n");
			}
			builder.append(element.toString());
			first = false;
		}
		builder.append("}\n");

		return builder.toString();
	}

}
