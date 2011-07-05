/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON JavaScript with Scope.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JavaScriptWithScopeElement extends JavaScriptElement {

	/** The BSON type for a string. */
	@SuppressWarnings("hiding")
	public static final ElementType TYPE = ElementType.JAVA_SCRIPT_WITH_SCOPE;

	/** The BSON scope value. */
	private final Document myScope;

	/**
	 * Constructs a new {@link JavaScriptWithScopeElement}.
	 * 
	 * @param name
	 *            The name for the BSON string.
	 * @param javaScript
	 *            The BSON JavaScript value.
	 * @param scope
	 *            The scope for the JavaScript
	 */
	public JavaScriptWithScopeElement(final String name,
			final String javaScript, final Document scope) {
		super(TYPE, name, javaScript);

		myScope = scope;
	}

	/**
	 * Accepts the visitor and calls the
	 * {@link Visitor#visitJavaScript(String,String,Document)} method.
	 * 
	 * @see Element#accept(Visitor)
	 */
	@Override
	public void accept(final Visitor visitor) {
		visitor.visitJavaScript(getName(), getJavaScript(), getScope());
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
		boolean result = false;
		if (this == object) {
			result = true;
		} else if ((object != null) && (getClass() == object.getClass())) {
			final JavaScriptWithScopeElement other = (JavaScriptWithScopeElement) object;

			result = super.equals(object)
					&& nullSafeEquals(myScope, other.myScope);
		}
		return result;
	}

	/**
	 * Returns the BSON JavaScript scope.
	 * 
	 * @return The BSON JavaScript scope.
	 */
	public Document getScope() {
		return myScope;
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
		result = 31 * result + ((myScope != null) ? myScope.hashCode() : 3);
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
		final StringBuilder builder = new StringBuilder();

		builder.append('"');
		builder.append(getName());
		builder.append("\" : ");
		builder.append(getJavaScript());
		builder.append(" (scope :");
		builder.append(myScope);
		builder.append(")");

		return builder.toString();
	}
}
