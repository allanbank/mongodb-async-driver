/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON JavaScript.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JavaScriptElement extends AbstractElement {

	/** The BSON type for a string. */
	public static final ElementType TYPE = ElementType.JAVA_SCRIPT;

	/** The BSON string value. */
	private final String myJavaScript;

	/**
	 * Constructs a new {@link JavaScriptElement}.
	 * 
	 * @param name
	 *            The name for the BSON string.
	 * @param javaScript
	 *            The BSON JavaScript value.
	 */
	public JavaScriptElement(String name, String javaScript) {
		this(TYPE, name, javaScript);
	}

	/**
	 * Constructs a new {@link JavaScriptElement}.
	 * 
	 * @param type
	 *            The type of the inherited element.
	 * @param name
	 *            The name for the BSON string.
	 * @param javaScript
	 *            The BSON JavaScript value.
	 */
	protected JavaScriptElement(ElementType type, String name, String javaScript) {
		super(type, name);

		myJavaScript = javaScript;
	}

	/**
	 * Returns the BSON JavaScript value.
	 * 
	 * @return The BSON JavaScript value.
	 */
	public String getJavaScript() {
		return myJavaScript;
	}

	/**
	 * Accepts the visitor and calls the
	 * {@link Visitor#visitJavaScript(String,String)} method.
	 * 
	 * @see Element#accept(Visitor)
	 */
	@Override
	public void accept(Visitor visitor) {
		visitor.visitJavaScript(getName(), getJavaScript());
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
				+ ((myJavaScript != null) ? myJavaScript.hashCode() : 3);
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
			JavaScriptElement other = (JavaScriptElement) object;

			result = super.equals(object)
					&& nullSafeEquals(myJavaScript, other.myJavaScript);
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

		builder.append('"');
		builder.append(getName());
		builder.append("\" : ");
		builder.append(myJavaScript);

		return builder.toString();
	}
}
