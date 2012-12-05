/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
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
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JavaScriptWithScopeElement extends JavaScriptElement {

    /** The BSON type for a string. */
    @SuppressWarnings("hiding")
    public static final ElementType TYPE = ElementType.JAVA_SCRIPT_WITH_SCOPE;

    /** Serialization version for the class. */
    private static final long serialVersionUID = -5697976862389984453L;

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
     * @throws IllegalArgumentException
     *             If the {@code name}, {@code javaScript}, or {@code scope} is
     *             <code>null</code>.
     */
    public JavaScriptWithScopeElement(final String name,
            final String javaScript, final Document scope) {
        super(name, javaScript);

        assertNotNull(scope, "JavaScript element's scope cannot be null.");

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
        }
        else if ((object != null) && (getClass() == object.getClass())) {
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
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return TYPE;
    }

    /**
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + super.hashCode();
        result = (31 * result) + ((myScope != null) ? myScope.hashCode() : 3);
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

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link JavaScriptElement}.
     * </p>
     */
    @Override
    public JavaScriptWithScopeElement withName(final String name) {
        return new JavaScriptWithScopeElement(name, getJavaScript(), myScope);
    }
}
