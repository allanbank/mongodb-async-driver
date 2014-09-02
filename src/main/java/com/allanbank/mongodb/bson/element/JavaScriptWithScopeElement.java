/*
 * #%L
 * JavaScriptWithScopeElement.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.allanbank.mongodb.bson.element;

import static com.allanbank.mongodb.util.Assertions.assertNotNull;

import java.util.Iterator;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.io.StringEncoder;

/**
 * A wrapper for a BSON JavaScript with Scope.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JavaScriptWithScopeElement extends JavaScriptElement {

    /** The BSON type for a string. */
    @SuppressWarnings("hiding")
    public static final ElementType TYPE = ElementType.JAVA_SCRIPT_WITH_SCOPE;

    /** Serialization version for the class. */
    private static final long serialVersionUID = -5697976862389984453L;

    /**
     * Computes and returns the number of bytes that are used to encode the
     * element.
     * 
     * @param name
     *            The name for the element.
     * @param javaScript
     *            The BSON JavaScript value.
     * @param scope
     *            The scope for the JavaScript
     * @return The size of the element when encoded in bytes.
     */
    private static long computeSize(final String name, final String javaScript,
            final Document scope) {
        long result = 11; // type (1) + name null byte (1) + length (4)
        // javaScript length (4) and null byte (1)
        result += StringEncoder.utf8Size(name);
        result += StringEncoder.utf8Size(javaScript);
        if (scope != null) {
            result += scope.size();
        }

        return result;
    }

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
        this(name, javaScript, scope, computeSize(name, javaScript, scope));

        assertNotNull(scope, "JavaScript element's scope cannot be null.");
    }

    /**
     * Constructs a new {@link JavaScriptWithScopeElement}.
     * 
     * @param name
     *            The name for the BSON string.
     * @param javaScript
     *            The BSON JavaScript value.
     * @param scope
     *            The scope for the JavaScript
     * @param size
     *            The size of the element when encoded in bytes. If not known
     *            then use the
     *            {@link JavaScriptWithScopeElement#JavaScriptWithScopeElement(String, String, Document)}
     *            constructor instead.
     * @throws IllegalArgumentException
     *             If the {@code name}, {@code javaScript}, or {@code scope} is
     *             <code>null</code>.
     */
    public JavaScriptWithScopeElement(final String name,
            final String javaScript, final Document scope, final long size) {
        super(name, javaScript, size);

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
     * {@inheritDoc}
     * <p>
     * Overridden to compare the Java Script (as text) if the base class
     * comparison is equals.
     * </p>
     */
    @Override
    public int compareTo(final Element otherElement) {
        int result = super.compareTo(otherElement);

        if (result == 0) {
            final JavaScriptWithScopeElement other = (JavaScriptWithScopeElement) otherElement;

            final Iterator<Element> thisIter = myScope.iterator();
            final Iterator<Element> otherIter = other.myScope.iterator();
            while (thisIter.hasNext() && otherIter.hasNext()) {
                result = thisIter.next().compareTo(otherIter.next());
                if (result != 0) {
                    return result;
                }
            }

            if (thisIter.hasNext()) {
                return 1;
            }
            else if (otherIter.hasNext()) {
                return -1;
            }
            else {
                return 0;
            }
        }

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
     * {@inheritDoc}
     * <p>
     * Returns a document representing the code and scope similar to the strict
     * JSON encoding.
     * </p>
     * <p>
     * <b>Note:</b> This value will not be recreated is a Object-->Element
     * conversion. A more generic sub-document is created instead.
     * </p>
     */
    @Override
    public Document getValueAsObject() {
        final DocumentBuilder b = BuilderFactory.start();
        b.add("$code", getJavaScript());
        b.add("$scope", myScope);

        return b.build();
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
     * {@inheritDoc}
     * <p>
     * Returns a new {@link JavaScriptElement}.
     * </p>
     */
    @Override
    public JavaScriptWithScopeElement withName(final String name) {
        if (getName().equals(name)) {
            return this;
        }
        return new JavaScriptWithScopeElement(name, getJavaScript(), myScope);
    }
}
