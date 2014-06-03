/*
 * #%L
 * NullElement.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.io.StringEncoder;

/**
 * A wrapper for a BSON null.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class NullElement extends AbstractElement {

    /** The BSON type for a binary. */
    public static final ElementType TYPE = ElementType.NULL;

    /** Serialization version for the class. */
    private static final long serialVersionUID = -4974513577366947524L;

    /**
     * Computes and returns the number of bytes that are used to encode the
     * element.
     * 
     * @param name
     *            The name for the element.
     * @return The size of the element when encoded in bytes.
     */
    private static long computeSize(final String name) {
        long result = 2; // type (1) + name null byte (1).
        result += StringEncoder.utf8Size(name);

        return result;
    }

    /**
     * Constructs a new {@link NullElement}.
     * 
     * @param name
     *            The name for the BSON null.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public NullElement(final String name) {
        this(name, computeSize(name));
    }

    /**
     * Constructs a new {@link NullElement}.
     * 
     * @param name
     *            The name for the BSON null.
     * @param size
     *            The size of the element when encoded in bytes. If not known
     *            then use the {@link NullElement#NullElement(String)}
     *            constructor instead.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public NullElement(final String name, final long size) {
        super(name, size);
    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitNull} method.
     * 
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitNull(getName());
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
     * Returns <code>null</code>.
     * </p>
     */
    @Override
    public Object getValueAsObject() {
        return null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the result of {@link String#valueOf(Object) String.valueOf(null)}
     * .
     * </p>
     */
    @Override
    public String getValueAsString() {
        return String.valueOf((Object) null);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link NullElement}.
     * </p>
     */
    @Override
    public NullElement withName(final String name) {
        if (getName().equals(name)) {
            return this;
        }
        return new NullElement(name);
    }
}
