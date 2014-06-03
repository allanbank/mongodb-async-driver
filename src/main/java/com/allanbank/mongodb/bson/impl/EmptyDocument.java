/*
 * #%L
 * EmptyDocument.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.bson.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.allanbank.mongodb.bson.Element;

/**
 * An immutable empty document.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class EmptyDocument extends AbstractDocument {

    /** An instance of the Empty Document. */
    public static final EmptyDocument INSTANCE = new EmptyDocument();

    /**
     * The bytes to encode an empty document. Length (4) + terminal null (1) =
     * 5.
     */
    public static final int SIZE = 5;

    /** Serialization version for the class. */
    private static final long serialVersionUID = -2775918328146027036L;

    /**
     * Constructs a new {@link EmptyDocument}.
     */
    public EmptyDocument() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return an empty list of elements.
     * </p>
     * 
     * @return The elements in the document.
     */
    @Override
    public List<Element> getElements() {
        return Collections.emptyList();
    }

    /**
     * Returns the size of the empty document when encoded as bytes. This is
     * always 5 bytes.
     * 
     * @return The size of the document when encoded as bytes.
     */
    @Override
    public long size() {
        return SIZE; // length (4) + terminal null (1).
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return an empty map as this document is always empty.
     * </p>
     */
    @Override
    protected Map<String, Element> getElementMap() {
        return Collections.emptyMap();
    }
}
