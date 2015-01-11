/*
 * #%L
 * Visitor.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.util.List;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.Visitor;

/**
 * Extension for visitors that could benefit from knowledge of the size of the
 * array and document elements.
 *
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface SizeAwareVisitor
        extends Visitor {

    /**
     * Visits an array of elements.
     * <p>
     * The {@link com.allanbank.mongodb.bson.element.ArrayElement}
     * implementation ensures that the list of elements is always the same list.
     * Visitors may use this fact to cache intermediate results.
     * </p>
     *
     * @param name
     *            The name of the element.
     * @param elements
     *            The elements in the array.
     * @param totalSize
     *            The total size of the
     *            {@link com.allanbank.mongodb.bson.element.ArrayElement}.
     */
    public void visitArray(String name, List<Element> elements, long totalSize);

    /**
     * Visits a sub-document element.
     * <p>
     * The {@link com.allanbank.mongodb.bson.element.DocumentElement}
     * implementation ensures that the list of elements is always the same list.
     * Visitors may use this fact to cache intermediate results.
     * </p>
     *
     * @param name
     *            The name of the element.
     * @param elements
     *            The sub elements of the document.
     * @param totalSize
     *            The total size of the
     *            {@link com.allanbank.mongodb.bson.element.DocumentElement}.
     */
    public void visitDocument(String name, List<Element> elements,
            long totalSize);
}
