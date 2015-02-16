/*
 * #%L
 * IdentityTransform.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client;

/**
 * IdentityTransform provides a {@link Transform} that returns the input object.
 * 
 * @param <T>
 *            The type for the transform.
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
public class IdentityTransform<T>
        implements Transform<T, T> {
    /**
     * Returns the {@link IdentityTransform} instance.
     * 
     * @param <T>
     *            The type for the transforms input and output.
     * @return The {@link IdentityTransform}.
     */
    @SuppressWarnings("unchecked")
    public static <T> Transform<T, T> identity() {
        return (Transform<T, T>) INSTANCE;
    }

    /**
     * Returns the {@link IdentityTransform} instance.
     * 
     * @param <T>
     *            The type for the transforms input and output.
     * @param clazz
     *            The class for the transform for type coercion.
     * @return The {@link IdentityTransform}.
     */
    @SuppressWarnings({ "unchecked", "unused" })
    public static <T> Transform<T, T> identity(Class<T> clazz) {
        return (Transform<T, T>) INSTANCE;
    }

    /** The instance of the IdentityTransform. */
    private static final IdentityTransform<?> INSTANCE = new IdentityTransform<Object>();

    /**
     * Creates a new IdentityTransform.
     */
    private IdentityTransform() {
        // There can be only one!
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the input.
     * </p>
     * 
     * @see Transform#transform
     */
    @Override
    public T transform(T input) {
        return input;
    }
}
