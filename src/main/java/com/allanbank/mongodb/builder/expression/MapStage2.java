/*
 * #%L
 * MapStage2.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.builder.expression;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * MapStage2 provides a container for the {@code $map} input field name and
 * {@code as} variable name before adding the expression.
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class MapStage2 {

    /** The name of the {@code input} field. */
    private final String myInputField;

    /** The name of the {@code as} variable. */
    private final String myVariableName;

    /**
     * Creates a new MapStage2.
     *
     * @param inputField
     *            The name of the {@code input} field.
     * @param variableName
     *            The name of the {@code as} variable.
     */
    /* package */MapStage2(final String inputField, final String variableName) {
        myInputField = inputField;
        myVariableName = variableName;
    }

    /**
     * Creates the final {@code $map} expression to evaluate.
     *
     * @param mapOperation
     *            The expression to be evaluated with the variables within the
     *            {@code $map} expression.
     * @return The {@link UnaryExpression} for the {@code $map}.
     */
    public Expression in(final Expression mapOperation) {
        return Expressions.map(myInputField, myVariableName, mapOperation);
    }
}
