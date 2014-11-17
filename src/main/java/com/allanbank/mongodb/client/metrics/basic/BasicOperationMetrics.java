/*
 * #%L
 * BasicOperationMetrics.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.metrics.basic;

import java.io.PrintWriter;

import com.allanbank.mongodb.client.metrics.AbstractMetrics;

/**
 * BasicOperationMetrics collects the metrics for a single opertaion type.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BasicOperationMetrics extends AbstractMetrics {

    /** The name of the operation. */
    private final String myName;

    /**
     * Creates a new BasicOperationMetrics.
     * 
     * @param operation
     *            The name of the operation.
     */
    public BasicOperationMetrics(final String operation) {
        myName = operation;
    }

    /**
     * Returns the name of the operation.
     * 
     * @return The name of the operation.
     */
    public String getName() {
        return myName;
    }

    /**
     * Writes a human readable form of the operation metrics.
     * 
     * @param writer
     *            The writer to write to.
     */
    @Override
    public void writeTo(final PrintWriter writer) {
        writeTo(writer, "Operation", getName());
    }
}
