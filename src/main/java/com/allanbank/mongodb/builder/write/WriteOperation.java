/*
 * #%L
 * WriteOperation.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.builder.write;

import java.io.Serializable;

import com.allanbank.mongodb.bson.Document;

/**
 * WriteOperation provides a common interface for all types of writes:
 * {@link InsertOperation inserts}, {@link UpdateOperation updates}, and
 * {@link DeleteOperation deletes}.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface WriteOperation extends Serializable {
    /**
     * Returns the document that should be used to route the write operation.
     * 
     * @return The document that should be used to route the write operation.
     */
    public Document getRoutingDocument();

    /**
     * Returns the type of write. Can be used to avoid instance of calls and in
     * switch statements.
     * 
     * @return The type of write.
     */
    public WriteOperationType getType();
}