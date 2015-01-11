/*
 * #%L
 * Builder.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.bson.builder;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Common interface for all builders.
 *
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@NotThreadSafe
public interface Builder {
    /**
     * Pops the sub-context. Since the outer context might be an array or a
     * document the appropriate builder type cannot be returned and the user
     * must maintain the appropriate outer scoped builder.
     *
     * @return The outer scoped builder.
     */
    public Builder pop();

    /**
     * Resets the builder back to an empty state.
     *
     * @return This builder for method call chaining.
     */
    public Builder reset();
}
