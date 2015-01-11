/*
 * #%L
 * DocumentEditor.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.bson;

import java.beans.PropertyEditorSupport;

import javax.annotation.concurrent.NotThreadSafe;

import com.allanbank.mongodb.bson.json.Json;
import com.allanbank.mongodb.error.JsonException;

/**
 * {@link java.beans.PropertyEditor} for the {@link Document} class.
 * <p>
 * The string value must a JSON document parsable via the
 * {@link Json#parse(String)} method.
 * </p>
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@NotThreadSafe
public class DocumentEditor
        extends PropertyEditorSupport {

    /**
     * Creates a new DocumentEditor.
     */
    public DocumentEditor() {
        super();
    }

    /**
     * Parse a string to a {@link Document}.
     */
    @Override
    public void setAsText(final String documentString) {
        try {
            setValue(Json.parse(documentString));
        }
        catch (final JsonException je) {
            throw new IllegalArgumentException("Could not parse '"
                    + documentString + "' into a document.", je);
        }
    }
}
