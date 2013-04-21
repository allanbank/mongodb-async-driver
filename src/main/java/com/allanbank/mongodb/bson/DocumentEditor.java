/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson;

import java.beans.PropertyEditorSupport;

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
public class DocumentEditor extends PropertyEditorSupport {

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
        catch (JsonException je) {
            throw new IllegalArgumentException("Could not parse '"
                    + documentString + "' into a document.", je);
        }
    }
}
