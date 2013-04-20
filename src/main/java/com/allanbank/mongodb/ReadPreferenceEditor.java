/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb;

import java.beans.PropertyEditorSupport;
import java.util.List;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.impl.RootDocument;
import com.allanbank.mongodb.bson.json.Json;
import com.allanbank.mongodb.error.JsonParseException;

/**
 * {@link java.beans.PropertyEditor} for the {@link ReadPreference} class.
 * <p>
 * The string value must be one of the following tokens or a parsable JSON
 * document in the form of {@link ReadPreference#asDocument()}.
 * </p>
 * </p> Valid tokens are:
 * <ul>
 * <li>PRIMARY</li>
 * <li>SECONDARY</li>
 * <li>CLOSEST</li>
 * <li>NEAREST</li>
 * <li>PREFER_PRIMARY</li>
 * <li>PREFER_SECONDARY</li>
 * </ul>
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReadPreferenceEditor extends PropertyEditorSupport {

    /**
     * Creates a new ReadPreferenceEditor.
     */
    public ReadPreferenceEditor() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to parse a string to a {@link ReadPreference}.
     * </p>
     * 
     * @throws IllegalArgumentException
     *             If the string cannot be parsed into a {@link ReadPreference}.
     */
    @Override
    public void setAsText(final String readPreferenceString)
            throws IllegalArgumentException {
        if ("PRIMARY".equalsIgnoreCase(readPreferenceString)) {
            setValue(ReadPreference.primary());
        }
        else if ("SECONDARY".equalsIgnoreCase(readPreferenceString)) {
            setValue(ReadPreference.secondary());
        }
        else if ("CLOSEST".equalsIgnoreCase(readPreferenceString)) {
            setValue(ReadPreference.closest());
        }
        else if ("NEAREST".equalsIgnoreCase(readPreferenceString)) {
            setValue(ReadPreference.closest());
        }
        else if ("PREFER_PRIMARY".equalsIgnoreCase(readPreferenceString)) {
            setValue(ReadPreference.preferPrimary());
        }
        else if ("PREFER_SECONDARY".equalsIgnoreCase(readPreferenceString)) {
            setValue(ReadPreference.preferSecondary());
        }
        else {
            // JSON Document?
            ReadPreference prefs;
            try {
                prefs = null;

                final Document doc = Json.parse(readPreferenceString);
                final List<DocumentElement> tagDocs = doc.find(
                        DocumentElement.class, "tags", ".*");
                final Document[] tagArray = new Document[tagDocs.size()];
                for (int i = 0; i < tagArray.length; ++i) {
                    tagArray[i] = new RootDocument(tagDocs.get(i).getElements());
                }

                final Element modeElement = doc.get("mode");
                if (modeElement != null) {
                    final String token = modeElement.getValueAsString();
                    final ReadPreference.Mode mode = ReadPreference.Mode
                            .valueOf(token);

                    switch (mode) {
                    case NEAREST: {
                        prefs = ReadPreference.closest(tagArray);
                        break;
                    }
                    case PRIMARY_ONLY: {
                        prefs = ReadPreference.primary();
                        break;
                    }
                    case PRIMARY_PREFERRED: {
                        prefs = ReadPreference.preferPrimary(tagArray);
                        break;
                    }
                    case SECONDARY_ONLY: {
                        prefs = ReadPreference.secondary(tagArray);
                        break;
                    }
                    case SECONDARY_PREFERRED: {
                        prefs = ReadPreference.preferSecondary(tagArray);
                        break;
                    }
                    case SERVER: {
                        final Element serverElement = doc.get("server");
                        if (serverElement != null) {
                            prefs = ReadPreference.server(serverElement
                                    .getValueAsString());
                        }
                        break;
                    }
                    }
                }
            }
            catch (final JsonParseException parseError) {
                // Fall through.
                prefs = null;
            }
            catch (final IllegalArgumentException invalidMode) {
                // Fall through.
                prefs = null;
            }

            if (prefs != null) {
                setValue(prefs);
            }
            else {
                throw new IllegalArgumentException(
                        "Could not determine the read preferences for '"
                                + readPreferenceString + "'.");
            }
        }
    }
}
