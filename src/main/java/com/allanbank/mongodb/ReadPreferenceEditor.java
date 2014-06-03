/*
 * #%L
 * ReadPreferenceEditor.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb;

import java.beans.PropertyEditorSupport;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.impl.RootDocument;
import com.allanbank.mongodb.bson.json.Json;
import com.allanbank.mongodb.error.JsonException;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * {@link java.beans.PropertyEditor} for the {@link ReadPreference} class.
 * <p>
 * The string value must be one of the following tokens or a parsable JSON
 * document in the form of {@link ReadPreference#asDocument()}.
 * </p>
 * </p> Valid tokens are:
 * <ul>
 * <li>{@code PRIMARY}</li>
 * <li>{@code SECONDARY}</li>
 * <li>{@code CLOSEST}</li>
 * <li>{@code NEAREST}</li>
 * <li>{@code PREFER_PRIMARY}</li>
 * <li>{@code PREFER_SECONDARY}</li>
 * </ul>
 * <p>
 * This editor will also parse a full MongoDB URI to extract the specified
 * {@link ReadPreference}. See the <a href=
 * "http://docs.mongodb.org/manual/reference/connection-string/#read-preference-options"
 * >Connection String URI Format</a> documentation for information on
 * constructing a MongoDB URI.
 * </p>
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2013-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReadPreferenceEditor extends PropertyEditorSupport {

    /** The set of fields used to determine a Durability from a MongoDB URI. */
    public static final Set<String> MONGODB_URI_FIELDS;

    /** The logger for the {@link ReadPreferenceEditor}. */
    protected static final Log LOG = LogFactory
            .getLog(ReadPreferenceEditor.class);

    /** Any empty array for tags. */
    private static final DocumentAssignable[] EMPTY_TAGS = new DocumentAssignable[0];

    static {
        final Set<String> fields = new HashSet<String>();
        fields.add("readpreferencetags");
        fields.add("readpreference");
        fields.add("slaveok");

        MONGODB_URI_FIELDS = Collections.unmodifiableSet(fields);
    }

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
        else if (MongoDbUri.isUri(readPreferenceString)) {
            final MongoDbUri uri = new MongoDbUri(readPreferenceString);
            final ReadPreference parsed = fromUriParameters(uri);
            if (parsed != null) {
                setValue(parsed);
            }
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
            catch (final JsonException parseError) {
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

    /**
     * Uses the URI parameters to determine a {@link ReadPreference}. May return
     * null if the URI did not contain any read preference settings.
     * 
     * @param uri
     *            The URI.
     * @return The {@link ReadPreference} from the URI parameters.
     */
    private ReadPreference fromUriParameters(final MongoDbUri uri) {

        final Map<String, String> parameters = uri.getParsedOptions();

        ReadPreference result = null;

        String value = parameters.remove("slaveok");
        if (value != null) {
            if (Boolean.parseBoolean(value)) {
                result = ReadPreference.SECONDARY;
            }
            else {
                result = ReadPreference.PRIMARY;
            }
        }

        value = parameters.remove("readpreference");
        final List<String> tagsValue = uri.getValuesFor("readpreferencetags");
        if (value != null) {
            if ("primary".equalsIgnoreCase(value)) {
                result = ReadPreference.PRIMARY;
            }
            else if ("primaryPreferred".equalsIgnoreCase(value)) {
                result = ReadPreference.preferPrimary(parseTags(tagsValue));
            }
            else if ("secondary".equalsIgnoreCase(value)) {
                result = ReadPreference.secondary(parseTags(tagsValue));
            }
            else if ("secondaryPreferred".equalsIgnoreCase(value)) {
                result = ReadPreference.preferSecondary(parseTags(tagsValue));
            }
            else if ("nearest".equalsIgnoreCase(value)) {
                result = ReadPreference.closest(parseTags(tagsValue));
            }
            else {
                LOG.warn("Unknown readPreference: '{}'. "
                        + "Defaulting to primary.", value);
                result = ReadPreference.PRIMARY;
            }
        }

        return result;
    }

    /**
     * Parses out the tags documents.
     * 
     * @param tagsValue
     *            The list of tags entries.
     * @return The tags documents.
     */
    private DocumentAssignable[] parseTags(final List<String> tagsValue) {
        if ((tagsValue == null) || tagsValue.isEmpty()) {
            return EMPTY_TAGS;
        }

        final List<DocumentAssignable> docs = new ArrayList<DocumentAssignable>(
                tagsValue.size());
        for (final String tagValue : tagsValue) {
            docs.add(Json.parse("{" + tagValue + "}"));
        }
        return docs.toArray(EMPTY_TAGS);
    }
}
