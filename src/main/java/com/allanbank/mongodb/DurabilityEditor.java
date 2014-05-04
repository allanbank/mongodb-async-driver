/*
 * Copyright 2013-2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */
package com.allanbank.mongodb;

import java.beans.PropertyEditorSupport;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * {@link java.beans.PropertyEditor} for the {@link Durability} class.
 * <p>
 * The string value must be one of the following tokens or parse-able by
 * {@link Durability#valueOf(String)} method.
 * </p>
 * <p>
 * Valid tokens are:
 * <ul>
 * <li>{@code NONE}</li>
 * <li>{@code ACK}</li>
 * <li>{@code FSYNC}</li>
 * <li>{@code JOURNAL}</li>
 * <li>{@code MAJORITY}</li>
 * </ul>
 * </p>
 * <p>
 * {@code FSYNC}, {@code JOURNAL}, and {@code MAJORITY} all use
 * {@value #DEFAULT_WAIT_TIME_MS} milliseconds for the wait time.
 * </p>
 * <p>
 * This editor will also parses a full MongoDB URI to extract the specified
 * {@link Durability}. See the <a href=
 * "http://docs.mongodb.org/manual/reference/connection-string/#write-concern-options"
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
public class DurabilityEditor extends PropertyEditorSupport {

    /** The default wait time for tokenized durabilities: {@value} ms. */
    public static final int DEFAULT_WAIT_TIME_MS = 30000;

    /** The set of fields used to determine a Durability from a MongoDB URI. */
    public static final Set<String> MONGODB_URI_FIELDS;

    static {
        final Set<String> fields = new HashSet<String>();
        fields.add("safe");
        fields.add("w");
        fields.add("wtimeout");
        fields.add("wtimeoutms");
        fields.add("fsync");
        fields.add("journal");

        MONGODB_URI_FIELDS = Collections.unmodifiableSet(fields);
    }

    /**
     * Creates a new DurabilityEditor.
     */
    public DurabilityEditor() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to parse a string to a {@link Durability}.
     * </p>
     * 
     * @throws IllegalArgumentException
     *             If the string cannot be parsed into a {@link Durability}.
     */
    @Override
    public void setAsText(final String durabilityString)
            throws IllegalArgumentException {

        if ("NONE".equalsIgnoreCase(durabilityString)) {
            setValue(Durability.NONE);
        }
        else if ("ACK".equalsIgnoreCase(durabilityString)) {
            setValue(Durability.ACK);
        }
        else if ("FSYNC".equalsIgnoreCase(durabilityString)) {
            setValue(Durability.fsyncDurable(DEFAULT_WAIT_TIME_MS));
        }
        else if ("JOURNAL".equalsIgnoreCase(durabilityString)) {
            setValue(Durability.journalDurable(DEFAULT_WAIT_TIME_MS));
        }
        else if ("MAJORITY".equalsIgnoreCase(durabilityString)) {
            setValue(Durability.replicaDurable(Durability.MAJORITY_MODE,
                    DEFAULT_WAIT_TIME_MS));
        }
        else if (MongoDbUri.isUri(durabilityString)) {
            final MongoDbUri uri = new MongoDbUri(durabilityString);
            final Durability parsed = fromUriParameters(uri.getParsedOptions());
            if (parsed != null) {
                setValue(parsed);
            }
        }
        else {
            final Durability durability = Durability.valueOf(durabilityString);
            if (durability != null) {
                setValue(durability);
            }
            else {
                throw new IllegalArgumentException(
                        "Could not determine the durability for '"
                                + durabilityString + "'.");
            }
        }
    }

    /**
     * Uses the URI parameters to determine a durability. May return null if the
     * URI did not contain any durability settings.
     * 
     * @param parameters
     *            The URI parameters.
     * @return The {@link Durability} from the URI parameters.
     */
    private Durability fromUriParameters(final Map<String, String> parameters) {
        boolean safe = false;
        int w = -1;
        String wTxt = null;
        boolean fsync = false;
        boolean journal = false;
        int wtimeout = 0;

        // Without order surprising things happen.
        for (final Map.Entry<String, String> entry : parameters.entrySet()) {
            final String parameter = entry.getKey();
            final String value = entry.getValue();

            if ("safe".equalsIgnoreCase(parameter)) {
                safe = Boolean.parseBoolean(value);
            }
            else if ("w".equalsIgnoreCase(parameter)) {
                safe = true;
                try {
                    w = Integer.parseInt(value);
                    wTxt = null;
                }
                catch (final NumberFormatException nfe) {
                    w = 1;
                    wTxt = value;
                }
            }
            else if ("wtimeout".equalsIgnoreCase(parameter)
                    || "wtimeoutms".equalsIgnoreCase(parameter)) {
                safe = true;
                wtimeout = Integer.parseInt(value);
                if (w < 1) {
                    w = 1;
                }
            }
            else if ("fsync".equalsIgnoreCase(parameter)) {
                fsync = Boolean.parseBoolean(value);
                if (fsync) {
                    journal = false;
                    safe = true;
                }
            }
            else if ("journal".equalsIgnoreCase(parameter)) {
                journal = Boolean.parseBoolean(value);
                if (journal) {
                    fsync = false;
                    safe = true;
                }
            }
        }

        // Figure out the intended durability.
        Durability result = null;
        if (safe) {
            if (fsync) {
                result = Durability.fsyncDurable(wtimeout);
            }
            else if (journal) {
                result = Durability.journalDurable(wtimeout);
            }
            else if (w > 0) {
                if (wTxt != null) {
                    result = Durability.replicaDurable(wTxt, wtimeout);
                }
                else {
                    result = Durability.replicaDurable(w, wtimeout);
                }
            }
            else {
                result = Durability.ACK;
            }
        }
        return result;

    }
}
