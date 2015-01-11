/*
 * #%L
 * CredentialEditor.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * CredentialEditor provides the ability to parse the credentials from a MongoDB
 * URI.
 * <p>
 * This editor will parses a full MongoDB URI to extract the specified
 * {@link Credential}. See the <a href=
 * "http://docs.mongodb.org/manual/reference/connection-string/#authentication-options"
 * >Connection String URI Format</a> documentation for information on
 * constructing a MongoDB URI.
 * </p>
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
@NotThreadSafe
public class CredentialEditor
        extends PropertyEditorSupport {

    /**
     * The fields parsed out of a MongoDB URI to construct a {@link Credential}.
     */
    public static final Set<String> MONGODB_URI_FIELDS;

    /** The logger for the {@link CredentialEditor}. */
    protected static final Log LOG = LogFactory.getLog(CredentialEditor.class);

    static {
        final Set<String> fields = new HashSet<String>();
        fields.add("gssapiservicename");
        fields.add("authsource");
        fields.add("authmechanism");

        MONGODB_URI_FIELDS = Collections.unmodifiableSet(fields);
    }

    /**
     * Creates a new CredentialEditor.
     */
    public CredentialEditor() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to parse a string to a {@link Credential}.
     * </p>
     *
     * @throws IllegalArgumentException
     *             If the string cannot be parsed into a {@link Credential}.
     */
    @Override
    public void setAsText(final String credentialString)
            throws IllegalArgumentException {

        if (MongoDbUri.isUri(credentialString)) {
            Credential.Builder builder = null;

            final MongoDbUri uri = new MongoDbUri(credentialString);
            if (uri.getUserName() != null) {
                builder = Credential.builder().userName(uri.getUserName())
                        .password(uri.getPassword().toCharArray());
                final String database = uri.getDatabase();
                if (!database.isEmpty()) {
                    builder.database(database);
                }
            }
            final Credential parsed = fromUriParameters(builder,
                    uri.getParsedOptions());
            if (parsed != null) {
                setValue(parsed);
            }
        }
        else {
            throw new IllegalArgumentException(
                    "Could not determine the credentials for '"
                            + credentialString + "'.");
        }
    }

    /**
     * Creates a Credential from the MongoDB URI parameters. Any fields used
     * from the parameters are removed from the {@code parameters} map.
     *
     * @param builder
     *            The credentials builder to update.
     * @param parameters
     *            The map of URI parameters.
     * @return The Credential.
     */
    private Credential fromUriParameters(final Credential.Builder builder,
            final Map<String, String> parameters) {

        final String gssapiServiceName = parameters.remove("gssapiservicename");
        if ((gssapiServiceName != null)) {
            if (builder != null) {
                builder.kerberos().addOption("kerberos.service.name",
                        gssapiServiceName);
            }
            else {
                LOG.info("Must supply a user name "
                        + "to set a gssapiServiceName: '{}'.",
                        gssapiServiceName);
            }
        }

        final String authSource = parameters.remove("authsource");
        if (authSource != null) {
            if (builder != null) {
                builder.setDatabase(authSource);
            }
            else {
                LOG.info("Must supply a user name "
                        + "to set a authSource: '{}'.", authSource);
            }
        }

        final String authMechanism = parameters.remove("authmechanism");
        if (authMechanism != null) {
            if (builder != null) {
                if ("MONGODB-CR".equalsIgnoreCase(authMechanism)) {
                    builder.mongodbCR();
                }
                else if ("MONGODB-X509".equalsIgnoreCase(authMechanism)) {
                    builder.x509();
                }
                else if ("GSSAPI".equalsIgnoreCase(authMechanism)) {
                    builder.kerberos();
                }
                else if ("PLAIN".equalsIgnoreCase(authMechanism)) {
                    builder.plainSasl();
                }
                else if ("SCRAM-SHA-1".equalsIgnoreCase(authMechanism)) {
                    builder.scramSha1();
                }
                else {
                    LOG.warn("Unknown authMechanism: '{}'. "
                            + "Not authenticating.", authMechanism);
                }
            }
            else {
                LOG.info("Must supply a user name and password "
                        + "to set a authMechanism: '{}'.", authMechanism);
            }
        }

        if (builder != null) {
            return builder.build();
        }
        return null;
    }

}
