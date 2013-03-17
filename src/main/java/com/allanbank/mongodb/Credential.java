/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb;

import java.io.Serializable;
import java.util.Arrays;

import com.allanbank.mongodb.connection.auth.Authenticator;
import com.allanbank.mongodb.error.MongoDbAuthenticationException;

/**
 * Credential provides an immutable set of credentials for accessing MongoDB.
 * <p>
 * A given client can support a different set of credentials for each database
 * the client is accessing. The client can also authenticate against the
 * {@value #ADMIN_DB} database which will apply across all databases.
 * </p>
 * <p>
 * <em>Note:</em> While we use the term username/password within this class the
 * values may not actually be a user name or a password. In addition not all
 * authentication mechanisms may use all of the fields in this class. See the
 * documentation for the authenticator being used for details on what values are
 * expected for each off the fields in this class.
 * </p>
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class Credential implements Serializable {

    /**
     * The name of the administration database used to authenticate a
     * administrator to MongoDB.
     */
    public static final String ADMIN_DB = "admin";

    /**
     * Constant for Kerberos authentication.
     * <p>
     * <em>Note:</em> Use of the Kerberos for authentication requires the
     * driver's extensions. See the <a href=
     * "http://www.allanbank.com/mongodb-async-driver/userguide/kerberos.html"
     * >Kerberos Usage Guide</a> for details.
     * </p>
     * 
     * @see <a
     *      href="http://www.allanbank.com/mongodb-async-driver/userguide/kerberos.html">Kerberos
     *      Usage Guide</a>
     */
    public static final String KERBEROS;

    /** Constant for traditional MongoDB Chllenge/Response. */
    public static final String MONGODB_CR;

    /** An empty password array. */
    public static final char[] NO_PASSWORD = new char[0];

    /** Serialization version of the class. */
    private static final long serialVersionUID = -6251469373336569336L;

    static {
        KERBEROS = "com.allanbank.mongodb.extensions.authentication.KerberosAuthenticator";
        MONGODB_CR = "com.allanbank.mongodb.connection.auth.MongoDbAuthenticator";
    }

    /**
     * The authentication type or mode that the credentials should be used with.
     */
    private final String myAuthenticationType;

    /** The template authenticator for the credentials. */
    private transient Authenticator myAuthenticator;

    /** The database the credentials are valid for. */
    private final String myDatabase;

    /** The password for the credentials set. */
    private final char[] myPassword;

    /** The username for the credentials set. */
    private final String myUserName;

    /**
     * Creates a new Credential.
     * 
     * @param username
     *            The username for the credentials set.
     * @param password
     *            The password for the credentials set. A clone of this array is
     *            made so the original can be cleared via something like
     *            {@link java.util.Arrays#fill(char[], char)
     *            Arrays.fill(password, 0)}.
     * @param authenticationType
     *            The authentication type or mode that the credentials should be
     *            used with.
     */
    public Credential(final String username, final char[] password,
            final String authenticationType) {
        this(username, password, ADMIN_DB, authenticationType);
    }

    /**
     * Creates a new Credential.
     * 
     * @param username
     *            The username for the credentials set.
     * @param password
     *            The password for the credentials set. A clone of this array is
     *            made so the original can be cleared via something like
     *            {@link java.util.Arrays#fill(char[], char)
     *            Arrays.fill(password, 0)}.
     * @param database
     *            The database the credentials are valid for. Use
     *            {@link #ADMIN_DB} to authenticate as an administrator.
     * @param authenticator
     *            A custom authenticator to use with the credentials.
     */
    public Credential(final String username, final char[] password,
            final String database, final Authenticator authenticator) {
        myUserName = username;
        myDatabase = database;
        myAuthenticationType = authenticator.getClass().getName();
        myAuthenticator = authenticator;

        if (password == null) {
            myPassword = NO_PASSWORD;
        }
        else {
            myPassword = password.clone();
        }
    }

    /**
     * Creates a new Credential.
     * 
     * @param username
     *            The username for the credentials set.
     * @param password
     *            The password for the credentials set. A clone of this array is
     *            made so the original can be cleared via something like
     *            {@link java.util.Arrays#fill(char[], char)
     *            Arrays.fill(password, 0)}.
     * @param database
     *            The database the credentials are valid for. Use
     *            {@link #ADMIN_DB} to authenticate as an administrator.
     * @param authenticationType
     *            The authentication type or mode that the credentials should be
     *            used with.
     */
    public Credential(final String username, final char[] password,
            final String database, final String authenticationType) {
        myUserName = username;
        myDatabase = database;
        myAuthenticationType = authenticationType;
        myAuthenticator = null;

        if (password == null) {
            myPassword = NO_PASSWORD;
        }
        else {
            myPassword = password.clone();
        }
    }

    /**
     * Returns an authenticator for the credentials.
     * 
     * @return The authenticator for the credentials.
     * @throws MongoDbAuthenticationException
     *             On a failure to load the authenticator for the credentials.
     */
    public Authenticator authenticator() throws MongoDbAuthenticationException {
        if (myAuthenticator == null) {
            try {
                loadAuthenticator();
            }
            catch (final ClassNotFoundException e) {
                throw new MongoDbAuthenticationException(e);
            }
            catch (final InstantiationException e) {
                throw new MongoDbAuthenticationException(e);
            }
            catch (final IllegalAccessException e) {
                throw new MongoDbAuthenticationException(e);
            }
        }

        return myAuthenticator.clone();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return true if the passed value equals these credential.
     * </p>
     */
    @Override
    public boolean equals(final Object object) {
        boolean result = false;
        if (this == object) {
            result = true;
        }
        else if ((object != null) && (getClass() == object.getClass())) {
            final Credential other = (Credential) object;

            result = nullSafeEquals(myAuthenticationType,
                    other.myAuthenticationType)
                    && nullSafeEquals(myDatabase, other.myDatabase)
                    && nullSafeEquals(myUserName, other.myUserName)
                    && Arrays.equals(myPassword, other.myPassword);
        }
        return result;
    }

    /**
     * Returns the authentication type or mode that the credentials should be
     * used with.
     * 
     * @return The authentication type or mode that the credentials should be
     *         used with.
     */
    public String getAuthenticationType() {
        return myAuthenticationType;
    }

    /**
     * Returns the authenticator value.
     * 
     * @return The authenticator value.
     */
    public Authenticator getAuthenticator() {
        return myAuthenticator;
    }

    /**
     * Returns the database the credentials are valid for. Use {@link #ADMIN_DB}
     * to authenticate as an administrator.
     * 
     * @return The database the credentials are valid for.
     */
    public String getDatabase() {
        return myDatabase;
    }

    /**
     * Returns the password for the credentials set. A clone of the internal
     * array is returns that should be cleared when it is done being used via
     * something like {@link java.util.Arrays#fill(char[], char)
     * Arrays.fill(password, 0)}
     * 
     * @return The password for the credentials set.
     */
    public char[] getPassword() {
        return myPassword.clone();
    }

    /**
     * Returns the username for the credentials set.
     * 
     * @return The username for the credentials set.
     */
    public String getUserName() {
        return myUserName;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to hash the credentials.
     * </p>
     */
    @Override
    public int hashCode() {
        int result = 1;

        result = (31 * result)
                + ((myAuthenticationType == null) ? 0 : myAuthenticationType
                        .hashCode());
        result = (31 * result)
                + ((myDatabase == null) ? 0 : myDatabase.hashCode());
        result = (31 * result) + Arrays.hashCode(myPassword);
        result = (31 * result)
                + ((myUserName == null) ? 0 : myUserName.hashCode());

        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to returns the credentials in a human readable form.
     * </p>
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("{ username : '");
        builder.append(myUserName);
        builder.append("', database : '");
        builder.append(myDatabase);
        builder.append("', password : '<redacted>', type: '");
        if (KERBEROS.equals(myAuthenticationType)) {
            builder.append("KERBEROS");
        }
        else if (MONGODB_CR.equals(myAuthenticationType)) {
            builder.append("MONGODB-CR");
        }
        else if (myAuthenticationType != null) {
            builder.append(myAuthenticationType);
        }

        builder.append("' }");

        return builder.toString();
    }

    /**
     * Loads the authenticator for the credentials.
     * 
     * @throws ClassNotFoundException
     *             If the authenticators Class cannot be found.
     * @throws InstantiationException
     *             If the authenticator cannot be instantiated.
     * @throws IllegalAccessException
     *             If the authenticator cannot be accessed.
     */
    /* package */void loadAuthenticator() throws ClassNotFoundException,
            InstantiationException, IllegalAccessException {
        if (myAuthenticator == null) {
            myAuthenticator = (Authenticator) Class.forName(
                    getAuthenticationType()).newInstance();
        }
    }

    /**
     * Does a null safe equals comparison.
     * 
     * @param rhs
     *            The right-hand-side of the comparison.
     * @param lhs
     *            The left-hand-side of the comparison.
     * @return True if the rhs equals the lhs. Note: nullSafeEquals(null, null)
     *         returns true.
     */
    private boolean nullSafeEquals(final Object rhs, final Object lhs) {
        return (rhs == lhs) || ((rhs != null) && rhs.equals(lhs));
    }
}
