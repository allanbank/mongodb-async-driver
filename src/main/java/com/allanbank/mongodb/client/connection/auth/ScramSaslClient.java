/*
 * #%L
 * ScramSaslClient.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.connection.auth;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.allanbank.mongodb.util.IOUtils;

/**
 * ScramSaslClient provides a {@link SaslClient} implementation that implements
 * the SCRAM-SHA-1 as specified by RFC 5802.
 * <p>
 * This implementation contains a complete implementation of the
 * {@code saslPrep} algorithm for the username and password as specified by RFC
 * 4013 which is itself a profile for {@code stringprep} from RFC 3454.
 * </p>
 * 
 * @see <a href="http://tools.ietf.org/html/rfc5802">RFC 5802</a>
 * @see <a href="http://tools.ietf.org/html/rfc4013">RFC 4013</a>
 * @see <a href="http://tools.ietf.org/html/rfc3454">RFC 3454</a>
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the
 *         extensions.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class ScramSaslClient implements SaslClient {

    /** The official name of the SASL Mechanism. */
    public static final String MECHANISM = "SCRAM-SHA-1";

    /** The ASCII encoding. */
    protected static final Charset ASCII = Charset.forName("US-ASCII");

    /**
     * The size of the random value in bytes pre-Base64 encoding:
     * {@value #RANDOM_BYTES}. Chosen to be larger than the {@link #HMAC_NAME
     * Hmac} output (20 bytes) but have no Base64 padding.
     */
    protected static final int RANDOM_BYTES = 24;

    /**
     * The {@code saslPrep} specified disallowed characters. Note that some
     * large ranges are missing from this set and are instead check via code in
     * the {@link #saslPrepCheckProhibited(String)} method directly.
     */
    protected static final Set<Integer> SASL_PREP_DISALLOWED;

    /** The {@code saslPrep} specified replacement mappings. */
    protected static final Map<Character, String> SASL_PREP_MAPPINGS;

    /** The UTF-8 encoding. To be used after {@link #saslPrep(char[])} */
    protected static final Charset UTF_8 = Charset.forName("UTF-8");

    /** The name of the {@link MessageDigest} used. */
    private static final String DIGEST_NAME = "SHA-1";

    /** An empty array of bytes to initialize password arrays. */
    private static final byte[] EMPTY_BYTES = new byte[0];

    /** An empty array of character to initialize password arrays. */
    private static final char[] EMPTY_CHARS = new char[0];

    /**
     * The header for the client's initial message. Also know as the GS2 header.
     */
    private static final String GS2_HEADER = "n,,";

    /** The name of the {@link Mac} used: {@value #HMAC_NAME} */
    private static final String HMAC_NAME = "HmacSHA1";

    /** The initial value to seed the hash iterations. */
    private static final byte[] ONE_UINT32_BE = new byte[] { 0, 0, 0, 1 };

    static {
        final Map<Character, String> saslPrepMappings = new HashMap<Character, String>();

        // RFC 3454 - B.1 - Commonly Mapped to Nothing.
        saslPrepMappings.put(Character.valueOf('\u00AD'), "");
        saslPrepMappings.put(Character.valueOf('\u034F'), "");
        saslPrepMappings.put(Character.valueOf('\u1806'), "");
        saslPrepMappings.put(Character.valueOf('\u180B'), "");
        saslPrepMappings.put(Character.valueOf('\u180C'), "");
        saslPrepMappings.put(Character.valueOf('\u180D'), "");
        saslPrepMappings.put(Character.valueOf('\u200B'), "");
        saslPrepMappings.put(Character.valueOf('\u200C'), "");
        saslPrepMappings.put(Character.valueOf('\u200D'), "");
        saslPrepMappings.put(Character.valueOf('\u2060'), "");
        saslPrepMappings.put(Character.valueOf('\uFE00'), "");
        saslPrepMappings.put(Character.valueOf('\uFE01'), "");
        saslPrepMappings.put(Character.valueOf('\uFE02'), "");
        saslPrepMappings.put(Character.valueOf('\uFE03'), "");
        saslPrepMappings.put(Character.valueOf('\uFE04'), "");
        saslPrepMappings.put(Character.valueOf('\uFE05'), "");
        saslPrepMappings.put(Character.valueOf('\uFE06'), "");
        saslPrepMappings.put(Character.valueOf('\uFE07'), "");
        saslPrepMappings.put(Character.valueOf('\uFE08'), "");
        saslPrepMappings.put(Character.valueOf('\uFE09'), "");
        saslPrepMappings.put(Character.valueOf('\uFE0A'), "");
        saslPrepMappings.put(Character.valueOf('\uFE0B'), "");
        saslPrepMappings.put(Character.valueOf('\uFE0C'), "");
        saslPrepMappings.put(Character.valueOf('\uFE0D'), "");
        saslPrepMappings.put(Character.valueOf('\uFE0E'), "");
        saslPrepMappings.put(Character.valueOf('\uFE0F'), "");
        saslPrepMappings.put(Character.valueOf('\uFEFF'), "");

        // RFC 3454 - C.1.2 Non-ASCII space characters
        saslPrepMappings.put(Character.valueOf('\u00A0'), " ");
        saslPrepMappings.put(Character.valueOf('\u1680'), " ");
        saslPrepMappings.put(Character.valueOf('\u2000'), " ");
        saslPrepMappings.put(Character.valueOf('\u2001'), " ");
        saslPrepMappings.put(Character.valueOf('\u2002'), " ");
        saslPrepMappings.put(Character.valueOf('\u2003'), " ");
        saslPrepMappings.put(Character.valueOf('\u2004'), " ");
        saslPrepMappings.put(Character.valueOf('\u2005'), " ");
        saslPrepMappings.put(Character.valueOf('\u2006'), " ");
        saslPrepMappings.put(Character.valueOf('\u2007'), " ");
        saslPrepMappings.put(Character.valueOf('\u2008'), " ");
        saslPrepMappings.put(Character.valueOf('\u2009'), " ");
        saslPrepMappings.put(Character.valueOf('\u200A'), " ");
        saslPrepMappings.put(Character.valueOf('\u200B'), " ");
        saslPrepMappings.put(Character.valueOf('\u202F'), " ");
        saslPrepMappings.put(Character.valueOf('\u205F'), " ");
        saslPrepMappings.put(Character.valueOf('\u3000'), " ");

        SASL_PREP_MAPPINGS = Collections.unmodifiableMap(saslPrepMappings);

        final Set<Integer> disallowed = new HashSet<Integer>();

        // RFC 3454 - C.1.2 Non-ASCII space characters
        disallowed.add(Integer.valueOf(0x00A0));
        disallowed.add(Integer.valueOf(0x1680));
        disallowed.add(Integer.valueOf(0x2000));
        disallowed.add(Integer.valueOf(0x2001));
        disallowed.add(Integer.valueOf(0x2002));
        disallowed.add(Integer.valueOf(0x2003));
        disallowed.add(Integer.valueOf(0x2004));
        disallowed.add(Integer.valueOf(0x2005));
        disallowed.add(Integer.valueOf(0x2006));
        disallowed.add(Integer.valueOf(0x2007));
        disallowed.add(Integer.valueOf(0x2008));
        disallowed.add(Integer.valueOf(0x2009));
        disallowed.add(Integer.valueOf(0x200A));
        disallowed.add(Integer.valueOf(0x200B));
        disallowed.add(Integer.valueOf(0x202F));
        disallowed.add(Integer.valueOf(0x205F));
        disallowed.add(Integer.valueOf(0x3000));

        // RFC 3454 - C.2.1 ASCII control characters
        addRange(disallowed, 0x0000, 0x001F);
        disallowed.add(Integer.valueOf(0x007F));

        // RFC 3454 - C.2.2 Non-ASCII control characters
        addRange(disallowed, 0x0080, 0x009F);
        disallowed.add(Integer.valueOf(0x06DD));
        disallowed.add(Integer.valueOf(0x070F));
        disallowed.add(Integer.valueOf(0x180E));
        disallowed.add(Integer.valueOf(0x200C));
        disallowed.add(Integer.valueOf(0x200D));
        disallowed.add(Integer.valueOf(0x2028));
        disallowed.add(Integer.valueOf(0x2029));
        disallowed.add(Integer.valueOf(0x2060));
        disallowed.add(Integer.valueOf(0x2061));
        disallowed.add(Integer.valueOf(0x2062));
        disallowed.add(Integer.valueOf(0x2063));
        addRange(disallowed, 0x206A, 0x206F);
        disallowed.add(Integer.valueOf(0xFEFF));
        addRange(disallowed, 0xFFF9, 0xFFFC);
        addRange(disallowed, 0x1D173, 0x1D17A);

        // RFC 3454 - C.3 Private use
        // Done in code since very large ranges.
        // addRange(disallowed, 0xE000, 0xF8FF);
        // addRange(disallowed, 0xF0000, 0xFFFFD);
        // addRange(disallowed, 0x100000, 0x10FFFD);

        // RFC 3454 - C.4 Non-character code points
        addRange(disallowed, 0xFDD0, 0xFDEF);
        addRange(disallowed, 0xFFFE, 0xFFFF);
        addRange(disallowed, 0x1FFFE, 0x1FFFF);
        addRange(disallowed, 0x2FFFE, 0x2FFFF);
        addRange(disallowed, 0x3FFFE, 0x3FFFF);
        addRange(disallowed, 0x4FFFE, 0x4FFFF);
        addRange(disallowed, 0x5FFFE, 0x5FFFF);
        addRange(disallowed, 0x6FFFE, 0x6FFFF);
        addRange(disallowed, 0x7FFFE, 0x7FFFF);
        addRange(disallowed, 0x8FFFE, 0x8FFFF);
        addRange(disallowed, 0x9FFFE, 0x9FFFF);
        addRange(disallowed, 0xAFFFE, 0xAFFFF);
        addRange(disallowed, 0xBFFFE, 0xBFFFF);
        addRange(disallowed, 0xCFFFE, 0xCFFFF);
        addRange(disallowed, 0xDFFFE, 0xDFFFF);
        addRange(disallowed, 0xEFFFE, 0xEFFFF);
        addRange(disallowed, 0xFFFFE, 0xFFFFF);
        addRange(disallowed, 0x10FFFE, 0x10FFFF);

        // RFC 3454 - C.5 Surrogate codes
        // Done in code since very large ranges.
        // addRange(disallowed, 0xD800, 0xDFFF);

        // RFC 3454 - C.6 Inappropriate for plain text
        disallowed.add(Integer.valueOf(0xFFF9));
        disallowed.add(Integer.valueOf(0xFFFA));
        disallowed.add(Integer.valueOf(0xFFFB));
        disallowed.add(Integer.valueOf(0xFFFC));
        disallowed.add(Integer.valueOf(0xFFFD));

        // RFC 3454 - C.7 Inappropriate for canonical representation
        addRange(disallowed, 0x2FF0, 0x2FFB);

        // RFC 3454 - C.8 Change display properties or are deprecated
        disallowed.add(Integer.valueOf(0x0340));
        disallowed.add(Integer.valueOf(0x0341));
        disallowed.add(Integer.valueOf(0x200E));
        disallowed.add(Integer.valueOf(0x200F));
        disallowed.add(Integer.valueOf(0x202A));
        disallowed.add(Integer.valueOf(0x202B));
        disallowed.add(Integer.valueOf(0x202C));
        disallowed.add(Integer.valueOf(0x202D));
        disallowed.add(Integer.valueOf(0x202E));
        disallowed.add(Integer.valueOf(0x206A));
        disallowed.add(Integer.valueOf(0x206B));
        disallowed.add(Integer.valueOf(0x206C));
        disallowed.add(Integer.valueOf(0x206D));
        disallowed.add(Integer.valueOf(0x206E));
        disallowed.add(Integer.valueOf(0x206F));

        // RFC 3454 - C.9 Tagging characters
        disallowed.add(Integer.valueOf(0xE0001));
        addRange(disallowed, 0xE0020, 0xE007F);

        SASL_PREP_DISALLOWED = Collections.unmodifiableSet(disallowed);
    }

    /**
     * Adds a range to the set of code points.
     * 
     * @param codepoints
     *            The set of code points to augment.
     * @param start
     *            The start of the range, inclusive.
     * @param end
     *            The end of the range, inclusive.
     */
    static private void addRange(final Set<Integer> codepoints,
            final int start, final int end) {
        // Unicode ranges are inclusive so <= in the for loop.
        for (int codepoint = start; codepoint <= end; ++codepoint) {
            codepoints.add(Integer.valueOf(codepoint));
        }
    }

    /** The handler to retrieve the user's name and password. */
    private final CallbackHandler myCallbackHandler;

    /** The first client message without the {@link #GS2_HEADER}. */
    private String myClientFirstMessageBare;

    /** The random value used as the client nonce. */
    private String myClientNonce;

    /** The number of iterations that server wants the password hashed. */
    private int myIterationCount;

    /** The salt for the password from the server. */
    private byte[] mySalt;

    /** The random value provided by the server. */
    private String myServerNonce;

    /** The signature from the server. */
    private String myServerSignature;

    /** The state of the SASL/SCRAM exchange. */
    private State myState;

    /** The collected user's name. */
    private String myUsername;

    /**
     * Creates a new ScramSaslClient.
     * 
     * @param callbackHandler
     *            The handler to retrieve the user's name and password.
     */
    public ScramSaslClient(final CallbackHandler callbackHandler) {
        myCallbackHandler = callbackHandler;

        dispose();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to clear any per session state.
     * </p>
     */
    @Override
    public void dispose() {
        myClientFirstMessageBare = null;
        myClientNonce = null;
        myIterationCount = 0;
        mySalt = null;
        myServerNonce = null;
        myServerSignature = null;
        myUsername = null;

        // Last to make sure not state persists.
        myState = State.INITIAL;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to evaluate the challenge based on the current state of the
     * client.
     * </p>
     * 
     * @see javax.security.sasl.SaslClient#evaluateChallenge(byte[])
     */
    @Override
    public byte[] evaluateChallenge(final byte[] challenge)
            throws SaslException {
        byte[] response = EMPTY_BYTES;
        switch (myState) {
        case INITIAL:
            response = createInitialMessage();
            myState = State.FIRST_SENT;
            break;
        case FIRST_SENT:
            response = createProof(challenge);
            myState = State.PROOF_SENT;
            break;
        case PROOF_SENT:
            evaluateFinalResult(challenge);
            myState = State.COMPLETE;
            break;
        default:
            throw new SaslException("No challenge expected in state " + myState);
        }
        return response;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return {@value #MECHANISM}.
     * </p>
     * 
     * @return Returns the constant {@value #MECHANISM}.
     */
    @Override
    public String getMechanismName() {
        return MECHANISM;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return <code>null</code> as there are no negotiated
     * properties.
     * </p>
     */
    @Override
    public Object getNegotiatedProperty(final String propName) {
        return null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return {@code true}.
     * </p>
     * 
     * @return Returns {@code true}.
     */
    @Override
    public boolean hasInitialResponse() {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return true once all of the messages have been sent and
     * received (i.e., the {@link #myState} is {@link State#COMPLETE}).
     * </p>
     */
    @Override
    public boolean isComplete() {
        return myState == State.COMPLETE;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to throw an IllegalStateException as the {@value #MECHANISM}
     * does not "support integrity and/or privacy as the quality of protection".
     * </p>
     * 
     * @throws IllegalStateException
     *             As the {@value #MECHANISM} does not
     *             "support integrity and/or privacy as the quality of protection."
     */
    @Override
    public byte[] unwrap(final byte[] incoming, final int offset, final int len) {
        throw new IllegalStateException(MECHANISM
                + " does not support integrity and/or "
                + "privacy as the quality of protection.");
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to throw an IllegalStateException as the {@value #MECHANISM}
     * does not "support integrity and/or privacy as the quality of protection".
     * </p>
     * 
     * @throws IllegalStateException
     *             As the {@value #MECHANISM} does not
     *             "support integrity and/or privacy as the quality of protection."
     */
    @Override
    public byte[] wrap(final byte[] outgoing, final int offset, final int len) {
        throw new IllegalStateException(MECHANISM
                + " does not support integrity and/or "
                + "privacy as the quality of protection.");
    }

    /**
     * Creates the initial message from the client to the server.
     * <p>
     * See RFC 5802 Section 5 for the details on the message and its contents.
     * </p>
     * 
     * @return The message to send to the server.
     * @throws SaslException
     *             On a failure to create the message.
     */
    protected final byte[] createInitialMessage() throws SaslException {
        try {
            final NameCallback nameCallback = new NameCallback("Username?");
            myCallbackHandler.handle(new Callback[] { nameCallback });
            myUsername = nameCallback.getName();
            myClientNonce = createNonce();

            final StringBuilder buffer = new StringBuilder(GS2_HEADER);
            buffer.append("n=");
            buffer.append(saslName(myUsername));
            buffer.append(",r=");
            buffer.append(myClientNonce);

            myClientFirstMessageBare = buffer.substring(GS2_HEADER.length());

            return buffer.toString().getBytes(UTF_8);
        }
        catch (final UnsupportedCallbackException e) {
            throw new SaslException(e.getMessage(), e);
        }
        catch (final IOException e) {
            throw new SaslException(e.getMessage(), e);
        }
    }

    /**
     * Creates a random nonce of the same length as the hash algorithm.
     * 
     * @return The random nonce.
     */
    protected String createNonce() {
        final SecureRandom random = new SecureRandom();

        final byte[] nonce = new byte[RANDOM_BYTES];
        random.nextBytes(nonce);

        return IOUtils.toBase64(nonce);
    }

    /**
     * Creates the second message from the client to the server that contains
     * the proof the client has the user's password.
     * <p>
     * See RFC 5802 Section 5 for the details on the message and its contents.
     * </p>
     * 
     * @param challenge
     *            The challenge message from the server.
     * @return The message to send to the server.
     * @throws SaslException
     *             On a failure to create the message.
     */
    protected final byte[] createProof(final byte[] challenge)
            throws SaslException {

        final String message = new String(challenge, UTF_8);
        try {
            final Map<String, String> content = parse(message);

            final String serverNonce = content.get("r");
            final String base64Salt = content.get("s");
            final String iterCountString = content.get("i");
            int iterationCount = 0;

            if (content.containsKey("m")) {
                throw new SaslException(
                        "The server required mandatory extension is not supported: m="
                                + content.get("m"));
            }
            else if (serverNonce == null) {
                throw new SaslException("Could not find the server's nonce: '"
                        + message + "'.");
            }
            else if (!serverNonce.startsWith(myClientNonce)) {
                throw new SaslException("The server's nonce '" + serverNonce
                        + "' must start with the client's nonce '"
                        + myClientNonce + "'.");
            }
            else if (base64Salt == null) {
                throw new SaslException("Could not find the server's salt: '"
                        + message + "'.");
            }
            else if ((base64Salt.length() % 4) != 0) {
                throw new SaslException(
                        "The server's salt is not a valid Base64 value: '"
                                + base64Salt + "'.");
            }
            else if (iterCountString == null) {
                throw new SaslException("Could not find the iteration count: '"
                        + message + "'.");
            }
            else {
                iterationCount = Integer.parseInt(iterCountString);
                if (iterationCount <= 0) {
                    throw new SaslException("Iteration count " + iterationCount
                            + " must be a positive integer.");
                }
            }

            myServerNonce = serverNonce;
            mySalt = IOUtils.base64ToBytes(base64Salt);
            myIterationCount = iterationCount;

            final PasswordCallback passwordCallback = new PasswordCallback(
                    "Password", false);
            char[] password = EMPTY_CHARS;
            char[] passwordPrep = EMPTY_CHARS;
            ByteBuffer passwordBuffer = null;
            byte[] passwordBytes = EMPTY_BYTES;
            byte[] saltedPassword = EMPTY_BYTES;
            byte[] clientKey = EMPTY_BYTES;
            byte[] storedKey = EMPTY_BYTES;
            byte[] clientSignature = EMPTY_BYTES;
            byte[] clientProof = EMPTY_BYTES;
            byte[] serverKey = EMPTY_BYTES;
            try {
                myCallbackHandler.handle(new Callback[] { passwordCallback });
                password = passwordCallback.getPassword();
                passwordPrep = saslPrep(password);
                passwordBuffer = UTF_8.encode(CharBuffer.wrap(passwordPrep));
                passwordBytes = new byte[passwordBuffer.remaining()];
                passwordBuffer.get(passwordBytes);

                saltedPassword = generateSaltedPassword(passwordBytes);
                clientKey = computeHmac(saltedPassword, "Client Key");
                storedKey = MessageDigest.getInstance(DIGEST_NAME).digest(
                        clientKey);

                final String clientFinalMessageWithoutProof = "c="
                        + IOUtils.toBase64(GS2_HEADER.getBytes(ASCII)) + ",r="
                        + myServerNonce;
                final String authMessage = myClientFirstMessageBare + ","
                        + message + "," + clientFinalMessageWithoutProof;

                clientSignature = computeHmac(storedKey, authMessage);

                clientProof = clientKey.clone();
                for (int i = 0; i < clientProof.length; i++) {
                    clientProof[i] ^= clientSignature[i];
                }

                // Compute the server's signature for use in the final step.
                serverKey = computeHmac(saltedPassword, "Server Key");
                myServerSignature = IOUtils.toBase64(computeHmac(serverKey,
                        authMessage));

                final String finalMessageWithProof = clientFinalMessageWithoutProof
                        + ",p=" + IOUtils.toBase64(clientProof);
                return finalMessageWithProof.getBytes(ASCII);
            }
            finally {
                // Clear everywhere we copied the password in a form we don't
                // send to the server.
                passwordCallback.clearPassword();
                Arrays.fill(password, '\u0000');
                Arrays.fill(passwordPrep, '\u0000');
                if ((passwordBuffer != null) && passwordBuffer.isReadOnly()) {
                    passwordBuffer.rewind();
                    passwordBuffer.limit(passwordBuffer.capacity());
                    while (passwordBuffer.hasRemaining()) {
                        passwordBuffer.put((byte) 0);
                    }
                }
                Arrays.fill(passwordBytes, (byte) 0);
                Arrays.fill(saltedPassword, (byte) 0);
                Arrays.fill(clientKey, (byte) 0);
                Arrays.fill(storedKey, (byte) 0);
                Arrays.fill(clientSignature, (byte) 0);
                Arrays.fill(clientProof, (byte) 0);
                Arrays.fill(serverKey, (byte) 0);
            }
        }
        catch (final IllegalArgumentException e) {
            throw new SaslException(e.getMessage(), e);
        }
        catch (final UnsupportedCallbackException e) {
            throw new SaslException(e.getMessage(), e);
        }
        catch (final IOException e) {
            throw new SaslException(e.getMessage(), e);
        }
        catch (final NoSuchAlgorithmException e) {
            throw new SaslException(e.getMessage(), e);
        }
    }

    /**
     * Verifies that the final message from the server matches the server
     * signature we computed.
     * 
     * @param challenge
     *            The final message from the server.
     * @throws SaslException
     *             If the signature is missing from the message or it does not
     *             match the signature we computed.
     */
    protected final void evaluateFinalResult(final byte[] challenge)
            throws SaslException {
        final String message = new String(challenge, ASCII);
        final Map<String, String> content = parse(message);

        final String verifier = content.get("v");
        if (verifier == null) {
            throw new SaslException(
                    "The Server's final message did not contain a verifier: '"
                            + message + "'");
        }

        if (!verifier.equals(myServerSignature)) {
            throw new SaslException("The server's signature ('" + verifier
                    + "') does not match the expected signature: '"
                    + myServerSignature + "'.");
        }
    }

    /**
     * Performs a {@link #saslPrep(char[])} on the {@code name} and then
     * converts all '=' characters to "=3D" and all ',' characters to "=2C".
     * 
     * @param name
     *            The name to prepare.
     * @return The name after {@link #saslPrep(char[])} and replacement.
     * @throws SaslException
     *             On a failure to prepare the name.
     */
    protected String saslName(final String name) throws SaslException {
        final char[] prepared = saslPrep(name.toCharArray());

        final StringBuilder builder = new StringBuilder(prepared.length);
        for (final char c : prepared) {
            if (c == '=') {
                builder.append("=3D");
            }
            else if (c == ',') {
                builder.append("=2C");
            }
            else {
                builder.append(c);
            }
        }
        return builder.toString();
    }

    /**
     * Implements the SASLPrep algorithm on the {@code text} as defined by RFC
     * 4013.
     * 
     * @param text
     *            The text to be prepared.
     * @return The prepared text.
     * @throws SaslException
     *             If the algorithm discovers a violation of the SASLPrep
     *             invariants as defined in the RFC.
     */
    protected char[] saslPrep(final char[] text) throws SaslException {

        // If this text is ASCII then no prep is needed.
        if (ASCII.newEncoder().canEncode(CharBuffer.wrap(text))) {

            // 2.3 Prohibited Output
            saslPrepCheckProhibited(new String(text));

            return text;
        }

        // 2.1 Mapping
        final CharSequence mapped = saslPrepMap(text);

        // 2.2 Normalization
        final String nfkc = Normalizer.normalize(mapped, Normalizer.Form.NFKC);

        // 2.3 Prohibited Output
        saslPrepCheckProhibited(nfkc);

        // 2.4. Bidirectional Characters
        saslPrepCheckBidi(nfkc);

        // 2.5. Unassigned Code Points
        saslPrepCheckUnassigned(nfkc);

        return nfkc.toCharArray();
    }

    /**
     * Generates the SCRAM keyed hash. See section 3 of RFC 5802.
     * 
     * @param keyBytes
     *            The {@link Mac}'s key bytes.
     * @param string
     *            The text to hash.
     * @return The keyed hash for the {@code string}.
     * @throws SaslException
     *             On a failure to initialize the {@link Mac}.
     */
    private byte[] computeHmac(final byte[] keyBytes, final String string)
            throws SaslException {
        final Mac mac = initMac(keyBytes);

        return mac.doFinal(string.getBytes(ASCII));
    }

    /**
     * Generates the SCRAM salted password. See section 3 of RFC 5802.
     * 
     * @param passwordBytes
     *            The password bytes.
     * @return The salted password.
     * @throws SaslException
     *             On a failure to initialize the {@link Mac}.
     */
    private byte[] generateSaltedPassword(final byte[] passwordBytes)
            throws SaslException {
        final Mac mac = initMac(passwordBytes);

        mac.update(mySalt);
        mac.update(ONE_UINT32_BE);
        final byte[] result = mac.doFinal();

        byte[] previous = result;
        for (int i = 1; i < myIterationCount; i++) {
            previous = mac.doFinal(previous);
            for (int x = 0; x < result.length; x++) {
                result[x] ^= previous[x];
            }
        }

        return result;
    }

    /**
     * Creates and initializes the {@link Mac}.
     * 
     * @param keyBytes
     *            The key for the {@link Mac}.
     * @return The initialized {@link Mac}.
     * @throws SaslException
     *             On a failure to initialize the {@link Mac}.
     */
    private Mac initMac(final byte[] keyBytes) throws SaslException {
        try {
            final SecretKeySpec key = new SecretKeySpec(keyBytes, HMAC_NAME);
            final Mac mac = Mac.getInstance(HMAC_NAME);

            mac.init(key);

            return mac;
        }
        catch (final NoSuchAlgorithmException e) {
            throw new SaslException(e.getMessage(), e);
        }
        catch (final InvalidKeyException e) {
            throw new SaslException(e.getMessage(), e);
        }
    }

    /**
     * Parses the message into the fields and returns the map of field names
     * (one character each) and values.
     * 
     * @param message
     *            The message to parse.
     * @return The parsed fields from the message.
     * @throws SaslException
     *             In an invalid field is encountered.
     */
    private Map<String, String> parse(final String message)
            throws SaslException {
        final Map<String, String> results = new HashMap<String, String>();

        final StringTokenizer tokens = new StringTokenizer(message, ",");
        while (tokens.hasMoreTokens()) {
            final String token = tokens.nextToken();

            if ((token.length() > 1) && (token.charAt(1) == '=')) {
                results.put(token.substring(0, 1), token.substring(2));
            }
            else {
                throw new SaslException("Invalid field ('" + token
                        + "') in the message: '" + message + "'.");
            }
        }

        return results;
    }

    /**
     * Per-RFC 4013 section 2.4, throws a {@link SaslException} if the Bidi
     * check fails.
     * <p>
     * From RFC 3454 the Bidi check must ensure the following 3
     * invariants<blockquote>
     * <ol>
     * <li>The characters in section 5.8 MUST be prohibited.</li>
     * 
     * <li>If a string contains any RandALCat character, the string MUST NOT
     * contain any LCat character.</li>
     * 
     * <li>If a string contains any RandALCat character, a RandALCat character
     * MUST be the first character of the string, and a RandALCat character MUST
     * be the last character of the string.</li>
     * </ol>
     * </blockquote>
     * </p>
     * 
     * @param text
     *            The text to check.
     * @throws SaslException
     *             If the text violates the Bidi invariants from RFC 3454.
     */
    private void saslPrepCheckBidi(final String text) throws SaslException {
        final int length = text.length();
        int codePoint;
        if (!text.isEmpty()) {
            final int first = Character.codePointAt(text, 0);
            final int firstDir = Character.getDirectionality(first);
            if ((firstDir == Character.DIRECTIONALITY_RIGHT_TO_LEFT)
                    || (firstDir == Character.DIRECTIONALITY_RIGHT_TO_LEFT_ARABIC)) {

                // Can be no L (DIRECTIONALITY_LEFT_TO_RIGHT)
                int lastCodePoint = 0;
                int lastDir = 0;
                for (int i = 0; i < length; i += Character
                        .charCount(lastCodePoint)) {
                    lastCodePoint = Character.codePointAt(text, i);
                    lastDir = Character.getDirectionality(lastCodePoint);

                    if (lastDir == Character.DIRECTIONALITY_LEFT_TO_RIGHT) {
                        throw new SaslException(
                                "SaslPrep does not allow mixing "
                                        + "left-to-right ("
                                        + String.valueOf(Character
                                                .toChars(lastCodePoint))
                                        + ") and right-to-left ("
                                        + String.valueOf(Character
                                                .toChars(first)) + ") text. "
                                        + "See RFC 4013 section 2.4.");
                    }
                }

                // ... and last must be DIRECTIONALITY_RIGHT_TO_LEFT or
                // DIRECTIONALITY_RIGHT_TO_LEFT_ARABIC
                if ((lastDir != Character.DIRECTIONALITY_RIGHT_TO_LEFT)
                        && (lastDir != Character.DIRECTIONALITY_RIGHT_TO_LEFT_ARABIC)) {
                    throw new SaslException(
                            "SaslPrep does not allow text with a leading "
                                    + "left-to-right character ("
                                    + String.valueOf(Character.toChars(first))
                                    + ") to not end with a non-left-to-right "
                                    + "character ("
                                    + String.valueOf(Character
                                            .toChars(lastCodePoint)) + "). "
                                    + "See RFC 4013 section 2.4.");
                }
            }
            else {
                // Don't allow a RandALCat.
                for (int i = 0; i < length; i += Character.charCount(codePoint)) {
                    codePoint = Character.codePointAt(text, i);
                    final int dir = Character.getDirectionality(codePoint);

                    if ((dir == Character.DIRECTIONALITY_RIGHT_TO_LEFT)
                            || (dir == Character.DIRECTIONALITY_RIGHT_TO_LEFT_ARABIC)) {
                        throw new SaslException(
                                "SaslPrep does not allow left-to-right ("
                                        + String.valueOf(Character
                                                .toChars(codePoint))
                                        + ") characters without an initial left-to-right "
                                        + "character. See RFC 4013 section 2.4.");
                    }
                }
            }
        }
    }

    /**
     * Per-RFC 4013 section 2.3, throws a {@link SaslException} for any code
     * points in the {@code text} that are disallowed.
     * 
     * @param text
     *            The text to inspect.
     * @throws SaslException
     *             If any of the code points in the {@code text} that are
     *             disallowed.
     */
    private void saslPrepCheckProhibited(final String text)
            throws SaslException {
        final int length = text.length();
        int codePoint;
        for (int i = 0; i < length; i += Character.charCount(codePoint)) {
            codePoint = Character.codePointAt(text, i);

            if (SASL_PREP_DISALLOWED.contains(Integer.valueOf(codePoint))
                    // RFC 3454 - C.3 Private use
                    || ((0xE000 <= codePoint) && (codePoint <= 0xF8FF))
                    || ((0xF0000 <= codePoint) && (codePoint <= 0xFFFFD))
                    || ((0x100000 <= codePoint) && (codePoint <= 0x10FFFD)) ||
                    // RFC 3454 - C.5 Surrogate codes
                    ((0xD800 <= codePoint) && (codePoint <= 0xDFFF))) {
                throw new SaslException("SaslPrep disallowed character '"
                        + String.valueOf(Character.toChars(codePoint))
                        + "' (0x" + Integer.toHexString(codePoint)
                        + "). See RFC 4013 section 2.3.");
            }
        }
    }

    /**
     * Per-RFC 4013 section 2.5, throws a {@link SaslException} for any code
     * points in the {@code text} that have a {@link Character#getType(int)
     * type} of {@link Character#UNASSIGNED}.
     * 
     * @param text
     *            The text to inspect.
     * @throws SaslException
     *             If any of the code points in the {@code text} have a
     *             {@link Character#getType(int) type} of
     *             {@link Character#UNASSIGNED}.
     */
    private void saslPrepCheckUnassigned(final String text)
            throws SaslException {
        // RFC 3454 - A.1 Unassigned code points in Unicode 3.2
        final int nfkcLength = text.length();
        int codePoint;
        for (int i = 0; i < nfkcLength; i += Character.charCount(codePoint)) {
            codePoint = Character.codePointAt(text, i);
            final int type = Character.getType(codePoint);

            if (type == Character.UNASSIGNED) {
                throw new SaslException(
                        "SaslPrep does not allow unassigned unicode characters. "
                                + "See RFC 4013 section 2.5.");
            }
        }
    }

    /**
     * Per-RFC 4013 section 2.1, maps the specified character to the specified
     * replacements.
     * 
     * @param text
     *            The text to map.
     * @return The text updated with replacements.
     */
    private StringBuilder saslPrepMap(final char[] text) {
        final StringBuilder builder = new StringBuilder(text.length);
        for (final char c : text) {
            final String replacement = SASL_PREP_MAPPINGS.get(Character
                    .valueOf(c));
            if (replacement == null) {
                builder.append(c);
            }
            else {
                builder.append(replacement);
            }
        }

        return builder;
    }

    /**
     * State provides the state of the client.
     * 
     * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static enum State {
        /**
         * The state after the final message from the server acknowledging the
         * client's proof.
         */
        COMPLETE,

        /**
         * The state after the client sends the first message containing the
         * user name and nonce.
         */
        FIRST_SENT,

        /** The initial state for the client before any messages are exchanged. */
        INITIAL,

        /**
         * The state after the client sends the proof that it has the password
         * by hashing the complete state with the password.
         */
        PROOF_SENT
    }
}