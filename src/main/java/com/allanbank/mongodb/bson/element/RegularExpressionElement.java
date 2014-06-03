/*
 * #%L
 * RegularExpressionElement.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.bson.element;

import static com.allanbank.mongodb.util.Assertions.assertNotNull;

import java.util.regex.Pattern;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.io.StringEncoder;

/**
 * A wrapper for a BSON regular expression.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class RegularExpressionElement extends AbstractElement {

    /** Option for case insensitive matching. */
    public static final int CASE_INSENSITIVE;

    /** Option for dotall mode ('.' matches everything). */
    public static final int DOT_ALL;

    /** Option to make \w, \W, etc. locale dependent. */
    public static final int LOCALE_DEPENDENT;

    /** Option for multiline matching. */
    public static final int MULTILINE;

    /** Option for case insensitive matching. */
    public static final int OPTION_I;

    /** Option to make \w, \W, etc. locale dependent. */
    public static final int OPTION_L;

    /** Option for multiline matching. */
    public static final int OPTION_M;

    /** Option for verbose mode. */
    public static final int OPTION_MASK;

    /** Option for dotall mode ('.' matches everything). */
    public static final int OPTION_S;

    /** Option to make \w, \W, etc. match unicode. */
    public static final int OPTION_U;

    /** Option for verbose mode. */
    public static final int OPTION_X;

    /** The BSON type for a string. */
    public static final ElementType TYPE = ElementType.REGEX;

    /** Option to make \w, \W, etc. match unicode. */
    public static final int UNICODE;

    /** Option for verbose mode. */
    public static final int VERBOSE;

    /**
     * Option to make \w, \W, etc. match unicode from the pattern class. Added
     * in Java7
     */
    protected static final int PATTERN_UNICODE;

    /** The options for each possible bit field. */
    private static final String[] OPTIONS;

    /** Serialization version for the class. */
    private static final long serialVersionUID = 7842839168833403380L;

    static {

        OPTION_I = 0x01;
        OPTION_L = 0x02;
        OPTION_M = 0x04;
        OPTION_S = 0x08;
        OPTION_U = 0x10;
        OPTION_X = 0x20;
        OPTION_MASK = 0x3F;

        CASE_INSENSITIVE = OPTION_I;
        LOCALE_DEPENDENT = OPTION_L;
        MULTILINE = OPTION_M;
        DOT_ALL = OPTION_S;
        UNICODE = OPTION_U;
        VERBOSE = OPTION_X;

        final String[] options = new String[OPTION_MASK + 1];

        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < (OPTION_MASK + 1); ++i) {
            builder.setLength(0);

            // Options must be in alphabetic order.
            if ((i & OPTION_I) == OPTION_I) {
                builder.append('i');
            }
            if ((i & OPTION_L) == OPTION_L) {
                builder.append('l');
            }
            if ((i & OPTION_M) == OPTION_M) {
                builder.append('m');
            }
            if ((i & OPTION_S) == OPTION_S) {
                builder.append('s');
            }
            if ((i & OPTION_U) == OPTION_U) {
                builder.append('u');
            }
            if ((i & OPTION_X) == OPTION_X) {
                builder.append('x');
            }
            options[i] = builder.toString();
        }

        OPTIONS = options;

        // New in Java7
        PATTERN_UNICODE = 0x100;
    }

    /**
     * Converts the {@link Pattern#flags() pattern flags} into a options value.
     * <p>
     * Note that the {@link #VERBOSE} and {@link #LOCALE_DEPENDENT} do not have
     * {@link Pattern} equivalent flags.
     * </p>
     * <p>
     * <blockquote>
     * 
     * <pre>
     * {@link Pattern#CASE_INSENSITIVE} ==> {@link #CASE_INSENSITIVE}
     * {@link Pattern#MULTILINE} ==> {@link #MULTILINE}
     * {@link Pattern#DOTALL} ==> {@link #DOT_ALL}
     * {@link Pattern#UNICODE_CHARACTER_CLASS} ==> {@link #UNICODE}
     * </pre>
     * 
     * </blockquote>
     * 
     * @param pattern
     *            The pattern to extract the options from.
     * @return The options integer value.
     */
    @SuppressWarnings("javadoc")
    protected static int optionsAsInt(final Pattern pattern) {
        int optInt = 0;

        if (pattern != null) {
            final int flags = pattern.flags();
            if ((flags & Pattern.CASE_INSENSITIVE) == Pattern.CASE_INSENSITIVE) {
                optInt |= CASE_INSENSITIVE;
            }
            if ((flags & Pattern.MULTILINE) == Pattern.MULTILINE) {
                optInt |= MULTILINE;
            }
            if ((flags & Pattern.DOTALL) == Pattern.DOTALL) {
                optInt |= DOT_ALL;
            }
            if ((flags & PATTERN_UNICODE) == PATTERN_UNICODE) {
                optInt |= UNICODE;
            }
        }

        return optInt;
    }

    /**
     * Converts the options string into a options value.
     * 
     * @param options
     *            The possibly non-normalized options string.
     * @return The options integer value.
     */
    protected static int optionsAsInt(final String options) {
        int optInt = 0;

        if (options != null) {
            for (final char c : options.toCharArray()) {
                if ((c == 'i') || (c == 'I')) {
                    optInt |= OPTION_I;
                }
                else if ((c == 'l') || (c == 'L')) {
                    optInt |= OPTION_L;
                }
                else if ((c == 'm') || (c == 'M')) {
                    optInt |= OPTION_M;
                }
                else if ((c == 's') || (c == 'S')) {
                    optInt |= OPTION_S;
                }
                else if ((c == 'u') || (c == 'U')) {
                    optInt |= OPTION_U;
                }
                else if ((c == 'x') || (c == 'X')) {
                    optInt |= OPTION_X;
                }
                else {
                    throw new IllegalArgumentException(
                            "Invalid regular expression option '" + c
                                    + "' in options '" + options + "'.");
                }
            }
        }

        return optInt;
    }

    /**
     * Computes and returns the number of bytes that are used to encode the
     * element.
     * 
     * @param name
     *            The name for the element.
     * @param pattern
     *            The BSON regular expression pattern.
     * @param options
     *            The BSON regular expression options.
     * @return The size of the element when encoded in bytes.
     */
    private static long computeSize(final String name, final String pattern,
            final int options) {
        long result = 4; // type (1) + name null byte (1) +
        // pattern null byte (1) + options null byte (1).
        result += StringEncoder.utf8Size(name);
        result += StringEncoder.utf8Size(pattern);
        result += OPTIONS[options & OPTION_MASK].length(); // ASCII

        return result;
    }

    /** The BSON regular expression options. */
    private final int myOptions;

    /** The BSON regular expression pattern. */
    private final String myPattern;

    /**
     * Constructs a new {@link RegularExpressionElement}.
     * 
     * @param name
     *            The name for the BSON string.
     * @param pattern
     *            The regular expression {@link Pattern}.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code pattern} is <code>null</code>.
     */
    public RegularExpressionElement(final String name, final Pattern pattern) {
        this(name, (pattern != null) ? pattern.pattern() : null,
                optionsAsInt(pattern));
    }

    /**
     * Constructs a new {@link RegularExpressionElement}.
     * 
     * @param name
     *            The name for the BSON string.
     * @param pattern
     *            The BSON regular expression pattern.
     * @param options
     *            The BSON regular expression options.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code pattern} is <code>null</code>.
     */
    public RegularExpressionElement(final String name, final String pattern,
            final int options) {
        super(name, computeSize(name, pattern, options));

        assertNotNull(pattern,
                "Regular Expression element's pattern cannot be null.");

        myPattern = pattern;
        myOptions = options;
    }

    /**
     * Constructs a new {@link RegularExpressionElement}.
     * 
     * @param name
     *            The name for the BSON string.
     * @param pattern
     *            The BSON regular expression pattern.
     * @param options
     *            The BSON regular expression options.
     * @param size
     *            The size of the element when encoded in bytes. If not known
     *            then use the
     *            {@link RegularExpressionElement#RegularExpressionElement(String, String, int)}
     *            constructor instead.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code pattern} is <code>null</code>.
     */
    public RegularExpressionElement(final String name, final String pattern,
            final int options, final long size) {
        super(name, size);

        assertNotNull(pattern,
                "Regular Expression element's pattern cannot be null.");

        myPattern = pattern;
        myOptions = options;
    }

    /**
     * Constructs a new {@link RegularExpressionElement}.
     * 
     * @param name
     *            The name for the BSON string.
     * @param pattern
     *            The BSON regular expression pattern.
     * @param options
     *            The BSON regular expression options.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code pattern} is <code>null</code>.
     */
    public RegularExpressionElement(final String name, final String pattern,
            final String options) {
        this(name, pattern, optionsAsInt(options));
    }

    /**
     * Constructs a new {@link RegularExpressionElement}.
     * 
     * @param name
     *            The name for the BSON string.
     * @param pattern
     *            The BSON regular expression pattern.
     * @param options
     *            The BSON regular expression options.
     * @param size
     *            The size of the element when encoded in bytes. If not known
     *            then use the
     *            {@link RegularExpressionElement#RegularExpressionElement(String, String, String)}
     *            constructor instead.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code pattern} is <code>null</code>.
     */
    public RegularExpressionElement(final String name, final String pattern,
            final String options, final long size) {
        this(name, pattern, optionsAsInt(options), size);
    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitRegularExpression}
     * method.
     * 
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitRegularExpression(getName(), getPattern(),
                OPTIONS[getOptions() & OPTION_MASK]);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to compare the expressions (as strings) if the base class
     * comparison is equals.
     * </p>
     */
    @Override
    public int compareTo(final Element otherElement) {
        int result = super.compareTo(otherElement);

        if (result == 0) {
            final RegularExpressionElement other = (RegularExpressionElement) otherElement;

            result = myPattern.compareTo(other.myPattern);
            if (result == 0) {
                result = compare(myOptions, other.myOptions);
            }
        }

        return result;
    }

    /**
     * Determines if the passed object is of this same type as this object and
     * if so that its fields are equal.
     * 
     * @param object
     *            The object to compare to.
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object object) {
        boolean result = false;
        if (this == object) {
            result = true;
        }
        else if ((object != null) && (getClass() == object.getClass())) {
            final RegularExpressionElement other = (RegularExpressionElement) object;

            result = (myOptions == other.myOptions) && super.equals(object)
                    && nullSafeEquals(myPattern, other.myPattern);
        }
        return result;
    }

    /**
     * Returns the regular expression options.
     * 
     * @return The regular expression options.
     */
    public int getOptions() {
        return myOptions;
    }

    /**
     * Returns the regular expression pattern.
     * 
     * @return The regular expression pattern.
     */
    public String getPattern() {
        return myPattern;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return TYPE;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the {@link Pattern}.
     * </p>
     */
    @Override
    public Pattern getValueAsObject() {

        int options = 0;
        if ((myOptions & CASE_INSENSITIVE) == CASE_INSENSITIVE) {
            options |= Pattern.CASE_INSENSITIVE;
        }
        if ((myOptions & MULTILINE) == MULTILINE) {
            options |= Pattern.MULTILINE;
        }
        if ((myOptions & DOT_ALL) == DOT_ALL) {
            options |= Pattern.DOTALL;
        }
        if ((myOptions & UNICODE) == UNICODE) {
            options |= PATTERN_UNICODE;
        }

        return Pattern.compile(myPattern, options);
    }

    /**
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + super.hashCode();
        result = (31 * result)
                + ((myPattern != null) ? myPattern.hashCode() : 3);
        result = (31 * result) + myOptions;
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link RegularExpressionElement}.
     * </p>
     */
    @Override
    public RegularExpressionElement withName(final String name) {
        if (getName().equals(name)) {
            return this;
        }
        return new RegularExpressionElement(name, myPattern, myOptions);
    }
}
