/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import static com.allanbank.mongodb.util.Assertions.assertNotNull;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON symbol.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SymbolElement extends AbstractElement {

    /** The BSON type for a symbol. */
    public static final ElementType TYPE = ElementType.SYMBOL;

    /** Serialization version for the class. */
    private static final long serialVersionUID = -3181997000292958333L;

    /** The BSON string value. */
    private final String mySymbol;

    /**
     * Constructs a new {@link SymbolElement}.
     * 
     * @param name
     *            The name for the BSON string.
     * @param symbol
     *            The BSON symbol value.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code symbol} is <code>null</code>.
     */
    public SymbolElement(final String name, final String symbol) {
        super(name);

        assertNotNull(symbol, "Symbol element's symbol cannot be null.");

        mySymbol = symbol;
    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitSymbol} method.
     * 
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitSymbol(getName(), getSymbol());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to compare the string values if the base class comparison is
     * equals.
     * </p>
     * <p>
     * Note that for MongoDB {@link SymbolElement} and {@link StringElement}
     * will return equal based on the type. Care is taken here to make sure that
     * the values return the same value regardless of comparison order.
     * </p>
     */
    @Override
    public int compareTo(final Element otherElement) {
        int result = super.compareTo(otherElement);

        if (result == 0) {
            // Might be a StringElement or SymbolElement.
            final ElementType otherType = otherElement.getType();

            if (otherType == ElementType.SYMBOL) {
                result = mySymbol.compareTo(((SymbolElement) otherElement)
                        .getSymbol());
            }
            else {
                result = mySymbol.compareTo(((StringElement) otherElement)
                        .getValue());
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
            final SymbolElement other = (SymbolElement) object;

            result = super.equals(object)
                    && nullSafeEquals(mySymbol, other.mySymbol);
        }
        return result;
    }

    /**
     * Returns the BSON symbol value.
     * 
     * @return The BSON symbol value.
     */
    public String getSymbol() {
        return mySymbol;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return TYPE;
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
        result = (31 * result) + ((mySymbol != null) ? mySymbol.hashCode() : 3);
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link SymbolElement}.
     * </p>
     */
    @Override
    public SymbolElement withName(final String name) {
        return new SymbolElement(name, mySymbol);
    }
}
