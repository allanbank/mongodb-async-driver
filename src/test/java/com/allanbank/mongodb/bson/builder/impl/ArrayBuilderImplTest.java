/*
 * #%L
 * ArrayBuilderImplTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.bson.builder.impl;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigInteger;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.junit.Test;

import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementAssignable;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.LongElement;
import com.allanbank.mongodb.bson.element.NullElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.ObjectIdElement;
import com.allanbank.mongodb.bson.element.RegularExpressionElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.TimestampElement;
import com.allanbank.mongodb.bson.element.UuidElement;

/**
 * ArrayBuilderImplTest provides tests for the {@link ArrayBuilderImpl} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ArrayBuilderImplTest {

    /**
     * Test method for the {@link ArrayBuilderImpl#add(byte[])}.
     */
    @Test
    public void testAddByteArray() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        builder.add(new byte[] { 1 });
        assertEquals(new BinaryElement("0", new byte[] { 1 }),
                builder.build()[0]);

        builder.reset();
        builder.add((byte[]) null);
        assertEquals(new NullElement("0"), builder.build()[0]);
    }

    /**
     * Test method for the {@link ArrayBuilderImpl#add(Date)}.
     */
    @Test
    public void testAddDate() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        final Date d = new Date();
        builder.add(d);
        assertEquals(new TimestampElement("0", d.getTime()), builder.build()[0]);

        builder.reset();
        builder.add((Date) null);
        assertEquals(new NullElement("0"), builder.build()[0]);
    }

    /**
     * Test method for the {@link ArrayBuilderImpl#add(DocumentAssignable)}.
     */
    @Test
    public void testAddDocumentAssignable() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        final DocumentElement d = new DocumentElement("g");
        builder.add((DocumentAssignable) d);
        assertEquals(d.withName("0"), builder.build()[0]);

        builder.reset();
        builder.add((DocumentAssignable) null);
        assertEquals(new NullElement("0"), builder.build()[0]);
    }

    /**
     * Test method for the {@link ArrayBuilderImpl#add(DocumentElement)}.
     */
    @Test
    public void testAddDocumentElement() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        final DocumentElement d = new DocumentElement("g");
        builder.add(d);
        assertEquals(d.withName("0"), builder.build()[0]);

        builder.reset();
        builder.add((DocumentElement) null);
        assertEquals(new NullElement("0"), builder.build()[0]);
    }

    /**
     * Test method for the {@link ArrayBuilderImpl#add(ElementAssignable)}.
     */
    @Test
    public void testAddElementAssignable() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        final DocumentElement d = new DocumentElement("g");
        builder.add((ElementAssignable) d);
        assertEquals(new DocumentElement("0"), builder.build()[0]);

        builder.reset();
        try {
            builder.add((ElementAssignable) null);
            fail("Should have thrown an IllegalArgumentException.");
        }
        catch (final IllegalArgumentException good) {
            // Good.
        }
    }

    /**
     * Test method for the {@link ArrayBuilderImpl#addLegacyUuid(UUID)}.
     */
    @Test
    public void testAddLegacyUuid() {
        final UUID uuid = UUID.randomUUID();
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        builder.addLegacyUuid(uuid);
        assertEquals(new UuidElement("0", UuidElement.LEGACY_UUID_SUBTTYPE,
                uuid), builder.build()[0]);

        builder.reset();
        try {
            builder.addLegacyUuid((UUID) null);
            fail("Should have thrown a IllegalArguementException.");
        }
        catch (final IllegalArgumentException good) {
            // Good.
        }
    }

    /**
     * Test method for the {@link ArrayBuilderImpl#add(Object)}.
     */
    @Test
    public void testAddObject() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        builder.reset().add((Object) null);
        assertEquals(new NullElement("0"), builder.build()[0]);

        builder.reset().add(Boolean.valueOf(false));
        assertEquals(new BooleanElement("0", false), builder.build()[0]);

        builder.reset().add(Long.valueOf(3));
        assertEquals(new LongElement("0", 3), builder.build()[0]);

        builder.reset().add(BigInteger.valueOf(3));
        assertEquals(new LongElement("0", 3), builder.build()[0]);

        builder.reset().add(Double.valueOf(1.01));
        assertEquals(new DoubleElement("0", 1.01), builder.build()[0]);

        builder.reset().add(Float.valueOf(1.01F));
        assertEquals(new DoubleElement("0", 1.01F), builder.build()[0]);

        builder.reset().add(Short.valueOf((short) 1));
        assertEquals(new IntegerElement("0", 1), builder.build()[0]);

        builder.reset().add(new byte[12]);
        assertEquals(new BinaryElement("0", new byte[12]), builder.build()[0]);

        final ObjectId objectid = new ObjectId();
        builder.reset().add((Object) objectid);
        assertEquals(new ObjectIdElement("0", objectid), builder.build()[0]);

        final Pattern pattern = Pattern.compile("1234");
        builder.reset().add((Object) pattern);
        assertEquals(new RegularExpressionElement("0", pattern),
                builder.build()[0]);

        builder.reset().add((Object) "a");
        assertEquals(new StringElement("0", "a"), builder.build()[0]);

        final Date date = new Date();
        builder.reset().add((Object) date);
        assertEquals(new TimestampElement("0", date.getTime()),
                builder.build()[0]);

        final Calendar calendar = Calendar.getInstance();
        builder.reset().add(calendar);
        assertEquals(new TimestampElement("0", calendar.getTime().getTime()),
                builder.build()[0]);

        final UUID uuid = UUID.randomUUID();
        builder.reset().add((Object) uuid);
        assertEquals(new UuidElement("0", uuid), builder.build()[0]);

        final byte[] bytes = new byte[3];
        builder.reset().add((Object) bytes);
        assertEquals(new BinaryElement("0", bytes), builder.build()[0]);

        final DocumentAssignable b2 = BuilderFactory.start();
        builder.reset().add((Object) b2);
        assertEquals(new DocumentElement("0", b2.asDocument()),
                builder.build()[0]);

        final Element e1 = new IntegerElement("a", 1);
        builder.reset().add(e1);
        assertEquals(new IntegerElement("0", 1), builder.build()[0]);

        final Map<String, String> map = Collections.singletonMap("a", "b");
        builder.reset().add(map);
        assertEquals(new DocumentElement("0", new StringElement("a", "b")),
                builder.build()[0]);

        final Collection<String> collection = Collections.singleton("a");
        builder.reset().add(collection);
        assertEquals(new ArrayElement("0", new StringElement("0", "a")),
                builder.build()[0]);

        final String[] array = new String[] { "a" };
        builder.reset().add(array);
        assertEquals(new ArrayElement("0", new StringElement("0", "a")),
                builder.build()[0]);
    }

    /**
     * Test method for the {@link ArrayBuilderImpl#add(ObjectId)}.
     */
    @Test
    public void testAddObjectId() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        final ObjectId d = new ObjectId();
        builder.add(d);
        assertEquals(new ObjectIdElement("0", d), builder.build()[0]);

        builder.reset();
        builder.add((ObjectId) null);
        assertEquals(new NullElement("0"), builder.build()[0]);
    }

    /**
     * Test method for the {@link ArrayBuilderImpl#add(Pattern)}.
     */
    @Test
    public void testAddPattern() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        final Pattern d = Pattern.compile(".*");
        builder.add(d);
        assertEquals(new RegularExpressionElement("0", d), builder.build()[0]);

        builder.reset();
        builder.add((Pattern) null);
        assertEquals(new NullElement("0"), builder.build()[0]);
    }

    /**
     * Test method for the {@link ArrayBuilderImpl#add(String)}.
     */
    @Test
    public void testAddString() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        final String d = ".*";
        builder.add(d);
        assertEquals(new StringElement("0", d), builder.build()[0]);

        builder.reset();
        builder.add((String) null);
        assertEquals(new NullElement("0"), builder.build()[0]);
    }

    /**
     * Test method for the {@link ArrayBuilderImpl#add(Object)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testAddStringObjectWithUncoercable() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        builder.reset().add(new Object());
    }

    /**
     * Test method for the {@link ArrayBuilderImpl#add(UUID)}.
     */
    @Test
    public void testAddStringUUID() {
        final UUID uuid = UUID.randomUUID();
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        builder.add(uuid);
        assertEquals(new UuidElement("0", uuid), builder.build()[0]);

        builder.reset();
        builder.add((UUID) null);
        assertEquals(new NullElement("0"), builder.build()[0]);

    }

    /**
     * Test method for the {@link ArrayBuilderImpl#addUuid(UUID)}.
     */
    @Test
    public void testAddUuid() {
        final UUID uuid = UUID.randomUUID();
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        builder.addUuid(uuid);
        assertEquals(new UuidElement("0", uuid), builder.build()[0]);

        builder.reset();
        try {
            builder.addUuid((UUID) null);
            fail("Should have thrown a IllegalArguementException.");
        }
        catch (final IllegalArgumentException good) {
            // Good.
        }
    }

    /**
     * Test method for {@link ArrayBuilderImpl#pushArray()}.
     */
    @Test
    public void testPushArray() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        builder.pushArray();

        final ArrayElement element = builder.build("foo");
        assertTrue(element.getEntries().size() == 1);
        assertTrue(element.getEntries().get(0) instanceof ArrayElement);
        assertTrue(((ArrayElement) element.getEntries().get(0)).getEntries()
                .size() == 0);
    }

    /**
     * Test method for {@link ArrayBuilderImpl#reset()}.
     */
    @Test
    public void testReset() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        builder.pushArray();

        ArrayElement element = builder.build("foo");
        assertTrue(element.getEntries().size() == 1);
        assertTrue(element.getEntries().get(0) instanceof ArrayElement);
        assertTrue(((ArrayElement) element.getEntries().get(0)).getEntries()
                .size() == 0);

        assertSame(builder, builder.reset());

        element = builder.build("foo");
        assertTrue(element.getEntries().size() == 0);
    }

    /**
     * Test method for the {@link DocumentBuilderImpl#toString()}.
     */
    @Test
    public void testToString() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        builder.add(new byte[] { 1 });

        assertThat(builder.toString(),
                is(new ArrayElement("elements", builder.subElements())
                        .toString()));
    }

}
