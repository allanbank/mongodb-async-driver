/*
 * #%L
 * DocumentReferenceTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.bson;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.LongElement;
import com.allanbank.mongodb.bson.element.StringElement;

/**
 * DocumentReferenceTest provides tests for the {@link DocumentReference} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DocumentReferenceTest {

    /**
     * Test method for {@link DocumentReference#asDocument()}.
     */
    @Test
    public void testAsDocument() {
        final DocumentReference ref = new DocumentReference("d", "c",
                new IntegerElement("a", 1));
        final Document doc = ref.asDocument();

        assertEquals(new StringElement(DocumentReference.DATABASE_FIELD_NAME,
                "d"), doc.get(DocumentReference.DATABASE_FIELD_NAME));
        assertEquals(new StringElement(DocumentReference.COLLECTION_FIELD_NAME,
                "c"), doc.get(DocumentReference.COLLECTION_FIELD_NAME));
        assertEquals(new IntegerElement(DocumentReference.ID_FIELD_NAME, 1),
                doc.get(DocumentReference.ID_FIELD_NAME));
    }

    /**
     * Test method for {@link DocumentReference#asDocument()}.
     */
    @Test
    public void testAsDocumentWithNullDatabaseName() {
        final DocumentReference ref = new DocumentReference("c",
                new IntegerElement("a", 1));
        final Document doc = ref.asDocument();

        assertNull(doc.get(DocumentReference.DATABASE_FIELD_NAME));
        assertEquals(new StringElement(DocumentReference.COLLECTION_FIELD_NAME,
                "c"), doc.get(DocumentReference.COLLECTION_FIELD_NAME));
        assertEquals(new IntegerElement(DocumentReference.ID_FIELD_NAME, 1),
                doc.get(DocumentReference.ID_FIELD_NAME));
    }

    /**
     * Test method for
     * {@link DocumentReference#DocumentReference(String, Element)} .
     */
    @Test
    public void testDocumentReferenceStringElement() {
        final DocumentReference ref = new DocumentReference("c",
                new IntegerElement("a", 1));

        assertNull(ref.getDatabaseName());
        assertEquals("c", ref.getCollectionName());
        assertEquals(new IntegerElement("$id", 1), ref.getId());
    }

    /**
     * Test method for
     * {@link DocumentReference#DocumentReference(String, Element)} .
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testDocumentReferenceStringElementWithNullCollectionNameThrows() {
        new DocumentReference(null, new IntegerElement("a", 1));
    }

    /**
     * Test method for
     * {@link DocumentReference#DocumentReference(String, Element)} .
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testDocumentReferenceStringElementWithNullIdThrows() {
        new DocumentReference("c", null);
    }

    /**
     * Test method for
     * {@link DocumentReference#DocumentReference(String, String, Element)} .
     */
    @Test
    public void testDocumentReferenceStringStringElement() {
        final DocumentReference ref = new DocumentReference("d", "c",
                new IntegerElement("a", 1));

        assertEquals("d", ref.getDatabaseName());
        assertEquals("c", ref.getCollectionName());
        assertEquals(new IntegerElement("$id", 1), ref.getId());
    }

    /**
     * Test method for {@link DocumentReference#equals(java.lang.Object)} .
     */
    @Test
    public void testEqualsObject() {
        final Random random = new Random(System.currentTimeMillis());

        final List<DocumentReference> objs1 = new ArrayList<DocumentReference>();
        final List<DocumentReference> objs2 = new ArrayList<DocumentReference>();

        for (int i = 0; i < 10; ++i) {
            final int value = random.nextInt();
            for (final String collection : Arrays.asList("1", "foo", "bar",
                    "baz", "2")) {
                for (final String database : Arrays.asList("1", "foo", "bar",
                        "baz", null, "2")) {
                    objs1.add(new DocumentReference(database, collection,
                            new IntegerElement("a", value)));
                    objs2.add(new DocumentReference(database, collection,
                            new IntegerElement("a", value)));

                    if (database != null) {
                        // Check for non identity.
                        objs1.add(new DocumentReference(database, collection,
                                new DoubleElement("a", value)));
                        objs2.add(new DocumentReference(new String(database),
                                new String(collection), new DoubleElement("b",
                                        value)));
                    }
                }
            }
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final DocumentReference obj1 = objs1.get(i);
            DocumentReference obj2 = objs2.get(i);

            assertTrue(obj1.equals(obj1));
            assertNotSame(obj1, obj2);
            assertEquals(obj1, obj2);

            assertEquals(obj1.hashCode(), obj2.hashCode());

            for (int j = i + 1; j < objs1.size(); ++j) {
                obj2 = objs2.get(j);

                assertFalse(obj1.equals(obj2));
                assertFalse(obj1.hashCode() == obj2.hashCode());
            }

            assertFalse(obj1.equals("foo"));
            assertFalse(obj1.equals(null));
        }
    }

    /**
     * Test method for {@link DocumentReference#toString()}.
     */
    @Test
    public void testToString() {
        final DocumentReference ref = new DocumentReference("d", "c",
                new IntegerElement("a", 1));
        final String expected = "{ '$ref' : 'c', '$id' : 1, '$db' : 'd' }";
        final String result = ref.toString();

        assertEquals(expected, result);
    }

    /**
     * Test method for {@link DocumentReference#toString()}.
     */
    @Test
    public void testToStringWithNullDatabaseName() {
        final DocumentReference ref = new DocumentReference("c",
                new LongElement("a", 1));
        final String expected = "{ '$ref' : 'c', '$id' : NumberLong('1') }";
        final String result = ref.toString();

        assertEquals(expected, result);
    }
}
