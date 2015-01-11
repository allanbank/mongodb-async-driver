/*
 * #%L
 * DocumentEditorTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * DocumentEditorTest provides tests for the {@link DocumentEditor} class.
 *
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DocumentEditorTest {

    /**
     * Test method for {@link DocumentEditor#setAsText(String)}.
     */
    @Test
    public void testSetAsText() {

        final DocumentEditor editor = new DocumentEditor();

        editor.setAsText("{ a : 1 } ");

        assertThat(editor.getValue(),
                is((Object) BuilderFactory.start().add("a", 1).build()));
    }

    /**
     * Test method for {@link DocumentEditor#setAsText(String)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetAsTextNonJson() {

        final DocumentEditor editor = new DocumentEditor();

        editor.setAsText("{ a : 1  ");
    }
}
