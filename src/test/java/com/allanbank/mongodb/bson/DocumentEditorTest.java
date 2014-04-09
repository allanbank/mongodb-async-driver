/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
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
