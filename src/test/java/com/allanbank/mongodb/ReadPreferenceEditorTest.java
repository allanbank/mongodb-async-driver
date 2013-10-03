/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * ReadPreferenceEditorTest provides tests for the {@link ReadPreferenceEditor}
 * class.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReadPreferenceEditorTest {

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test
    public void testSetClosest() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("Closest");
        assertThat(editor.getValue(), is((Object) ReadPreference.CLOSEST));
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetJsonBadMode() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("{ mode : 'foo' }");
        assertThat(editor.getValue(),
                is((Object) ReadPreference.preferSecondary()));
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test
    public void testSetJsonNearest() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("{ mode : 'NEAREST' }");
        assertThat(editor.getValue(), is((Object) ReadPreference.closest()));
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test
    public void testSetJsonNearestWithTags() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("{ mode : 'NEAREST', tags : [ { a : 1 } ] }");
        assertThat(
                editor.getValue(),
                is((Object) ReadPreference.closest(BuilderFactory.start().add(
                        "a", 1))));
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetJsonNoMode() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("{ tags : [ { a : 1 } ] }");
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test
    public void testSetJsonNoTags() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("{ mode : 'SECONDARY_PREFERRED' }");
        assertThat(editor.getValue(),
                is((Object) ReadPreference.preferSecondary()));
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test
    public void testSetJsonPrimaryOnly() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("{ mode : 'PRIMARY_ONLY', tags : [ { a : 1 } ] }");
        assertThat(editor.getValue(), is((Object) ReadPreference.primary()));
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test
    public void testSetJsonPrimaryPreferred() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("{ mode : 'PRIMARY_PREFERRED', tags : [ { a : 1 } ] }");
        assertThat(editor.getValue(),
                is((Object) ReadPreference.preferPrimary(BuilderFactory.start()
                        .add("a", 1))));
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test
    public void testSetJsonSecondaryOnly() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("{ mode : 'SECONDARY_ONLY', tags : [ { a : 1 } ] }");
        assertThat(editor.getValue(),
                is((Object) ReadPreference.secondary(BuilderFactory.start()
                        .add("a", 1))));
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test
    public void testSetJsonSecondaryPreferred() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("{ mode : 'SECONDARY_PREFERRED', tags : [ { a : 1 } ] }");
        assertThat(editor.getValue(),
                is((Object) ReadPreference.preferSecondary(BuilderFactory
                        .start().add("a", 1))));
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test
    public void testSetJsonServer() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("{ mode : 'SERVER', server : 'foo', tags : [ { a : 1 } ] }");
        assertThat(editor.getValue(), is((Object) ReadPreference.server("foo")));
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetJsonServerWithoutServer() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("{ mode : 'SERVER', tags : [ { a : 1 } ] }");
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test
    public void testSetJsonTagsWrongType() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("{ mode : 'SECONDARY_PREFERRED', tags : 1 }");
        assertThat(editor.getValue(),
                is((Object) ReadPreference.preferSecondary()));
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test
    public void testSetNearest() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("NeaRest");
        assertThat(editor.getValue(), is((Object) ReadPreference.CLOSEST));
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetNonJson() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("'foo'");
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test
    public void testSetPreferPrimary() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("prefer_primary");
        assertThat(editor.getValue(),
                is((Object) ReadPreference.PREFER_PRIMARY));
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test
    public void testSetPreferSecondary() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("Prefer_Secondary");
        assertThat(editor.getValue(),
                is((Object) ReadPreference.PREFER_SECONDARY));
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test
    public void testSetPrimary() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("PRIMARY");
        assertThat(editor.getValue(), is((Object) ReadPreference.PRIMARY));
    }

    /**
     * Test method for {@link ReadPreferenceEditor#setAsText(String)}.
     */
    @Test
    public void testSetSecondary() {
        final ReadPreferenceEditor editor = new ReadPreferenceEditor();

        editor.setAsText("secondary");
        assertThat(editor.getValue(), is((Object) ReadPreference.SECONDARY));
    }
}
