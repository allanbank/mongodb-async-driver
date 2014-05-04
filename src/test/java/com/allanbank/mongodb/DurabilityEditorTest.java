/*
 * Copyright 2013-2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

/**
 * DurabilityEditorTest provides tests for the {@link DurabilityEditor} class.
 * 
 * @copyright 2013-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DurabilityEditorTest {

    /**
     * Test method for {@link DurabilityEditor#setAsText(String)}.
     */
    @Test
    public void testSetAck() {
        final DurabilityEditor editor = new DurabilityEditor();

        editor.setAsText("aCk");
        assertThat(editor.getValue(), is((Object) Durability.ACK));
    }

    /**
     * Test method for {@link DurabilityEditor#setAsText(String)}.
     */
    @Test
    public void testSetFsync() {
        final DurabilityEditor editor = new DurabilityEditor();

        editor.setAsText("FSYNC");
        assertThat(editor.getValue(),
                is((Object) Durability
                        .fsyncDurable(DurabilityEditor.DEFAULT_WAIT_TIME_MS)));
    }

    /**
     * Test method for {@link DurabilityEditor#setAsText(String)}.
     */
    @Test
    public void testSetJournal() {
        final DurabilityEditor editor = new DurabilityEditor();

        editor.setAsText("journal");
        assertThat(editor.getValue(),
                is((Object) Durability
                        .journalDurable(DurabilityEditor.DEFAULT_WAIT_TIME_MS)));
    }

    /**
     * Test method for {@link DurabilityEditor#setAsText(String)}.
     */
    @Test
    public void testSetJson() {
        final Durability durability = Durability.replicaDurable(true, "myMode",
                1234567);
        final DurabilityEditor editor = new DurabilityEditor();

        editor.setAsText(durability.asDocument().toString());
        assertThat(editor.getValue(), is((Object) durability));
    }

    /**
     * Test method for {@link DurabilityEditor#setAsText(String)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetJsonNotDurability() {
        final DurabilityEditor editor = new DurabilityEditor();

        editor.setAsText("{ foo : '1' }");
    }

    /**
     * Test method for {@link DurabilityEditor#setAsText(String)}.
     */
    @Test
    public void testSetMajority() {
        final DurabilityEditor editor = new DurabilityEditor();

        editor.setAsText("MAJORITy");
        assertThat(editor.getValue(),
                is((Object) Durability.replicaDurable(Durability.MAJORITY_MODE,
                        DurabilityEditor.DEFAULT_WAIT_TIME_MS)));
    }

    /**
     * Test method for {@link DurabilityEditor#setAsText(String)}.
     */
    @Test
    public void testSetNone() {
        final DurabilityEditor editor = new DurabilityEditor();

        editor.setAsText("NoNe");
        assertThat(editor.getValue(), is((Object) Durability.NONE));
    }

    /**
     * Test method for {@link DurabilityEditor#setAsText(String)}.
     */
    @Test
    public void testUriWMajortity() {
        final DurabilityEditor editor = new DurabilityEditor();

        editor.setAsText("mongodb://host:port?w=majority");
        assertThat(editor.getValue(),
                is((Object) Durability.replicaDurable("majority", 0)));
    }
}
