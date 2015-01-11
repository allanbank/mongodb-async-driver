/*
 * #%L
 * PatternUtilsTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.util.regex.Pattern;

import org.junit.Test;

/**
 * PatternUtilsTest provides tests for the {@link PatternUtils} class.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class PatternUtilsTest {

    /**
     * Test method for {@link PatternUtils#toPattern(java.lang.String)}.
     */
    @Test
    public void testToPattern() {
        final Pattern p = PatternUtils.toPattern("abc.*def");

        assertNotNull(p);
        assertEquals("abc.*def", p.pattern());
    }

    /**
     * Test method for {@link PatternUtils#toPattern(java.lang.String)}.
     */
    @Test
    public void testToPatternCachesTheAllPattern() {
        final Pattern p = PatternUtils.toPattern(".*");

        assertNotNull(p);
        assertSame(PatternUtils.ALL_PATTERN, p);
    }

}
