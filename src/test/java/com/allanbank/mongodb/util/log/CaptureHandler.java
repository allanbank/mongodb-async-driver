/*
 * #%L
 * CaptureHandler.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.util.log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

/**
 * A JUL Handler to capture the log records created.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CaptureHandler extends Handler {

    /** The captured log records. */
    private final List<LogRecord> myRecords = new ArrayList<LogRecord>();

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws SecurityException {
        myRecords.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush() {
        // Nothing.
    }

    /**
     * Returns the captured {@link LogRecord}s.
     * 
     * @return the captured {@link LogRecord}s.
     */
    public List<LogRecord> getRecords() {
        return Collections.unmodifiableList(myRecords);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(final LogRecord record) {
        myRecords.add(record);
    }
}