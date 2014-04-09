/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
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