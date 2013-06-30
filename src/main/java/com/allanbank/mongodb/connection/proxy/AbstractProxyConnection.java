/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.proxy;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.util.IOUtils;

/**
 * A helper class for constructing connections that are really just proxies on
 * top of other connections.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractProxyConnection implements Connection {

    /** The MongoDB client configuration. */
    protected final MongoClientConfiguration myConfig;

    /** The proxied connection. */
    private final Connection myProxiedConnection;

    /**
     * Creates a AbstractProxyConnection.
     * 
     * @param proxiedConnection
     *            The connection to forward to.
     * @param config
     *            The MongoDB client configuration.
     */
    public AbstractProxyConnection(final Connection proxiedConnection,
            final MongoClientConfiguration config) {
        myProxiedConnection = proxiedConnection;
        myConfig = config;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to create a proxy connection if not already created and add
     * the listener to that connection.
     * </p>
     */
    @Override
    public void addPropertyChangeListener(final PropertyChangeListener listener) {
        try {
            myProxiedConnection
                    .addPropertyChangeListener(new ProxiedChangeListener(this,
                            listener));
        }
        catch (final MongoDbException error) {
            onExceptin(error);
            listener.propertyChange(new PropertyChangeEvent(this,
                    OPEN_PROP_NAME, Boolean.TRUE, Boolean.FALSE));
        }
    }

    /**
     * Closes the underlying connection.
     * 
     * @see Connection#close()
     */
    @Override
    public void close() throws IOException {
        myProxiedConnection.close();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the proxied {@link Connection}.
     * </p>
     * 
     * @see java.io.Flushable#flush()
     */
    @Override
    public void flush() throws IOException {
        try {
            myProxiedConnection.flush();
        }
        catch (final MongoDbException error) {
            onExceptin(error);
            throw error;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the proxied {@link Connection}.
     * </p>
     */
    @Override
    public long getMessagesSent() {
        return myProxiedConnection.getMessagesSent();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the proxied {@link Connection}.
     * </p>
     */
    @Override
    public int getPendingCount() {
        try {
            return myProxiedConnection.getPendingCount();
        }
        catch (final MongoDbException error) {
            onExceptin(error);
            throw error;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the proxied {@link Connection}.
     * </p>
     */
    @Override
    public long getRepliesReceived() {
        return myProxiedConnection.getRepliesReceived();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward the call to the proxied connection.
     * </p>
     */
    @Override
    public String getServerName() {
        return getProxiedConnection().getServerName();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the proxied {@link Connection}.
     * </p>
     */
    @Override
    public long getTotalLatencyNanoSeconds() {
        return myProxiedConnection.getTotalLatencyNanoSeconds();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the proxied {@link Connection}.
     * </p>
     */
    @Override
    public boolean isIdle() {
        try {
            return myProxiedConnection.isIdle();
        }
        catch (final MongoDbException error) {
            onExceptin(error);
            throw error;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the proxied {@link Connection}.
     * </p>
     */
    @Override
    public boolean isOpen() {
        try {
            return myProxiedConnection.isOpen();
        }
        catch (final MongoDbException error) {
            onExceptin(error);
            throw error;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the proxied {@link Connection}.
     * </p>
     */
    @Override
    public boolean isShuttingDown() {
        return myProxiedConnection.isShuttingDown();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the proxied {@link Connection}.
     * </p>
     */
    @Override
    public void raiseErrors(final MongoDbException exception) {
        myProxiedConnection.raiseErrors(exception);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to create a proxy connection if not already created and add
     * the listener to that connection.
     * </p>
     */
    @Override
    public void removePropertyChangeListener(
            final PropertyChangeListener listener) {
        myProxiedConnection
                .removePropertyChangeListener(new ProxiedChangeListener(this,
                        listener));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the proxied {@link Connection}.
     * </p>
     */
    @Override
    public String send(final Message message,
            final Callback<Reply> replyCallback) throws MongoDbException {
        try {
            return myProxiedConnection.send(message, replyCallback);
        }
        catch (final MongoDbException error) {
            onExceptin(error);
            throw error;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the proxied {@link Connection}.
     * </p>
     */
    @Override
    public String send(final Message message1, final Message message2,
            final Callback<Reply> replyCallback) throws MongoDbException {
        try {
            return myProxiedConnection.send(message1, message2, replyCallback);
        }
        catch (final MongoDbException error) {
            onExceptin(error);
            throw error;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the proxied {@link Connection}.
     * </p>
     */
    @Override
    public void shutdown() {
        myProxiedConnection.shutdown();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the proxied {@link Connection}.
     * </p>
     */
    @Override
    public void waitForClosed(final int timeout, final TimeUnit timeoutUnits) {
        try {
            myProxiedConnection.waitForClosed(timeout, timeoutUnits);
        }
        catch (final MongoDbException error) {
            onExceptin(error);
            throw error;
        }
    }

    /**
     * Returns the proxiedConnection value.
     * 
     * @return The proxiedConnection value.
     */
    protected Connection getProxiedConnection() {
        return myProxiedConnection;
    }

    /**
     * Provides the ability for derived classes to intercept any exceptions from
     * the underlying proxied connection.
     * <p>
     * Closes the underlying connection.
     * </p>
     * 
     * @param exception
     *            The thrown exception.
     */
    protected void onExceptin(final MongoDbException exception) {
        // Close without fear of an exception.
        IOUtils.close(this);
    }

    /**
     * ProxiedChangeListener provides a change listener to modify the source of
     * the event to the outer connection from the (inner) proxied connection.
     * 
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected static class ProxiedChangeListener implements
            PropertyChangeListener {

        /** The delegate listener. */
        private final PropertyChangeListener myDelegate;

        /** The proxied connection. */
        private final AbstractProxyConnection myProxiedConn;

        /**
         * Creates a new ProxiedChangeListener.
         * 
         * @param proxiedConn
         *            The proxied connection.
         * @param delegate
         *            The delegate listener.
         */
        public ProxiedChangeListener(final AbstractProxyConnection proxiedConn,
                final PropertyChangeListener delegate) {
            myProxiedConn = proxiedConn;
            myDelegate = delegate;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to compare the nested delegate listeners.
         * </p>
         */
        @Override
        public boolean equals(final Object object) {
            boolean result = false;
            if (this == object) {
                result = true;
            }
            else if ((object != null) && (getClass() == object.getClass())) {
                final ProxiedChangeListener other = (ProxiedChangeListener) object;

                result = myDelegate.equals(other.myDelegate);
            }
            return result;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to return the delegates hash code.
         * </p>
         */
        @Override
        public int hashCode() {
            return ((myDelegate == null) ? 13 : myDelegate.hashCode());
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to change the source of the property change event to the
         * outer connection instead of the inner connection.
         * </p>
         */
        @Override
        public void propertyChange(final PropertyChangeEvent event) {

            final PropertyChangeEvent newEvent = new PropertyChangeEvent(
                    myProxiedConn, event.getPropertyName(),
                    event.getOldValue(), event.getNewValue());
            newEvent.setPropagationId(event.getPropagationId());
            myDelegate.propertyChange(newEvent);
        }
    }
}
