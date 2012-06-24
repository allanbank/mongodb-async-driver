/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.proxy;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.message.PendingMessage;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.state.ClusterState;

/**
 * A helper class for constructing connections that are really just proxies on
 * top of other connections.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractProxyConnection implements Connection {

    /** The state of the cluster. */
    protected final ClusterState myClusterState;

    /** The MongoDB client configuration. */
    protected final MongoDbConfiguration myConfig;

    /** The factory to create proxied connections. */
    protected final ProxiedConnectionFactory myConnectionFactory;

    /** The proxied connection. */
    private final Connection myProxiedConnection;

    /**
     * Creates a AbstractProxyConnection.
     * 
     * @param proxiedConnection
     *            The connection to forward to.
     * @param factory
     *            The factory to create proxied connections.
     * @param clusterState
     *            The state of the cluster.
     * @param config
     *            The MongoDB client configuration.
     */
    public AbstractProxyConnection(final Connection proxiedConnection,
            final ProxiedConnectionFactory factory,
            final ClusterState clusterState, final MongoDbConfiguration config) {
        myProxiedConnection = proxiedConnection;
        myConnectionFactory = factory;
        myClusterState = clusterState;
        myConfig = config;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward to the wrapped connection.
     * </p>
     */
    @Override
    public void addPending(final List<PendingMessage> pending) {
        try {
            myProxiedConnection.addPending(pending);
        }
        catch (final MongoDbException error) {
            onExceptin(error);
            throw error;
        }
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
                    .addPropertyChangeListener(new ProxiedChangeListener(
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
     * Overridden to forward to the wrapped connection.
     * </p>
     */
    @Override
    public void drainPending(final List<PendingMessage> pending) {
        try {
            myProxiedConnection.drainPending(pending);
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
    public void raiseErrors(final MongoDbException exception,
            final boolean notifyToBeSent) {
        myProxiedConnection.raiseErrors(exception, notifyToBeSent);
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
                .removePropertyChangeListener(new ProxiedChangeListener(
                        listener));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the proxied {@link Connection}.
     * </p>
     */
    @Override
    public void send(final Callback<Reply> reply, final Message... messages)
            throws MongoDbException {
        try {
            myProxiedConnection.send(reply, messages);
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
    public void send(final Message... messages) throws MongoDbException {
        send(null, messages);
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
        try {
            close();
        }
        catch (final IOException e) {
            // ignore.
        }
    }

    /**
     * ProxiedChangeListener provides a change listener to modify the source of
     * the event to the outer connection from the (inner) proxied connection.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected class ProxiedChangeListener implements PropertyChangeListener {

        /** The delegate listener. */
        private final PropertyChangeListener myDelegate;

        /**
         * Creates a new ProxiedChangeListener.
         * 
         * @param delegate
         *            The delegate listener.
         */
        public ProxiedChangeListener(final PropertyChangeListener delegate) {
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
                    AbstractProxyConnection.this, event.getPropertyName(),
                    event.getOldValue(), event.getNewValue());
            newEvent.setPropagationId(event.getPropagationId());
            myDelegate.propertyChange(newEvent);
        }
    }
}
