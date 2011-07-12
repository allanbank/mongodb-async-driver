/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.state;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link ClusterState} tracks the state of the cluster of MongoDB servers.
 * PropertyChangeEvents are fired when a server is added or marked writable/not
 * writable.
 * <p>
 * This class uses brute force synchronization to protect its internal state. It
 * is assumed that multiple connections will be concurrently updating the
 * {@link ClusterState} at once and that at any given time this class may not
 * contain the absolute truth about the state of the cluster. Instead
 * connections should keep querying for the state of the cluster via their
 * connection until the view the server returned and the {@link ClusterState}
 * are consistent. Since this class will not fire a {@link PropertyChangeEvent}
 * when the state is not truly modified the simplest mechanism is to keep
 * querying for the cluster state on the connection until no addition change
 * events are seen.
 * </p>
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ClusterState {

	/** Support for firing property change events. */
	private final PropertyChangeSupport myChangeSupport;

	/** The complete list of servers. */
	private final Map<String, ServerState> myServers;

	/** The complete list of writable servers. */
	private final List<ServerState> myWritableServers;

	/** The complete list of non-writable servers. */
	private final List<ServerState> myNonWritableServers;

	/**
	 * Creates a new CLusterState.
	 */
	public ClusterState() {
		myChangeSupport = new PropertyChangeSupport(this);
		myServers = new HashMap<String, ServerState>();
		myWritableServers = new ArrayList<ServerState>();
		myNonWritableServers = new ArrayList<ServerState>();
	}

	/**
	 * Adds a {@link ServerState} to the {@link ClusterState} for the address
	 * provided if one does not already exist.
	 * <p>
	 * This method is equivalent to calling {@link #get(String)}.
	 * </p>
	 * 
	 * @param address
	 *            The address of the {@link ServerState} to return.
	 * @return The {@link ServerState} for the address.
	 */
	public ServerState add(String address) {
		return get(address);
	}

	/**
	 * Adds a listener to the state.
	 * 
	 * @param listener
	 *            The listener for the state changes.
	 */
	public synchronized void addListener(PropertyChangeListener listener) {
		myChangeSupport.addPropertyChangeListener(listener);
	}

	/**
	 * Removes a listener to the state.
	 * 
	 * @param listener
	 *            The listener for the state changes.
	 */
	public synchronized void removeListener(PropertyChangeListener listener) {
		myChangeSupport.removePropertyChangeListener(listener);
	}

	/**
	 * Returns the server state for the address provided. If the
	 * {@link ServerState} does not already exist a non-writable state is
	 * created and returned.
	 * 
	 * @param address
	 *            The address of the {@link ServerState} to return.
	 * @return The {@link ServerState} for the address.
	 */
	public synchronized ServerState get(String address) {

		ServerState state = myServers.get(address);
		if (state == null) {
			state = new ServerState(address);
			state.setWritable(false);

			myServers.put(address, state);
			myNonWritableServers.add(state);

			myChangeSupport.firePropertyChange("server", null, state);
		}
		return state;
	}

	/**
	 * Marks the server as writable. Fires a {@link PropertyChangeEvent} if the
	 * server was previously non-writable.
	 * 
	 * @param server
	 *            The server to mark writable.
	 */
	public synchronized void markWritable(ServerState server) {
		if (!server.isWritable()) {
			server.setWritable(true);

			myNonWritableServers.remove(server);
			myWritableServers.add(server);
			myChangeSupport.firePropertyChange("writable", false, true);
		}
	}

	/**
	 * Marks the server as non-writable. Fires a {@link PropertyChangeEvent} if
	 * the server was previously writable.
	 * 
	 * @param server
	 *            The server to mark non-writable.
	 */
	public synchronized void markNotWritable(ServerState server) {
		if (server.isWritable()) {
			server.setWritable(false);

			myWritableServers.remove(server);
			myNonWritableServers.add(server);
			myChangeSupport.firePropertyChange("writable", true, false);
		}
	}

	/**
	 * Returns a copy of the list of servers. The list returned is a copy of the
	 * internal list and can be modified by the caller.
	 * 
	 * @return The complete list of servers.
	 */
	public synchronized List<ServerState> getServers() {
		return new ArrayList<ServerState>(myServers.values());
	}

	/**
	 * Returns a copy of the list of writable servers. The list returned is a
	 * copy of the internal list and can be modified by the caller.
	 * 
	 * @return The complete list of writable servers.
	 */
	public synchronized List<ServerState> getWritableServers() {
		return new ArrayList<ServerState>(myWritableServers);
	}

	/**
	 * Returns a copy of the list of non-writable servers. The list returned is
	 * a copy of the internal list and can be modified by the caller.
	 * 
	 * @return The complete list of non-writable servers.
	 */
	public synchronized List<ServerState> getNonWritableServers() {
		return new ArrayList<ServerState>(myNonWritableServers);
	}
}
