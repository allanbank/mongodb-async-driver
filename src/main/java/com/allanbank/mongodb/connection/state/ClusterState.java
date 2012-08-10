/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.state;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.util.ServerNameUtils;

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

    /** The complete list of non-writable servers. */
    private final List<ServerState> myNonWritableServers;

    /** The complete list of servers. */
    private final ConcurrentMap<String, ServerState> myServers;

    /** The complete list of writable servers. */
    private final List<ServerState> myWritableServers;

    /**
     * Creates a new CLusterState.
     */
    public ClusterState() {
        myChangeSupport = new PropertyChangeSupport(this);
        myServers = new ConcurrentHashMap<String, ServerState>();
        myWritableServers = new CopyOnWriteArrayList<ServerState>();
        myNonWritableServers = new CopyOnWriteArrayList<ServerState>();
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
    public ServerState add(final String address) {
        return get(address);
    }

    /**
     * Adds a listener to the state.
     * 
     * @param listener
     *            The listener for the state changes.
     */
    public void addListener(final PropertyChangeListener listener) {
        synchronized (this) {
            myChangeSupport.addPropertyChangeListener(listener);
        }
    }

    /**
     * Returns the set of servers that can be used based on the provided
     * {@link ReadPreference}.
     * 
     * @param readPreference
     *            The {@link ReadPreference} to filter the servers.
     * @return The {@link List} of servers that can be used. Servers will be
     *         ordered by preference to be used, most preferred to least
     *         preferred.
     */
    public List<ServerState> findCandidateServers(
            final ReadPreference readPreference) {
        List<ServerState> results = Collections.emptyList();

        switch (readPreference.getMode()) {
        case NEAREST:
            results = findNearestCandidates(readPreference);
            break;
        case PRIMARY_ONLY:
            results = findWritableCandidates(readPreference);
            break;
        case PRIMARY_PREFERRED:
            results = merge(findWritableCandidates(readPreference),
                    findNonWritableCandidates(readPreference));
            break;
        case SECONDARY_ONLY:
            results = findNonWritableCandidates(readPreference);
            break;
        case SECONDARY_PREFERRED:
            results = merge(findNonWritableCandidates(readPreference),
                    findWritableCandidates(readPreference));
            break;
        case SERVER:
            results = findCandidateServer(readPreference);
            break;
        }

        return results;
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
    public ServerState get(final String address) {

        final String normalized = ServerNameUtils.normalize(address);
        ServerState state = myServers.get(normalized);
        if (state == null) {

            state = new ServerState(normalized);
            state.setWritable(false);

            synchronized (this) {
                final ServerState existing = myServers.putIfAbsent(normalized,
                        state);
                if (existing != null) {
                    state = existing;
                }
                else {
                    myNonWritableServers.add(state);
                    myChangeSupport.firePropertyChange("server", null, state);
                }
            }
        }
        return state;
    }

    /**
     * Returns a copy of the list of non-writable servers. The list returned is
     * a copy of the internal list and can be modified by the caller.
     * 
     * @return The complete list of non-writable servers.
     */
    public List<ServerState> getNonWritableServers() {
        return new ArrayList<ServerState>(myNonWritableServers);
    }

    /**
     * Returns a copy of the list of servers. The list returned is a copy of the
     * internal list and can be modified by the caller.
     * 
     * @return The complete list of servers.
     */
    public List<ServerState> getServers() {
        return new ArrayList<ServerState>(myServers.values());
    }

    /**
     * Returns a copy of the list of writable servers. The list returned is a
     * copy of the internal list and can be modified by the caller.
     * 
     * @return The complete list of writable servers.
     */
    public List<ServerState> getWritableServers() {
        return new ArrayList<ServerState>(myWritableServers);
    }

    /**
     * Marks the server as non-writable. Fires a {@link PropertyChangeEvent} if
     * the server was previously writable.
     * 
     * @param server
     *            The server to mark non-writable.
     */
    public void markNotWritable(final ServerState server) {
        synchronized (this) {
            if (server.setWritable(false)) {
                myWritableServers.remove(server);
                myNonWritableServers.add(server);
                myChangeSupport.firePropertyChange("writable", true, false);
            }
        }
    }

    /**
     * Marks the server as writable. Fires a {@link PropertyChangeEvent} if the
     * server was previously non-writable.
     * 
     * @param server
     *            The server to mark writable.
     */
    public void markWritable(final ServerState server) {
        synchronized (this) {
            if (!server.setWritable(true)) {
                myNonWritableServers.remove(server);
                myWritableServers.add(server);
                myChangeSupport.firePropertyChange("writable", false, true);
            }
        }
    }

    /**
     * Removes a listener to the state.
     * 
     * @param listener
     *            The listener for the state changes.
     */
    public void removeListener(final PropertyChangeListener listener) {
        synchronized (this) {
            myChangeSupport.removePropertyChangeListener(listener);
        }
    }

    /**
     * Computes a relative CDF (cumulative distribution function) for the
     * servers based on the latency from the client.
     * <p>
     * The latency of each server is used to create a strict ordering of servers
     * from lowest latency to highest. The relative latency of the i'th server
     * is then calculated based on the function: <blockquote>
     * 
     * <pre>
     *                                       latency[0]
     *                relative_latency[i] =  ----------
     *                                       latency[i]
     * </pre>
     * 
     * </blockquote>
     * </p>
     * <p>
     * The relative latencies are then then summed and the probability of
     * selecting each server is then calculated by:<blockquote>
     * 
     * <pre>
     *                                  relative_latency[i]
     *     probability[i] = -------------------------------------------------
     *                      sum(relative_latency[0], ... relative_latency[n])
     * </pre>
     * 
     * </blockquote>
     * </p>
     * <p>
     * The CDF over these probabilities is returned.
     * </p>
     * 
     * @param servers
     *            The servers to compute the CDF for.
     * @return The CDF for the server latencies.
     */
    protected final double[] cdf(final List<ServerState> servers) {
        Collections.sort(servers, ServerLatencyComparator.COMPARATOR);

        // Pick a server to move to the front.
        final double[] relativeLatency = new double[servers.size()];
        double sum = 0;
        double first = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < relativeLatency.length; ++i) {
            final ServerState server = servers.get(i);
            double latency = server.getAverageLatency();

            // Turn the latency into a ratio of the lowest latency.
            if (first == Double.NEGATIVE_INFINITY) {
                first = latency;
                latency = 1.0D; // By definition N/N = 1.0.
            }
            else {
                latency /= first;
            }

            relativeLatency[i] = latency;
            sum += latency;
        }

        // Turn the latencies into a range of 0 <= relativeLatency < 1.
        // Also known as the CDF (cumulative distribution function)
        double accum = 0.0D;
        for (int i = 0; i < relativeLatency.length; ++i) {
            accum += relativeLatency[i];

            relativeLatency[i] = accum / sum;
        }

        return relativeLatency;
    }

    /**
     * Finds the candidate server, if known.
     * 
     * @param readPreference
     *            The read preference to match the server against.
     * @return The Server found in a singleton list or an empty list if the
     *         server is not known.
     */
    protected List<ServerState> findCandidateServer(
            final ReadPreference readPreference) {
        final ServerState server = myServers.get(ServerNameUtils
                .normalize(readPreference.getServer()));
        if ((server != null) && readPreference.matches(server.getTags())) {
            return Collections.singletonList(server);
        }
        return Collections.emptyList();
    }

    /**
     * Returns the list of servers that match the read preference's tags.
     * 
     * @param readPreference
     *            The read preference to match the server against.
     * @return The servers found in order of preference. Generally this is in
     *         latency order but we randomly move one of the servers to the
     *         front of the list to distribute the load across more servers.
     * 
     * @see #sort
     */
    protected List<ServerState> findNearestCandidates(
            final ReadPreference readPreference) {
        final List<ServerState> results = new ArrayList<ServerState>(
                myServers.size());
        for (final ServerState server : myServers.values()) {
            if (readPreference.matches(server.getTags())) {
                results.add(server);
            }
        }

        // Sort the server by preference.
        sort(results);

        return results;
    }

    /**
     * Returns the list of non-writable servers that match the read preference's
     * tags.
     * 
     * @param readPreference
     *            The read preference to match the server against.
     * @return The servers found in order of preference. Generally this is in
     *         latency order but we randomly move one of the servers to the
     *         front of the list to distribute the load across more servers.
     * 
     * @see #sort
     */
    protected List<ServerState> findNonWritableCandidates(
            final ReadPreference readPreference) {
        final List<ServerState> results = new ArrayList<ServerState>(
                myNonWritableServers.size());
        for (final ServerState server : myNonWritableServers) {
            if (readPreference.matches(server.getTags())) {
                results.add(server);
            }
        }

        // Sort the server by preference.
        sort(results);

        return results;
    }

    /**
     * Returns the list of writable servers that match the read preference's
     * tags.
     * 
     * @param readPreference
     *            The read preference to match the server against.
     * @return The servers found in order of preference. Generally this is in
     *         latency order but we randomly move one of the servers to the
     *         front of the list to distribute the load across more servers.
     * 
     * @see #sort
     */
    protected List<ServerState> findWritableCandidates(
            final ReadPreference readPreference) {
        final List<ServerState> results = new ArrayList<ServerState>(
                myWritableServers.size());
        for (final ServerState server : myWritableServers) {
            if (readPreference.matches(server.getTags())) {
                results.add(server);
            }
        }

        // Sort the server by preference.
        sort(results);

        return results;
    }

    /**
     * Sorts the servers based on the latency from the client.
     * <p>
     * To distribute the requests across servers more evenly the first server is
     * replaced with a random server based on a single sided simplified Gaussian
     * distribution.
     * </p>
     * 
     * @param servers
     *            The servers to be sorted.
     * 
     * @see #cdf(List)
     */
    protected final void sort(final List<ServerState> servers) {
        if (servers.isEmpty() || (servers.size() == 1)) {
            return;
        }

        // Pick a server to move to the front.
        final double[] cdf = cdf(servers);
        final double random = Math.random();
        int index = Arrays.binarySearch(cdf, random);

        // Probably a negative index since not expecting an exact match.
        if (index < 0) {
            // Undo (-(insertion point) - 1)
            index = Math.abs(index + 1);
        }

        // Should not be needed. random should be < 1.0 and
        // relativeLatency[relativeLatency.length] == 1.0
        //
        // assert (random < 1.0D) :
        // "The random value should be strictly less than 1.0.";
        // assert (cdf[cdf.length - 1] <= 1.0001) :
        // "The cdf of the last server should be 1.0.";
        // assert (0.9999 <= cdf[cdf.length - 1]) :
        // "The cdf of the last server should be 1.0.";
        index = Math.min(cdf.length - 1, index);

        // Swap the lucky winner into the first position.
        Collections.swap(servers, 0, index);
    }

    /**
     * Merges the two lists into a single list.
     * 
     * @param list1
     *            The first list of servers.
     * @param list2
     *            The second list of servers.
     * @return The 2 lists of servers merged into a single list.
     */
    private final List<ServerState> merge(final List<ServerState> list1,
            final List<ServerState> list2) {
        List<ServerState> results;
        if (list1.isEmpty()) {
            results = list2;
        }
        else if (list2.isEmpty()) {
            results = list1;
        }
        else {
            results = new ArrayList<ServerState>(list1.size() + list2.size());
            results.addAll(list1);
            results.addAll(list2);
        }
        return results;
    }

}
