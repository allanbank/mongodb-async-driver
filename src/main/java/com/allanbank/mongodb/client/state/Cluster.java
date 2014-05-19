/*
 * Copyright 2011-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */
package com.allanbank.mongodb.client.state;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.client.ClusterStats;
import com.allanbank.mongodb.client.VersionRange;
import com.allanbank.mongodb.util.ServerNameUtils;

/**
 * {@link Cluster} tracks the state of the cluster of MongoDB servers.
 * PropertyChangeEvents are fired when a server is added or marked writable/not
 * writable.
 * <p>
 * This class uses brute force synchronization to protect its internal state. It
 * is assumed that multiple connections will be concurrently updating the
 * {@link Cluster} at once and that at any given time this class may not contain
 * the absolute truth about the state of the cluster. Instead connections should
 * keep querying for the state of the cluster via their connection until the
 * view the server returned and the {@link Cluster} are consistent. Since this
 * class will not fire a {@link PropertyChangeEvent} when the state is not truly
 * modified the simplest mechanism is to keep querying for the cluster state on
 * the connection until no addition change events are seen.
 * </p>
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Cluster implements ClusterStats {

    /** The property sued for adding a new server. */
    public static final String SERVER_PROP = "server";

    /** The property name for if there is a writable server. */
    public static final String WRITABLE_PROP = "writable";

    /** The complete list of servers. */
    protected final ConcurrentMap<String, Server> myServers;

    /** The range of versions within the cluster. */
    protected VersionRange myServerVersionRange;

    /** The smallest maximum number of operations in a batch in the cluster. */
    protected int mySmallestMaxBatchedWriteOperations;

    /** The smallest maximum document size in the cluster. */
    protected long mySmallestMaxBsonObjectSize;

    /** Support for firing property change events. */
    /* package */final PropertyChangeSupport myChangeSupport;

    /** The listener for changes to the server. */
    /* package */final ServerListener myListener;

    /** The complete list of non-writable servers. */
    /* package */final CopyOnWriteArrayList<Server> myNonWritableServers;

    /** The complete list of writable servers. */
    /* package */final CopyOnWriteArrayList<Server> myWritableServers;

    /** The configuration for connecting to the servers. */
    private final MongoClientConfiguration myConfig;

    /**
     * Creates a new CLusterState.
     * 
     * @param config
     *            The configuration for the cluster.
     */
    public Cluster(final MongoClientConfiguration config) {
        myConfig = config;
        myChangeSupport = new PropertyChangeSupport(this);
        myServers = new ConcurrentHashMap<String, Server>();
        myWritableServers = new CopyOnWriteArrayList<Server>();
        myNonWritableServers = new CopyOnWriteArrayList<Server>();
        myListener = new ServerListener();
        myServerVersionRange = VersionRange.range(Version.parse("0"),
                Version.parse("0"));
    }

    /**
     * Adds a {@link Server} to the {@link Cluster} for the address provided if
     * one does not already exist.
     * 
     * @param address
     *            The address of the {@link Server} to return.
     * @return The {@link Server} for the address.
     */
    public Server add(final InetSocketAddress address) {
        final String normalized = ServerNameUtils.normalize(address);
        Server server = myServers.get(normalized);
        if (server == null) {

            server = new Server(address);

            synchronized (this) {
                final Server existing = myServers.putIfAbsent(normalized,
                        server);
                if (existing != null) {
                    server = existing;
                }
                else {
                    myNonWritableServers.add(server);
                    myChangeSupport.firePropertyChange(SERVER_PROP, null,
                            server);

                    server.addListener(myListener);
                }
            }
        }
        return server;
    }

    /**
     * Adds a {@link Server} to the {@link Cluster} for the address provided if
     * one does not already exist.
     * <p>
     * This method is equivalent to calling {@link #add(InetSocketAddress)
     * add(ServerNameUtils.parse(address))}.
     * </p>
     * 
     * @param address
     *            The address of the {@link Server} to return.
     * @return The {@link Server} for the address.
     */
    public Server add(final String address) {
        Server server = myServers.get(address);
        if (server == null) {
            server = add(ServerNameUtils.parse(address));
        }

        return server;
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
     * Removes all of the servers from the cluster.
     */
    public void clear() {
        for (final Server server : myServers.values()) {
            remove(server);
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
    public List<Server> findCandidateServers(final ReadPreference readPreference) {
        List<Server> results = Collections.emptyList();

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
     * Returns the server state for the address provided. If the {@link Server}
     * does not already exist a non-writable state is created and returned.
     * <p>
     * This method is equivalent to calling {@link #add(String) add(address)}.
     * </p>
     * 
     * @param address
     *            The address of the {@link Server} to return.
     * @return The {@link Server} for the address.
     */
    public Server get(final String address) {
        return add(address);
    }

    /**
     * Returns a copy of the list of non-writable servers. The list returned is
     * a copy of the internal list and can be modified by the caller.
     * 
     * @return The complete list of non-writable servers.
     */
    public List<Server> getNonWritableServers() {
        return new ArrayList<Server>(myNonWritableServers);
    }

    /**
     * Returns a copy of the list of servers. The list returned is a copy of the
     * internal list and can be modified by the caller.
     * 
     * @return The complete list of servers.
     */
    public List<Server> getServers() {
        return new ArrayList<Server>(myServers.values());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VersionRange getServerVersionRange() {
        return myServerVersionRange;
    }

    /**
     * Returns smallest value for the maximum number of write operations allowed
     * in a single write command.
     * 
     * @return The smallest value for maximum number of write operations allowed
     *         in a single write command.
     */
    @Override
    public int getSmallestMaxBatchedWriteOperations() {
        return mySmallestMaxBatchedWriteOperations;
    }

    /**
     * Returns the smallest value for the maximum BSON object size within the
     * cluster.
     * 
     * @return The smallest value for the maximum BSON object size within the
     *         cluster.
     */
    @Override
    public long getSmallestMaxBsonObjectSize() {
        return mySmallestMaxBsonObjectSize;
    }

    /**
     * Returns a copy of the list of writable servers. The list returned is a
     * copy of the internal list and can be modified by the caller.
     * 
     * @return The complete list of writable servers.
     */
    public List<Server> getWritableServers() {
        return new ArrayList<Server>(myWritableServers);
    }

    /**
     * Removes the specified server from the cluster.
     * 
     * @param server
     *            The server to remove from the cluster.
     */
    public void remove(final Server server) {

        final Server removed = myServers.remove(server.getCanonicalName());
        if (removed != null) {
            removed.removeListener(myListener);
            myNonWritableServers.remove(removed);
            myWritableServers.remove(removed);

            updateVersions();
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
     * is then calculated based on the function:
     * </p>
     * <blockquote>
     * 
     * <pre>
     *                                       latency[0]
     *                relative_latency[i] =  ----------
     *                                       latency[i]
     * </pre>
     * 
     * </blockquote>
     * <p>
     * The relative latencies are then then summed and the probability of
     * selecting each server is then calculated by:
     * </p>
     * <blockquote>
     * 
     * <pre>
     *                                  relative_latency[i]
     *     probability[i] = -------------------------------------------------
     *                      sum(relative_latency[0], ... relative_latency[n])
     * </pre>
     * 
     * </blockquote>
     * 
     * <p>
     * The CDF over these probabilities is returned.
     * </p>
     * 
     * @param servers
     *            The servers to compute the CDF for.
     * @return The CDF for the server latencies.
     */
    protected final double[] cdf(final List<Server> servers) {
        Collections.sort(servers, ServerLatencyComparator.COMPARATOR);

        // Pick a server to move to the front.
        final double[] relativeLatency = new double[servers.size()];
        double sum = 0;
        double first = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < relativeLatency.length; ++i) {
            final Server server = servers.get(i);
            double latency = server.getAverageLatency();

            // Turn the latency into a ratio of the lowest latency.
            if (first == Double.NEGATIVE_INFINITY) {
                first = latency;
                latency = 1.0D; // By definition N/N = 1.0.
            }
            else {
                latency /= first;
            }

            latency = (1.0D / latency); // 4 times as long is 1/4 as likely.
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
    protected List<Server> findCandidateServer(
            final ReadPreference readPreference) {
        final Server server = myServers.get(readPreference.getServer());
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
    protected List<Server> findNearestCandidates(
            final ReadPreference readPreference) {
        final List<Server> results = new ArrayList<Server>(myServers.size());
        for (final Server server : myServers.values()) {
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
    protected List<Server> findNonWritableCandidates(
            final ReadPreference readPreference) {
        final List<Server> results = new ArrayList<Server>(
                myNonWritableServers.size());
        for (final Server server : myNonWritableServers) {
            if (readPreference.matches(server.getTags())
                    && isRecentEnough(server.getSecondsBehind())) {
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
    protected List<Server> findWritableCandidates(
            final ReadPreference readPreference) {
        final List<Server> results = new ArrayList<Server>(
                myWritableServers.size());
        for (final Server server : myWritableServers) {
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
    protected final void sort(final List<Server> servers) {
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
     * Updates the min/max versions across all servers. Since the max BSON
     * object size is tied to the version we also update that value.
     */
    protected void updateVersions() {
        Version min = null;
        Version max = null;

        long smallestMaxBsonObjectSize = Long.MAX_VALUE;
        int smallestMaxBatchedWriteOperations = Integer.MAX_VALUE;

        for (final Server server : myServers.values()) {
            min = Version.earlier(min, server.getVersion());
            max = Version.later(max, server.getVersion());

            smallestMaxBsonObjectSize = Math.min(smallestMaxBsonObjectSize,
                    server.getMaxBsonObjectSize());
            smallestMaxBatchedWriteOperations = Math.min(
                    smallestMaxBatchedWriteOperations,
                    server.getMaxBatchedWriteOperations());
        }

        myServerVersionRange = VersionRange.range(min, max);
        mySmallestMaxBsonObjectSize = smallestMaxBsonObjectSize;
        mySmallestMaxBatchedWriteOperations = smallestMaxBatchedWriteOperations;
    }

    /**
     * Returns true if the server is recent enough to be queried.
     * 
     * @param secondsBehind
     *            The number of seconds the server is behind.
     * @return True if the server is recent enough to be queried, false
     *         otherwise.
     */
    private boolean isRecentEnough(final double secondsBehind) {
        return ((secondsBehind * 1000) < myConfig.getMaxSecondaryLag());
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
    private final List<Server> merge(final List<Server> list1,
            final List<Server> list2) {
        List<Server> results;
        if (list1.isEmpty()) {
            results = list2;
        }
        else if (list2.isEmpty()) {
            results = list1;
        }
        else {
            results = new ArrayList<Server>(list1.size() + list2.size());
            results.addAll(list1);
            results.addAll(list2);
        }
        return results;
    }

    /**
     * ServerListener provides a listener for the state updates of the
     * {@link Server}.
     * 
     * @api.no This class is <b>NOT</b> part of the drivers API. This class may
     *         be mutated in incompatible ways between any two releases of the
     *         driver.
     * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected final class ServerListener implements PropertyChangeListener {
        @Override
        public void propertyChange(final PropertyChangeEvent evt) {
            final String propertyName = evt.getPropertyName();
            final Server server = (Server) evt.getSource();

            if (Server.STATE_PROP.equals(propertyName)) {

                final boolean old = !myWritableServers.isEmpty();

                if (Server.State.WRITABLE == evt.getNewValue()) {
                    myWritableServers.addIfAbsent(server);
                    myNonWritableServers.remove(server);
                }
                else if (Server.State.READ_ONLY == evt.getNewValue()) {
                    myWritableServers.remove(server);
                    myNonWritableServers.addIfAbsent(server);
                }
                else {
                    myWritableServers.remove(server);
                    myNonWritableServers.remove(server);
                }

                myChangeSupport.firePropertyChange(WRITABLE_PROP, old,
                        !myWritableServers.isEmpty());

            }
            else if (Server.CANONICAL_NAME_PROP.equals(propertyName)) {
                // Resolved a new canonical name. e.g., What the server
                // calls itself in the cluster.

                // Remove the entry with the old name.
                myServers.remove(evt.getOldValue(), server);

                // And add with the new name. Checking for duplicate entries.
                final Server existing = myServers.putIfAbsent(
                        server.getCanonicalName(), server);
                if (existing != null) {
                    // Already have a Server with that name. Remove the listener
                    // and let this server get garbage collected.
                    myNonWritableServers.remove(server);
                    myWritableServers.remove(server);
                    server.removeListener(myListener);

                    myChangeSupport.firePropertyChange(SERVER_PROP, server,
                            null);
                }
            }
            else if (Server.VERSION_PROP.equals(propertyName)) {
                // If the old version is either the high or low for the cluster
                // (or the version is UNKNOWN) then recompute the high/low
                // versions.
                final Version old = (Version) evt.getOldValue();

                if (Version.UNKNOWN.equals(old)
                        || (myServerVersionRange.getUpperBounds()
                                .compareTo(old) <= 0)
                        || (myServerVersionRange.getLowerBounds()
                                .compareTo(old) >= 0)) {
                    updateVersions();
                }
            }
        }
    }
}
