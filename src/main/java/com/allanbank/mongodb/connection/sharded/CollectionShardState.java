/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.sharded;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.builder.ComparisonOperator;
import com.allanbank.mongodb.builder.LogicalOperator;
import com.allanbank.mongodb.builder.MiscellaneousOperator;

/**
 * CollectionShardState provides a mapping from an element of the shard key to
 * the shard that contains the data.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CollectionShardState {

    /**
     * The current mapping from an element to a shard.
     * <p>
     * The key in the map is the maximum value in the range since
     * {@link SortedMap#headMap(Object)} is those keys strictly less (not
     * inclusive) which is identical to the condition for the MongoDB ranges.
     * </p>
     */
    private final AtomicReference<SortedMap<Element, Shard>> myMapping;

    /**
     * The name of the shard key for the collection. Compound shard keys are not
     * supported, yet.
     */
    private final String myShardKey;

    /**
     * Creates a new CollectionShardState.
     * 
     * @param shardKey
     *            The name of the shard key for the collection. Compound shard
     *            keys are not supported, yet.
     */
    public CollectionShardState(final String shardKey) {
        myShardKey = shardKey;
        myMapping = new AtomicReference<SortedMap<Element, Shard>>(
                new TreeMap<Element, Shard>());
    }

    /**
     * Locates the shard that should receive the request based on the element
     * value provided.
     * 
     * @param queryOrDocument
     *            The being used or the document being inserted. Used to
     *            determine the shard to use based on the shard mapping.
     * @return The shard to send the request to. If no shard can be identified
     *         then this method returns <code>null</code>.
     */
    public Shard findShard(final Document queryOrDocument) {

        // Copy the mapping for consistent results.
        final SortedMap<Element, Shard> chunkMapping = myMapping.get();
        Shard result = null;

        // Find the keyElement.
        Element keyElement = queryOrDocument.findFirst(myShardKey);
        if (keyElement == null) {
            // Look under an $and. If a single field then still ok.
            final List<Element> keyElements = queryOrDocument.find(
                    LogicalOperator.AND.getToken(), ".*", myShardKey);

            if (keyElements.size() == 1) {
                keyElement = keyElements.get(0);
            }
        }

        if (keyElement != null) {
            // Two cases:
            // 1) Simple element (non-doc or doc without $ operators).
            // 2) Doc with a set of operators that uses a single shard.

            if (keyElement.getType() == ElementType.DOCUMENT) {
                result = findShardForDocument(chunkMapping,
                        (DocumentElement) keyElement);
            }
            else {
                result = findShardForElement(chunkMapping, keyElement);
            }

        }
        return result;
    }

    /**
     * Returns the current chunk mapping to shards. Each entry contains the key
     * for the exclusive end of the chunk range.
     * 
     * @return The chunks mapping. Each entry contains the key for the exclusive
     *         end of the chunk range.
     */
    public SortedMap<Element, Shard> getMapping() {
        return myMapping.get();
    }

    /**
     * Sets the current chunk mapping to shards. Each entry contains the key for
     * the exclusive end of the chunk range.
     * 
     * @param mapping
     *            The chunks mapping. Each entry contains the key for the
     *            exclusive end of the chunk range.
     */
    public void setMapping(final SortedMap<Element, Shard> mapping) {
        myMapping.set(Collections.unmodifiableSortedMap(mapping));
    }

    /**
     * Returns a subset of the {@code chunks} map that contain the values
     * greater than the specified {@code key}.
     * <p>
     * Find the all chunks that may contain an entry that are strictly greater
     * than the key.
     * </p>
     * <p>
     * This is equivalent to the {@link SortedMap#tailMap(Object)} except we
     * don't want the first entry if it equals the specified key.
     * </p>
     * 
     * @param chunks
     *            The {@code chunks} mapping. Each entry contains the key for
     *            the exclusive end of the chunk range.
     * @param key
     *            The {@code key} to return all {@code chunks} with values
     *            greater than.
     * @return The subset of {@code chunks} with values greater than the
     *         {@code key}.
     */
    protected SortedMap<Element, Shard> filterForGreaterThan(
            final SortedMap<Element, Shard> chunks, final Element key) {

        SortedMap<Element, Shard> greaterThan = chunks;

        // tailMap() is inclusive.
        // Only an issue if the first key is equal to the key (in which case it
        // should be excluded since that shard does not contain any data of
        // interest).
        final SortedMap<Element, Shard> tail = chunks.tailMap(key);
        if (!tail.isEmpty()) {
            if (key.equals(tail.firstKey())) {
                // Need the next tail map.
                greaterThan = chunks.tailMap(secondKey(tail, tail.firstKey()));
            }
            else {
                greaterThan = tail;
            }
        }

        return greaterThan;
    }

    /**
     * Returns a subset of the {@code chunks} map that contain the values
     * greater than or equal to the the specified {@code key}.
     * <p>
     * Find the all chunks that may contain an entry that are greater than the
     * key.
     * </p>
     * <p>
     * This is equivalent to the {@link SortedMap#tailMap(Object)} except we
     * don't want the first key if it equals the specified key since that is the
     * exclusive end of the range (e.g., the key is in the next range).
     * </p>
     * <p>
     * This turns out to be exactly the same as the
     * {@link #filterForGreaterThan(SortedMap, Element)} logic so we delegate to
     * that method.
     * </p>
     * 
     * @param chunks
     *            The {@code chunks} mapping. Each entry contains the key for
     *            the exclusive end of the chunk range.
     * @param key
     *            The {@code key} to return all {@code chunks} with values
     *            greater than or equal to.
     * @return The subset of {@code chunks} with values greater than or equal to
     *         the {@code key}.
     */
    protected SortedMap<Element, Shard> filterForGreaterThanOrEqual(
            final SortedMap<Element, Shard> chunks, final Element key) {
        return filterForGreaterThan(chunks, key);
    }

    /**
     * Returns a subset of the {@code chunks} map that contain the values in the
     * the specified {@code key}.
     * 
     * @param fieldName
     *            The name of the key field.
     * @param chunks
     *            The {@code chunks} mapping. Each entry contains the key for
     *            the exclusive end of the chunk range.
     * @param key
     *            The {@code key} to return all {@code chunks} with values it
     *            contains.
     * @return The subset of {@code chunks} with values in the {@code key}.
     */
    protected SortedMap<Element, Shard> filterForIn(final String fieldName,
            final SortedMap<Element, Shard> chunks, final Element key) {
        final SortedMap<Element, Shard> in = new TreeMap<Element, Shard>();

        if (key.getType() == ElementType.ARRAY) {
            // Build a sub map of just the entries in the "in" array.
            final ArrayElement values = (ArrayElement) key;
            for (final Element value : values.getEntries()) {

                final Element shardKey = findKeyFor(chunks,
                        value.withName(fieldName));
                if (shardKey != null) {
                    in.put(key, chunks.get(shardKey));
                }

            }
        }
        else {
            // Assume the value is a single entry.
            final Element shardKey = findKeyFor(chunks, key);
            if (shardKey != null) {
                in.put(key, chunks.get(shardKey));
            }
        }

        return in;
    }

    /**
     * Returns a subset of the {@code chunks} map that contain the values less
     * than the specified {@code key}.
     * <p>
     * Find the all chunks that may contain an entry that are less than the key.
     * </p>
     * <p>
     * This is equivalent to the {@link SortedMap#headMap(Object)} plus one
     * entry past the head map. We find the right entry for the head map using
     * the {@link SortedMap#tailMap(Object)}, iterator one past the first key
     * and then get the {@code headMap(...)} with that entry's key.
     * </p>
     * 
     * @param chunks
     *            The {@code chunks} mapping. Each entry contains the key for
     *            the exclusive end of the chunk range.
     * @param key
     *            The {@code key} to return all {@code chunks} with values less
     *            than.
     * @return The subset of {@code chunks} with values less than the
     *         {@code key}.
     */
    protected SortedMap<Element, Shard> filterForLessThan(
            final SortedMap<Element, Shard> chunks, final Element key) {
        Element headKey = chunks.lastKey();
        final SortedMap<Element, Shard> tail = chunks.tailMap(key);

        // Move to the next chunk for the non-inclusive headMap.
        headKey = secondKey(tail, headKey);

        return chunks.headMap(headKey);
    }

    /**
     * Returns a subset of the {@code chunks} map that contain the values less
     * than or equal to the the specified {@code key}.
     * 
     * @param chunks
     *            The {@code chunks} mapping. Each entry contains the key for
     *            the exclusive end of the chunk range.
     * @param key
     *            The {@code key} to return all {@code chunks} with values less
     *            than or equal to.
     * @return The subset of {@code chunks} with values less than or equal to
     *         the {@code key}.
     */
    protected SortedMap<Element, Shard> filterForLessThanOrEqual(
            final SortedMap<Element, Shard> chunks, final Element key) {

        Element headKey = chunks.lastKey();
        final SortedMap<Element, Shard> tail = chunks.tailMap(key);
        if (!tail.isEmpty()) {
            if (key.equals(tail.firstKey())) {
                // Iterate 2 past (third key). key is in the next chunk and we
                // need the key past that for the headMap() since it is
                // non-inclusive.
                headKey = thirdKey(tail, headKey);
            }
            else {
                // Only iterate 1 past.
                headKey = secondKey(tail, headKey);
            }
        }

        return chunks.headMap(headKey);
    }

    /**
     * Finds the single shard that the document query will use or null if we
     * cannot determine a single shard for the shard key's query document.
     * 
     * @param chunkMapping
     *            The mapping for chunk ranges. Each entry contains the key for
     *            the exclusive end of the chunk range.
     * @param keyDocument
     *            The shard key's query document.
     * @return The shard holding all of the possibly matching documents or null
     *         if we cannot determine a single shard for the shard key's query
     *         document.
     */
    protected Shard findShardForDocument(
            final SortedMap<Element, Shard> chunkMapping,
            final DocumentElement keyDocument) {

        Shard result = null;

        // Look for "$" operators.
        if (keyDocument.findFirst("^$.*") == null) {
            // Using a document as the key. Just treat as a plain element.
            result = findShardForElement(chunkMapping, keyDocument);
        }
        else {
            // Limit the mapping based on the operators.
            SortedMap<Element, Shard> remainingChunks = chunkMapping;
            for (final Element operator : keyDocument) {
                final String name = operator.getName();
                final Element key = operator.withName(keyDocument.getName());

                if (ComparisonOperator.LT.getToken().equals(name)) {
                    remainingChunks = filterForLessThan(remainingChunks, key);
                }
                else if (ComparisonOperator.LTE.getToken().equals(name)) {
                    remainingChunks = filterForLessThanOrEqual(remainingChunks,
                            key);
                }
                else if (ComparisonOperator.GT.getToken().equals(name)) {
                    remainingChunks = filterForGreaterThan(remainingChunks, key);
                }
                else if (ComparisonOperator.GTE.getToken().equals(name)) {
                    remainingChunks = filterForGreaterThanOrEqual(
                            remainingChunks, key);
                }
                else if (MiscellaneousOperator.IN.getToken().equals(name)) {
                    remainingChunks = filterForIn(keyDocument.getName(),
                            remainingChunks, key);
                }
                // Handle a subset. Simple rooted.
                // else if
                // (MiscellaneousOperator.REG_EX.getToken().equals(name)) {
                //
                //
                // Should be able to do something with this but not sure the
                // cost benefit.
                // else if (MiscellaneousOperator.TYPE.getToken().equals(name))
                // {
                //
                // }
                else {
                    // Can't optimize.
                    remainingChunks = chunkMapping;
                    break;
                }
            }

            // If left is empty we can optimize.
            if (!remainingChunks.isEmpty()) {

                // If it is all the same shard we can still route the query to
                // that single shard.
                result = remainingChunks.get(remainingChunks.firstKey());
                for (final Shard other : remainingChunks.values()) {
                    if (!other.equals(result)) {
                        result = null;
                    }
                }
            }
        }

        return result;
    }

    /**
     * Finds the single shard that contains the shard key's value.
     * 
     * @param chunkMapping
     *            The mapping for chunk ranges. Each entry contains the key for
     *            the exclusive end of the chunk range.
     * @param keyElement
     *            The shard key value to target to a shard.
     * @return The shard holding all of the possibly matching documents or null
     *         if we cannot determine a single shard for the shard key's query
     *         document.
     */
    protected Shard findShardForElement(
            final SortedMap<Element, Shard> chunkMapping,
            final Element keyElement) {
        final Element chunkKey = findKeyFor(chunkMapping, keyElement);
        if (chunkKey != null) {
            return chunkMapping.get(chunkKey);
        }
        return null;
    }

    /**
     * Returns the key for the chunk containing the key value. Note that if the
     * key is in the map we return the next key in the mapping since the mapping
     * entries are exclusive of the range.
     * 
     * @param mapping
     *            The mapping for chunk ranges. Each entry contains the key for
     *            the exclusive end of the chunk range.
     * @param keyElement
     *            The key to find the chunk for.
     * @return The key for the chunk containing the key value.
     */
    private Element findKeyFor(final SortedMap<Element, Shard> mapping,
            final Element keyElement) {
        Element result = null;

        final SortedMap<Element, Shard> tail = mapping.tailMap(keyElement);
        if (tail.isEmpty()) {
            // Something.
            result = mapping.lastKey();
        }
        else if (keyElement.equals(tail.firstKey())) {
            // The key is actually in the next chunk.
            result = secondKey(tail, tail.lastKey());
        }
        else {
            result = tail.firstKey();
        }
        return result;
    }

    /**
     * Returns the second key from the {@code chunks} map.
     * 
     * @param chunks
     *            The map to pull the second key from.
     * @param defaultKey
     *            The key to return if the map only has a single entry.
     * @return The second key from the map or the {@code defaultKey}.
     */
    private Element secondKey(final SortedMap<Element, Shard> chunks,
            final Element defaultKey) {
        Element key = defaultKey;
        final Iterator<Element> iterator = chunks.keySet().iterator();
        if (iterator.hasNext()) {
            iterator.next();
            if (iterator.hasNext()) {
                key = iterator.next();
            }
        }
        return key;
    }

    /**
     * Returns the third key from the {@code chunks} map.
     * 
     * @param chunks
     *            The map to pull the third key from.
     * @param defaultKey
     *            The key to return if the map only has one or two entries.
     * @return The third key from the map or the {@code defaultKey}.
     */
    private Element thirdKey(final SortedMap<Element, Shard> chunks,
            final Element defaultKey) {
        Element key = defaultKey;
        final Iterator<Element> iterator = chunks.keySet().iterator();
        if (iterator.hasNext()) {
            iterator.next();
            if (iterator.hasNext()) {
                iterator.next();
                if (iterator.hasNext()) {
                    key = iterator.next();
                }
            }
        }
        return key;
    }
}
