/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.allanbank.mongodb.BatchedAsyncMongoCollection;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.impl.EmptyDocument;
import com.allanbank.mongodb.builder.write.DeleteOperation;
import com.allanbank.mongodb.builder.write.InsertOperation;
import com.allanbank.mongodb.builder.write.UpdateOperation;
import com.allanbank.mongodb.builder.write.WriteOperation;
import com.allanbank.mongodb.builder.write.WriteOperationType;
import com.allanbank.mongodb.error.DocumentToLargeException;

/**
 * BatchedWrite provides a container for a group of write operations to be sent
 * to the server as one group.
 * <p>
 * The default mode ({@link BatchedWriteMode#SERIALIZE_AND_CONTINUE}) for this
 * class is to submit the operations to the server in the order that they are
 * added to the Builder and to apply as many of the writes as possible (commonly
 * referred to as continue-on-error). This has the effect of causing the fewest
 * surprises and optimizing the performance of the writes since the driver can
 * send multiple distinct writes to the server at once.
 * </p>
 * <p>
 * The {@link BatchedWriteMode#SERIALIZE_AND_STOP} mode also sends each write as
 * a separate request but instead of attempting all writes the driver will stop
 * sending requests once one of the writes fails. This also prevents the driver
 * from sending multiple write messages to the server which can degrade
 * performance.
 * </p>
 * <p>
 * The last mode, {@link BatchedWriteMode#REORDERED}, may re-order writes to
 * maximize performance. Similar to the
 * {@link BatchedWriteMode#SERIALIZE_AND_CONTINUE} this mode will also attempt
 * all writes. The reordering of writes is across all {@link WriteOperationType}
 * s.
 * </p>
 * <p>
 * If using a MongoDB server after {@link #REQUIRED_VERSION 2.5.5} a batched
 * write will result in use of the new write commands.
 * </p>
 * <p>
 * For a more generalized batched write and query capability see the
 * {@link BatchedAsyncMongoCollection} and {@link MongoCollection#startBatch()}.
 * </p>
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BatchedWrite implements Serializable {

    /**
     * Builder for creating {@link BatchedWrite}s.
     *
     * @api.yes This class is part of the driver's API. Public and protected
     *          members will be deprecated for at least 1 non-bugfix release
     *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
     *          before being removed or modified.
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static class Builder {

        /** The durability for the writes. */
        protected Durability myDurability;

        /** The mode for submitting the writes to the server. */
        protected BatchedWriteMode myMode;

        /** The writes to submit to the server. */
        protected final List<WriteOperation> myWrites;

        /**
         * Creates a new Builder.
         */
        public Builder() {
            myWrites = new ArrayList<WriteOperation>();

            reset();
        }

        /**
         * Constructs a new {@link BatchedWrite} object from the state of the
         * builder.
         *
         * @return The new {@link BatchedWrite} object.
         */
        public BatchedWrite build() {
            return new BatchedWrite(this);
        }

        /**
         * Update a document based on a query.
         * <p>
         * Defaults to deleting as many documents as match the query.
         * </p>
         * <p>
         * This method is delegates to
         * {@link #delete(DocumentAssignable, boolean) delete(query, false)}
         * </p>
         *
         * @param query
         *            The query to find the document to delete.
         * @return This builder for chaining method calls.
         */
        public Builder delete(final DocumentAssignable query) {
            return delete(query, false);
        }

        /**
         * Update a document based on a query.
         * <p>
         * Defaults to deleting as many documents as match the query.
         * </p>
         *
         * @param query
         *            The query to find the document to delete.
         * @param singleDelete
         *            If true then only a single document will be deleted. If
         *            running in a sharded environment then this field must be
         *            false or the query must contain the shard key.
         * @return This builder for chaining method calls.
         */
        public Builder delete(final DocumentAssignable query,
                final boolean singleDelete) {
            return write(new DeleteOperation(query, false));
        }

        /**
         * Sets the durability for the writes.
         * <p>
         * This method delegates to {@link #setDurability(Durability)}.
         * </p>
         *
         * @param durability
         *            The new value for the durability for the writes.
         * @return This builder for chaining method calls.
         */
        public Builder durability(final Durability durability) {
            return setDurability(durability);
        }

        /**
         * Returns the durability for the write.
         *
         * @return This durability for the write.
         */
        public Durability getDurability() {
            return myDurability;
        }

        /**
         * Adds an insert operation to the batched write.
         *
         * @param document
         *            The document to insert.
         * @return This builder for chaining method calls.
         */
        public Builder insert(final DocumentAssignable document) {
            return write(new InsertOperation(document));
        }

        /**
         * Sets the mode for submitting the writes to the server.
         * <p>
         * This method delegates to {@link #setMode(BatchedWriteMode)}.
         * </p>
         *
         * @param mode
         *            The new value for the mode for submitting the writes to
         *            the server.
         * @return This builder for chaining method calls.
         */
        public Builder mode(final BatchedWriteMode mode) {
            return setMode(mode);
        }

        /**
         * Resets the builder back to its initial state for reuse.
         *
         * @return This builder for chaining method calls.
         */
        public Builder reset() {
            myWrites.clear();
            myMode = BatchedWriteMode.SERIALIZE_AND_CONTINUE;
            myDurability = null;

            return this;
        }

        /**
         * Saves the {@code document} to MongoDB.
         * <p>
         * If the {@code document} does not contain an {@code _id} field then
         * this method is equivalent to: {@link #insert(DocumentAssignable)
         * insert(document)}.
         * </p>
         * <p>
         * If the {@code document} does contain an {@code _id} field then this
         * method is equivalent to:
         * {@link #update(DocumentAssignable, DocumentAssignable)
         * updateAsync(BuilderFactory.start().add(document.get("_id")),
         * document, false, true)}.
         * </p>
         *
         * @param document
         *            The document to save.
         * @return This builder for chaining method calls.
         */
        public Builder save(final DocumentAssignable document) {
            final Document doc = document.asDocument();
            final Element id = doc.get("_id");
            if (id == null) {
                return insert(doc);
            }
            return update(BuilderFactory.start().add(id), doc, false, true);
        }

        /**
         * Sets the durability for the writes.
         *
         * @param durability
         *            The new value for the durability for the writes.
         * @return This builder for chaining method calls.
         */
        public Builder setDurability(final Durability durability) {
            myDurability = durability;
            return this;
        }

        /**
         * Sets the mode for submitting the writes to the server.
         *
         * @param mode
         *            The new value for the mode for submitting the writes to
         *            the server.
         * @return This builder for chaining method calls.
         */
        public Builder setMode(final BatchedWriteMode mode) {
            myMode = mode;
            return this;
        }

        /**
         * Sets the writes to submit to the server.
         *
         * @param writes
         *            The new value for the writes to submit to the server.
         * @return This builder for chaining method calls.
         */
        public Builder setWrites(final List<WriteOperation> writes) {
            myWrites.clear();
            if (writes != null) {
                myWrites.addAll(writes);
            }
            return this;
        }

        /**
         * Update a document based on a query.
         * <p>
         * Defaults to updating a single document and not performing an upsert
         * if no document is found.
         * </p>
         * <p>
         * This method is delegates to
         * {@link #update(DocumentAssignable, DocumentAssignable, boolean, boolean)
         * update(query, update, false, false)}
         * </p>
         *
         * @param query
         *            The query to find the document to update.
         * @param update
         *            The update operations to apply to the document.
         * @return This builder for chaining method calls.
         */
        public Builder update(final DocumentAssignable query,
                final DocumentAssignable update) {
            return update(query, update, false, false);
        }

        /**
         * Update a document based on a query.
         * <p>
         * Defaults to updating a single document and not performing an upsert
         * if no document is found.
         * </p>
         *
         * @param query
         *            The query to find the document to update.
         * @param update
         *            The update operations to apply to the document.
         * @param multiUpdate
         *            If true then the update is applied to all of the matching
         *            documents, otherwise only the first document found is
         *            updated.
         * @param upsert
         *            If true then if no document is found then a new document
         *            is created and updated, otherwise no operation is
         *            performed.
         * @return This builder for chaining method calls.
         */
        public Builder update(final DocumentAssignable query,
                final DocumentAssignable update, final boolean multiUpdate,
                final boolean upsert) {
            return write(new UpdateOperation(query, update, multiUpdate, upsert));
        }

        /**
         * Adds a single write to the list of writes to send to the server.
         *
         * @param write
         *            The write to add to the list of writes to send to the
         *            server.
         * @return This builder for chaining method calls.
         */
        public Builder write(final WriteOperation write) {
            myWrites.add(write);
            return this;
        }

        /**
         * Sets the writes to submit to the server.
         * <p>
         * This method delegates to {@link #setWrites(List)}.
         * </p>
         *
         * @param writes
         *            The new value for the writes to submit to the server.
         * @return This builder for chaining method calls.
         */
        public Builder writes(final List<WriteOperation> writes) {
            return setWrites(writes);
        }
    }

    /**
     * Bundle is a container for the write command and the
     * {@link WriteOperation} it contains.
     *
     * @api.yes This class is part of the driver's API. Public and protected
     *          members will be deprecated for at least 1 non-bugfix release
     *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
     *          before being removed or modified.
     */
    public static final class Bundle {
        /** The command containing the bundled write operations. */
        private final Document myCommand;

        /** The writes that are bundled in the command. */
        private final List<WriteOperation> myWrites;

        /**
         * Creates a new Bundle.
         *
         * @param command
         *            The command containing the bundled write operations.
         * @param writes
         *            The writes that are bundled in the command.
         */
        protected Bundle(final Document command,
                final List<WriteOperation> writes) {
            super();
            myCommand = command;
            myWrites = Collections
                    .unmodifiableList(new ArrayList<WriteOperation>(writes));
        }

        /**
         * Returns the command containing the bundled write operations.
         *
         * @return The command containing the bundled write operations.
         */
        public Document getCommand() {
            return myCommand;
        }

        /**
         * Returns the writes that are bundled in the command.
         *
         * @return The writes that are bundled in the command.
         */
        public List<WriteOperation> getWrites() {
            return myWrites;
        }
    }

    /** The first version of MongoDB to support the {@code aggregate} command. */
    public static final Version REQUIRED_VERSION = Version.parse("2.5.5");

    /** Serialization version for the class. */
    private static final long serialVersionUID = 6984498574755719178L;

    /**
     * Creates a new builder for a {@link BatchedWrite}.
     *
     * @return The builder to construct a {@link BatchedWrite}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** The durability for the writes. */
    private final Durability myDurability;

    /** The mode for submitting the writes to the server. */
    private final BatchedWriteMode myMode;

    /** The writes to submit to the server. */
    private final List<WriteOperation> myWrites;

    /**
     * Creates a new BatchedWrite.
     *
     * @param builder
     *            The builder for the writes.
     */
    protected BatchedWrite(final Builder builder) {
        myWrites = Collections.unmodifiableList(new ArrayList<WriteOperation>(
                builder.myWrites));
        myMode = builder.myMode;
        myDurability = builder.myDurability;
    }

    /**
     * Returns the durability for the writes.
     *
     * @return The durability for the writes.
     */
    public Durability getDurability() {
        return myDurability;
    }

    /**
     * Returns the mode for submitting the writes to the server.
     *
     * @return The mode for submitting the writes to the server.
     */
    public BatchedWriteMode getMode() {
        return myMode;
    }

    /**
     * Returns the writes to submit to the server.
     *
     * @return The writes to submit to the server.
     */
    public List<WriteOperation> getWrites() {
        return myWrites;
    }

    /**
     * Creates write commands for all of the insert, updates and deletes. The
     * number and order of the writes is based on the {@link #getMode() mode}.
     *
     * @param collectionName
     *            The name of the collection the documents will be inserted
     *            into.
     * @param maxCommandSize
     *            The maximum document size.
     * @param maxOperationsPerBundle
     *            The maximum number of writes to include in each bundle.
     * @return The list of command documents to be sent.
     */
    public List<Bundle> toBundles(final String collectionName,
            final long maxCommandSize, final int maxOperationsPerBundle) {
        switch (getMode()) {
        case REORDERED: {
            return createOptimized(collectionName, maxCommandSize,
                    maxOperationsPerBundle);
        }
        case SERIALIZE_AND_CONTINUE: {
            return createSerialized(collectionName, maxCommandSize,
                    maxOperationsPerBundle, false);
        }
        default: {
            return createSerialized(collectionName, maxCommandSize,
                    maxOperationsPerBundle, true);
        }
        }
    }

    /**
     * Adds the document to the array of documents.
     *
     * @param array
     *            The array to add the operation to.
     * @param operation
     *            The operation to add.
     */
    private void add(final ArrayBuilder array, final WriteOperation operation) {
        switch (operation.getType()) {
        case INSERT: {
            final InsertOperation insertOperation = (InsertOperation) operation;

            array.add(insertOperation.getDocument());
            break;
        }
        case UPDATE: {
            final UpdateOperation updateOperation = (UpdateOperation) operation;
            final DocumentBuilder update = array.push();

            update.add("q", updateOperation.getQuery());
            update.add("u", updateOperation.getUpdate());
            if (updateOperation.isUpsert()) {
                update.add("upsert", true);
            }
            if (updateOperation.isMultiUpdate()) {
                update.add("multi", true);
            }
            break;
        }
        case DELETE: {
            final DeleteOperation deleteOperation = (DeleteOperation) operation;
            array.push().add("q", deleteOperation.getQuery())
                    .add("limit", deleteOperation.isSingleDelete() ? 1 : 0);
            break;
        }
        }
    }

    /**
     * Adds the durability ('writeConcern') to the command document.
     *
     * @param command
     *            The command document to add the durability to.
     * @param durability
     *            The durability to add. May be <code>null</code>.
     */
    private void addDurability(final DocumentBuilder command,
            final Durability durability) {
        if (durability != null) {
            final DocumentBuilder durabilityDoc = command.push("writeConcern");
            if (durability.equals(Durability.NONE)) {
                durabilityDoc.add("w", 0);
            }
            else if (durability.equals(Durability.ACK)) {
                durabilityDoc.add("w", 1);
            }
            else {
                boolean first = true;
                for (final Element part : durability.asDocument()) {
                    if (first) {
                        // The first element is "getlasterror".
                        first = false;
                    }
                    else {
                        durabilityDoc.add(part);
                    }
                }
            }
        }
    }

    /**
     * Creates a {@link DocumentToLargeException} for the operation.
     *
     * @param operation
     *            The large operation.
     * @param size
     *            The size of the operation.
     * @param maxCommandSize
     *            The maximum size of the operation.
     * @return The created exception.
     */
    private DocumentToLargeException createDocumentToLargeException(
            final WriteOperation operation, final int size,
            final int maxCommandSize) {

        switch (operation.getType()) {
        case INSERT: {
            final InsertOperation insertOperation = (InsertOperation) operation;
            return new DocumentToLargeException(size, maxCommandSize,
                    insertOperation.getDocument());
        }
        case UPDATE: {
            final UpdateOperation updateOperation = (UpdateOperation) operation;
            return new DocumentToLargeException(size, maxCommandSize,
                    updateOperation.getQuery());
        }
        case DELETE: {
            final DeleteOperation deleteOperation = (DeleteOperation) operation;
            return new DocumentToLargeException(size, maxCommandSize,
                    deleteOperation.getQuery());
        }
        }

        return new DocumentToLargeException(size, maxCommandSize,
                EmptyDocument.INSTANCE);
    }

    /**
     * Reorders the writes into as few write commands as possible.
     * <p>
     * <b>Note</b>: MongoDB gives a slightly larger document for the command (<a
     * href=
     * "https://github.com/mongodb/mongo/blob/master/src/mongo/bson/util/builder.h#L56"
     * >16K</a>). This is for the command overhead. We don't explicitly use the
     * overhead but we may end up implicitly using it in the case of a operation
     * that is just at or below maxCommandSize. For those cases we start the
     * 'head' map below with the full map. That allows the big operations to be
     * added to command documents of there own once the command overhead has
     * been factored in.
     * </p>
     *
     * @param collectionName
     *            The name of the collection the documents will be inserted
     *            into.
     * @param maxCommandSize
     *            The maximum document size.
     * @param maxOperationsPerBundle
     *            The maximum number of writes to include in each bundle.
     * @return The list of command documents to be sent.
     */
    private List<Bundle> createOptimized(final String collectionName,
            final long maxCommandSize, final int maxOperationsPerBundle) {
        // Bucket the operations and sort by size.
        Map<WriteOperationType, SortedMap<Long, List<WriteOperation>>> operationsBuckets;
        operationsBuckets = new LinkedHashMap<WriteOperationType, SortedMap<Long, List<WriteOperation>>>();
        for (final WriteOperation writeOp : getWrites()) {
            SortedMap<Long, List<WriteOperation>> operations = operationsBuckets
                    .get(writeOp.getType());
            if (operations == null) {
                operations = new TreeMap<Long, List<WriteOperation>>();
                operationsBuckets.put(writeOp.getType(), operations);
            }

            final Long size = Long.valueOf(sizeOf(-1, writeOp));
            List<WriteOperation> list = operations.get(size);
            if (list == null) {
                list = new LinkedList<WriteOperation>();
                operations.put(size, list);
            }
            list.add(writeOp);
        }

        // Check if any operation is too big.
        final Long maxMessageSize = Long.valueOf(maxCommandSize + 1);
        for (final SortedMap<Long, List<WriteOperation>> operations : operationsBuckets
                .values()) {
            if (!operations.tailMap(maxMessageSize).isEmpty()) {
                final Long biggest = operations.lastKey();
                final List<WriteOperation> operation = operations.get(biggest);
                throw createDocumentToLargeException(operation.get(0),
                        biggest.intValue(), (int) maxCommandSize);
            }
        }

        // Now build commands packing the operations into a few messages as
        // possible.
        final List<Bundle> commands = new ArrayList<Bundle>();
        final List<WriteOperation> bundled = new ArrayList<WriteOperation>(
                myWrites.size());
        final DocumentBuilder command = BuilderFactory.start();
        for (final Map.Entry<WriteOperationType, SortedMap<Long, List<WriteOperation>>> entry : operationsBuckets
                .entrySet()) {
            final SortedMap<Long, List<WriteOperation>> operations = entry
                    .getValue();
            while (!operations.isEmpty()) {
                final ArrayBuilder docs = start(entry.getKey(), collectionName,
                        false, command);
                long remaining = maxCommandSize - command.build().size();

                SortedMap<Long, List<WriteOperation>> head = operations;
                int index = 0;
                while (!head.isEmpty()
                        && (bundled.size() < maxOperationsPerBundle)) {
                    final Long biggest = head.lastKey();
                    final List<WriteOperation> bigOps = head.get(biggest);
                    final WriteOperation operation = bigOps.remove(0);
                    if (bigOps.isEmpty()) {
                        head.remove(biggest);
                    }

                    add(docs, operation);
                    bundled.add(operation);

                    remaining -= sizeOf(index, operation);
                    index += 1;
                    head = operations.headMap(Long.valueOf(remaining
                            - sizeOfIndex(index)));
                }

                commands.add(new Bundle(command.build(), bundled));
                bundled.clear();
            }
        }

        return commands;
    }

    /**
     * Creates write commands for each sequence of insert, updates and deletes.
     * <p>
     * <b>Note</b>: MongoDB gives a slightly larger document for the command (<a
     * href=
     * "https://github.com/mongodb/mongo/blob/master/src/mongo/bson/util/builder.h#L56"
     * >16K</a>). This is for the command overhead. We don't explicitly use the
     * overhead but we may end up using it in the case of a operation that is
     * just at or below maxCommandSize. That is why we start the 'head' map
     * below with the full map. That allows those big operations to be added to
     * commands of there own once the command overhead has been factored in.
     * </p>
     *
     * @param collectionName
     *            The name of the collection the documents will be inserted
     *            into.
     * @param maxCommandSize
     *            The maximum document size.
     * @param stopOnError
     *            If true then the ordered flag is set to true.
     * @param maxOperationsPerBundle
     *            The maximum number of writes to include in each bundle.
     * @return The list of command documents to be sent.
     */
    private List<Bundle> createSerialized(final String collectionName,
            final long maxCommandSize, final int maxOperationsPerBundle,
            final boolean stopOnError) {
        final List<Bundle> commands = new ArrayList<Bundle>();
        final DocumentBuilder command = BuilderFactory.start();

        final List<WriteOperation> toSend = getWrites();
        final List<WriteOperation> bundled = new ArrayList<WriteOperation>(
                toSend.size());

        ArrayBuilder opsArray = null;
        WriteOperationType lastType = null;

        long remaining = maxCommandSize;
        for (final WriteOperation writeOp : toSend) {
            long size = sizeOf(-1, writeOp);
            final long indexSize = sizeOfIndex(bundled.size());
            if (maxCommandSize < size) {
                throw createDocumentToLargeException(writeOp, (int) size,
                        (int) maxCommandSize);
            }
            size += indexSize; // Add in the index overhead.

            // Close a command if change type or too big.
            if (!bundled.isEmpty()
                    && ((lastType != writeOp.getType())
                            || ((remaining - size) < 0) || (maxOperationsPerBundle <= bundled
                            .size()))) {
                commands.add(new Bundle(command.build(), bundled));
                bundled.clear();
            }

            // Start a command? - Maybe after closing?
            if (bundled.isEmpty()) {
                opsArray = start(writeOp.getType(), collectionName,
                        stopOnError, command);
                lastType = writeOp.getType();
                remaining = (maxCommandSize - command.build().size());
            }

            // Add the operation.
            add(opsArray, writeOp);
            bundled.add(writeOp);

            // Remove the size of the operation from the remaining.
            remaining -= size;
        }

        if (!bundled.isEmpty()) {
            commands.add(new Bundle(command.build(), bundled));
        }

        return commands;
    }

    /**
     * Returns the size of the encoded operation.
     * <p>
     * For an {@code InsertOperation} this is the size of the document to
     * insert.
     * <p>
     * For an {@code UpdateOperation} this includes the space for:
     * <dl>
     * <dt>Document Overhead</dt>
     * <dd>type (1 byte), length (4 bytes), trailing null (1 byte).</dd>
     * <dt>'q' field</dt>
     * <dd>name (2 bytes), type (1 byte), value (document size)</dd>
     * <dt>'u' field</dt>
     * <dd>name (2 bytes), type (1 byte), value (document size)</dd>
     * <dt>'upsert' field</dt>
     * <dd>name (7 bytes), type (1 byte), value (1 byte)</dd>
     * <dt>'multi' field</dt>
     * <dd>name (6 bytes), type (1 byte), value (1 byte)</dd>
     * </dl>
     * </p>
     * <p>
     * For a {@code DeleteOperation} this includes the space for:
     * <dl>
     * <dt>Document Overhead</dt>
     * <dd>type (1 byte), length (4 bytes), trailing null (1 byte).</dd>
     * <dt>'q' field</dt>
     * <dd>name (2 bytes), type (1 byte), value (document size)</dd>
     * <dt>'limit' field</dt>
     * <dd>name (6 bytes), type (1 byte), value (4 bytes)</dd>
     * </dl>
     *
     * @param index
     *            The index of the operation in the operations array.
     * @param operation
     *            The operation to determine the size of.
     * @return The size of the operation.
     */
    private long sizeOf(final int index, final WriteOperation operation) {
        long result = 0;
        switch (operation.getType()) {
        case INSERT: {
            final InsertOperation insertOperation = (InsertOperation) operation;
            result = sizeOfIndex(index) + insertOperation.getDocument().size();
            break;
        }
        case UPDATE: {
            final UpdateOperation updateOperation = (UpdateOperation) operation;
            result = sizeOfIndex(index) + updateOperation.getQuery().size()
                    + updateOperation.getUpdate().size() + 29;
            break;
        }
        case DELETE: {
            final DeleteOperation deleteOperation = (DeleteOperation) operation;
            result = sizeOfIndex(index) + deleteOperation.getQuery().size()
                    + 20;
            break;
        }
        }

        return result;
    }

    /**
     * Returns the number of bytes required to encode the index within the array
     * element.
     *
     * @param index
     *            The index to return the size of.
     * @return The length of the encoded index.
     */
    private long sizeOfIndex(final int index) {
        if (index < 0) {
            return 0; // For estimating operation sizes.
        }
        else if (index < 10) {
            return 3; // single character plus a null plus a type.
        }
        else if (index < 100) {
            return 4; // two characters plus a null plus a type.
        }
        else if (index < 1000) {
            return 5; // three characters plus a null plus a type.
        }
        else if (index < 10000) {
            return 6; // four characters plus a null plus a type.
        }
        else if (index < 100000) {
            return 7; // five characters plus a null plus a type.
        }
        else if (index < 1000000) {
            return 8; // size characters plus a null plus a type.
        }

        return Integer.toString(index).length() + 2;
    }

    /**
     * Starts a new command document.
     *
     * @param operation
     *            The operation to start.
     * @param collectionName
     *            The collection to operate on.
     * @param stopOnError
     *            If true then the operations should stop once an error is
     *            encountered. Is mapped to the {@code ordered} field in the
     *            command document.
     * @param command
     *            The command builder.
     * @return The {@link ArrayBuilder} for the operations array.
     */
    private ArrayBuilder start(final WriteOperationType operation,
            final String collectionName, final boolean stopOnError,
            final DocumentBuilder command) {

        String commandName = "";
        String arrayName = "";
        switch (operation) {
        case INSERT: {
            commandName = "insert";
            arrayName = "documents";
            break;
        }
        case UPDATE: {
            commandName = "update";
            arrayName = "updates";
            break;
        }
        case DELETE: {
            commandName = "delete";
            arrayName = "deletes";
            break;
        }
        }

        command.reset();
        command.add(commandName, collectionName);
        if (!stopOnError) {
            command.add("ordered", stopOnError);
        }
        addDurability(command, getDurability());

        return command.pushArray(arrayName);
    }
}
