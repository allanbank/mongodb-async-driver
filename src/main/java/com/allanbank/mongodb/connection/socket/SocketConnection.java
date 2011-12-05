/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.socket;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.Operation;
import com.allanbank.mongodb.connection.messsage.Delete;
import com.allanbank.mongodb.connection.messsage.GetMore;
import com.allanbank.mongodb.connection.messsage.Header;
import com.allanbank.mongodb.connection.messsage.Insert;
import com.allanbank.mongodb.connection.messsage.KillCursors;
import com.allanbank.mongodb.connection.messsage.Query;
import com.allanbank.mongodb.connection.messsage.Reply;
import com.allanbank.mongodb.connection.messsage.Update;

/**
 * Provides a blocking Socket based connection to a MongoDB server.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SocketConnection implements Connection {

    /** The length of the message header in bytes. */
    public static final int HEADER_LENGTH = 16;

    /** The writer for BSON documents. Shares this objects {@link #myOutBuffer}. */
    private final BsonOutputStream myBsonOut;

    /** The buffered input stream. */
    private final BufferedInputStream myInput;

    /** The next message id. */
    private int myNextId;

    /** The buffered output stream. */
    private final BufferedOutputStream myOutput;

    /** The writer for BSON documents. Shares this objects {@link #myOutBuffer}. */
    private final BsonInputStream myBsonIn;

    /** The open socket. */
    private final Socket mySocket;

    /**
     * Creates a new SocketConnection to a MongoDB server.
     * 
     * @param address
     *            The address of the MongoDB server to connect to.
     * @param config
     *            The configuration for the Connection to the MongoDB server.
     * @throws SocketException
     *             On a failure connecting to the MongoDB server.
     * @throws IOException
     *             On a failure to read or write data to the MongoDB server.
     */
    public SocketConnection(final InetSocketAddress address,
            final MongoDbConfiguration config) throws SocketException,
            IOException {

        mySocket = new Socket();

        mySocket.setKeepAlive(config.isUsingSoKeepalive());
        mySocket.setSoTimeout(config.getReadTimeout());
        mySocket.connect(address, config.getConnectTimeout());

        myInput = new BufferedInputStream(mySocket.getInputStream());
        myBsonIn = new BsonInputStream(myInput);

        myOutput = new BufferedOutputStream(mySocket.getOutputStream());
        myBsonOut = new BsonOutputStream(myOutput);

        myNextId = 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        myOutput.close();
        myInput.close();
        mySocket.close();
    }

    /**
     * /** {@inheritDoc}
     */
    @Override
    public void flush() throws IOException {
        myOutput.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int send(Message... messages) throws MongoDbException {

        try {
            int messageId = -1;
            for (Message message : messages) {
                messageId = myNextId++;
                message.write(messageId, myBsonOut);
            }

            return messageId;
        }
        catch (final IOException ioe) {
            throw new MongoDbException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message receive() throws MongoDbException {
        try {
            final int length = myBsonIn.readInt();
            final int requestId = myBsonIn.readInt();
            final int responseId = myBsonIn.readInt();
            final int opCode = myBsonIn.readInt();

            Operation op = Operation.fromCode(opCode);
            if (op == null) {
                // Huh? Dazed and confused
                throw new MongoDbException("Unexpected operation read '"
                        + opCode + "'.");
            }

            Header header = new Header(length, requestId, responseId, op);
            Message message = null;
            switch (op) {
            case REPLY:
                message = new Reply(header, myBsonIn);
                break;
            case QUERY:
                message = new Query(header, myBsonIn);
                break;
            case UPDATE:
                message = new Update(myBsonIn);
                break;
            case INSERT:
                message = new Insert(header, myBsonIn);
                break;
            case GET_MORE:
                message = new GetMore(myBsonIn);
                break;
            case DELETE:
                message = new Delete(myBsonIn);
                break;
            case KILL_CURSORS:
                message = new KillCursors(myBsonIn);
                break;

            }

            return message;
        }
        catch (final IOException ioe) {
            throw new MongoDbException(ioe);
        }
    }
}
