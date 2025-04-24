package com.vmware.kvstore.core;

import com.vmware.kvstore.exceptions.KeyDeletedException;
import com.vmware.kvstore.exceptions.KeyNotFoundException;
import com.vmware.kvstore.store.RootStore;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;

public class Engine {
    private final OutputStream os;

    public Engine(final OutputStream os) {
        this.os = os;
    }

    private RootStore<String, String> rs = new RootStore<>();
    private final Deque<Transaction<String, String>> activeTransactions = new ArrayDeque<>();

    private void executeWrite(final String key, final String value) {
        if (!activeTransactions.isEmpty()) {
            Transaction<String, String> ts = activeTransactions.peekLast();
            ts.write(key, value);
        } else {
            rs.write(key, value);
        }
    }

    private String executeRead(final String key) throws KeyNotFoundException, KeyDeletedException {
        if (!activeTransactions.isEmpty()) {
            Transaction<String, String> ts = activeTransactions.peekLast();
            try { // Return transaction value
                return ts.read(key);
            } catch (KeyNotFoundException e) {
                // Return root store value if not update in transaction
                return rs.read(key);
            }
        }
        return rs.read(key);
    }

    private void executeDelete(final String key) throws KeyNotFoundException {
        if (!activeTransactions.isEmpty()) {
            Transaction<String, String> ts = activeTransactions.peekLast();
            ts.delete(key);
        } else {
            rs.delete(key);
        }
    }

    private void executeStart() {
        activeTransactions.offer(new Transaction<String, String>());
    }

    private void executeAbort() {
        activeTransactions.pollLast();
    }

    private void executeCommit() {
        Transaction<String, String> topTransaction = activeTransactions.pollLast();
        if (topTransaction != null) {
            Transaction<String, String> newTop = activeTransactions.peekLast();
            if (newTop != null) {
                newTop.merge(topTransaction);
            } else {
                rs.apply(topTransaction);
            }
        }
    }

    public void execute(final Command command, final String key) throws IOException {
        if (command == Command.READ) {
            try {
                final String v = executeRead(key);
                os.write(String.format("%s\n", v).getBytes(StandardCharsets.UTF_8));
            } catch (KeyDeletedException | KeyNotFoundException e) {
                os.write(String.format("Key not found: %s\n", key).getBytes(StandardCharsets.UTF_8));
            }
        } else if (command == Command.DELETE) {
            try {
                executeDelete(key);
            } catch (KeyNotFoundException e) {
                os.write(String.format("Key not found: %s\n", key).getBytes());
            }
        }
    }
    public void execute(final Command command, final String key, final String value) throws IOException {
        if (command == Command.WRITE) {
            executeWrite(key, value);
        } else if (command == Command.COMMIT) {
            executeCommit();
        } else if (command == Command.ABORT) {
            executeAbort();
        } else if (command == Command.START) {
            executeStart();
        }
    }

    public void execute(final Command command) {
        if (command == Command.COMMIT) {
            executeCommit();
        } else if (command == Command.ABORT) {
            executeAbort();
        } else if (command == Command.START) {
            executeStart();
        }
    }

    public void reset() {
        rs.reset();
        activeTransactions.clear();
    }

}
