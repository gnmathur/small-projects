package com.vmware.kvstore.exceptions;

// Exception thrown when a tey that was deleted in a transaction was read
public class KeyDeletedException extends Exception {
    public KeyDeletedException() {
        super();
    }
}
