package com.vmware.kvstore.exceptions;

// Exception raised if the key was not found in the transaction or root store
public class KeyNotFoundException extends Exception {
    public KeyNotFoundException() {
        super();
    }
}
