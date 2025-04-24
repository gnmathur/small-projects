package com.vmware.kvstore.core;

import com.vmware.kvstore.exceptions.KeyDeletedException;
import com.vmware.kvstore.exceptions.KeyNotFoundException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Transaction<K, V> {
    private final Set<K> deletedKeys = new HashSet<>();
    private final Map<K, V> updatedKeys = new HashMap<>();

    public void delete(K key) {
        updatedKeys.remove(key);
        deletedKeys.add(key);
    }

    public V read(K key) throws KeyNotFoundException, KeyDeletedException {
        if (deletedKeys.contains(key)) {
            throw new KeyDeletedException();
        }
        if (!updatedKeys.containsKey(key)) {
            throw new KeyNotFoundException();
        }
        return updatedKeys.get(key);
    }

    public void merge(final Transaction<K, V> otherT) {
        updatedKeys.putAll(otherT.getUpdatedKeys());
        deletedKeys.addAll(otherT.getDeletedKeys());
        for (K k: otherT.getDeletedKeys()) {
            updatedKeys.remove(k);
        }
    }
    public void write(K key, V value) {
        updatedKeys.put(key, value);
        // Un-mark as deleted if this key was marked as deleted in this transaction
        deletedKeys.remove(key);
    }

    public Map<K, V> getUpdatedKeys() {
        return updatedKeys;
    }

    public Set<K> getDeletedKeys() {
        return deletedKeys;
    }
}
