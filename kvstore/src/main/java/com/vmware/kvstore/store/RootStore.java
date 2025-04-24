package com.vmware.kvstore.store;

import com.vmware.kvstore.core.Transaction;
import com.vmware.kvstore.exceptions.KeyNotFoundException;

import java.util.HashMap;
import java.util.Map;

public class RootStore<K, V> {
    protected final Map<K, V> backingStore = new HashMap<>();

    public void write(final K key, final V value) {
        backingStore.put(key, value);
    }

    public V read(final K key) throws KeyNotFoundException {
        if (!backingStore.containsKey(key)) {
            throw new KeyNotFoundException();
        }
        return backingStore.get(key);
    }

    public void delete(K key) throws KeyNotFoundException  {
        if (!backingStore.containsKey(key)) {
            throw new KeyNotFoundException();
        }
        backingStore.remove(key);
    }

    public void apply(final Transaction<K, V> t) {
        backingStore.putAll(t.getUpdatedKeys());
        for (K key: t.getDeletedKeys()) {
            backingStore.remove(key);
        }
    }

    public void reset() {
        backingStore.clear();
    }
}
