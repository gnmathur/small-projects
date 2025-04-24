package com.vmware;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.vmware.kvstore.core.Engine;
import com.vmware.kvstore.core.Command;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class KVStoreTest {
    static class Fixtures {
        static ByteArrayOutputStream os = new ByteArrayOutputStream();
        static Engine e = new Engine(Fixtures.os);
    }

    @Before
    public void preamble() {
        Fixtures.os.reset();
        Fixtures.e.reset();
    }

    @Test
    public void writesFollowedByReadsNoTransaction() throws IOException {
        Fixtures.e.execute(Command.WRITE, "key1", "key1value");
        Fixtures.e.execute(Command.WRITE, "key2", "key2value");

        Fixtures.e.execute(Command.READ, "key1");
        assertEquals("key1value", Fixtures.os.toString());

        Fixtures.os.reset();

        Fixtures.e.execute(Command.READ, "key2");
        assertEquals("key2value", Fixtures.os.toString());
    }

    @Test
    public void deleteShouldDeleteKeyNoTransaction() throws IOException {
        Fixtures.e.execute(Command.WRITE, "key1", "key1value");
        Fixtures.e.execute(Command.WRITE, "key2", "key2value");
        Fixtures.e.execute(Command.WRITE, "key3", "key3value");

        Fixtures.e.execute(Command.DELETE, "key2");
        Fixtures.e.execute(Command.READ, "key2");
        assertEquals("Key not found: key2", Fixtures.os.toString());

    }

    @Test
    public void deleteShouldNotOtherKeysNoTransaction() throws IOException {
        Fixtures.e.execute(Command.WRITE, "key1", "key1value");
        Fixtures.e.execute(Command.WRITE, "key2", "key2value");
        Fixtures.e.execute(Command.WRITE, "key3", "key3value");

        Fixtures.e.execute(Command.DELETE, "key2");

        Fixtures.e.execute(Command.READ, "key1");
        assertEquals("key1value", Fixtures.os.toString());

        Fixtures.os.reset();

        Fixtures.e.execute(Command.READ, "key3");
        assertEquals("key3value", Fixtures.os.toString());
    }

    @Test
    public void simpleTransaction() throws IOException {
        Fixtures.e.execute(Command.WRITE, "key1", "key1value");
        Fixtures.e.execute(Command.START);
        Fixtures.e.execute(Command.WRITE, "key2", "key2value");
        Fixtures.e.execute(Command.READ, "key2");
        assertEquals("key2value", Fixtures.os.toString());
        Fixtures.e.execute(Command.COMMIT);

        Fixtures.os.reset();
        Fixtures.e.execute(Command.READ, "key1");
        assertEquals("key1value", Fixtures.os.toString());

        Fixtures.os.reset();
        Fixtures.e.execute(Command.READ, "key2");
        assertEquals("key2value", Fixtures.os.toString());

    }

    @Test
    public void simpleAbortedTransaction() throws IOException {
        Fixtures.e.execute(Command.WRITE, "key1", "key1value");
        Fixtures.e.execute(Command.START);
        Fixtures.e.execute(Command.WRITE, "key2", "key2value");
        Fixtures.e.execute(Command.READ, "key2");
        // Key value read from the uncommitted transaction
        assertEquals("key2value", Fixtures.os.toString());
        Fixtures.e.execute(Command.ABORT);

        Fixtures.os.reset();
        Fixtures.e.execute(Command.READ, "key1");
        // Root store key should be as before
        assertEquals("key1value", Fixtures.os.toString());

        Fixtures.os.reset();
        Fixtures.e.execute(Command.READ, "key2");
        // Aborted transaction key is not written to the root store
        assertEquals("Key not found: key2", Fixtures.os.toString());
    }

    @Test
    public void exerciseTest() throws IOException {
        Fixtures.e.execute(Command.WRITE, "a", "hello");
        Fixtures.e.execute(Command.READ, "a");
        // Key value read from the root store
        assertEquals("hello", Fixtures.os.toString());

        Fixtures.e.execute(Command.START);
        Fixtures.e.execute(Command.WRITE, "a", "hello-again");
        Fixtures.os.reset();
        Fixtures.e.execute(Command.READ, "a");
        // Key value read from the root store
        assertEquals("hello-again", Fixtures.os.toString());

        Fixtures.e.execute(Command.START);
        Fixtures.e.execute(Command.DELETE, "a");
        Fixtures.os.reset();
        Fixtures.e.execute(Command.READ, "a");
        // Key value read from the root store
        assertEquals("Key not found: a", Fixtures.os.toString());

        Fixtures.e.execute(Command.COMMIT);
        Fixtures.os.reset();
        Fixtures.e.execute(Command.READ, "a");
        // Key value read from the root store
        assertEquals("Key not found: a", Fixtures.os.toString());

        Fixtures.e.execute(Command.WRITE, "a", "once-more");
        Fixtures.os.reset();
        Fixtures.e.execute(Command.READ, "a");
        // Key value read from the root store
        assertEquals("once-more", Fixtures.os.toString());

        Fixtures.e.execute(Command.ABORT);
        Fixtures.os.reset();
        Fixtures.e.execute(Command.READ, "a");
        // Key value read from the root store
        assertEquals("hello", Fixtures.os.toString());

    }
}
