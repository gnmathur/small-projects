package com.vmware.kvstore.core;

import org.junit.Test;

import java.io.OutputStream;

public class EngineTest {
    static class Fixtures {
        static OutputStream os = System.out;
        static Engine e = new Engine(Fixtures.os);
    }

    @Test
    public void testWriteAndRead() {

    }
}