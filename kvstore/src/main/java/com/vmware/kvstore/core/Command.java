package com.vmware.kvstore.core;

public enum Command {
    READ,
    WRITE,
    DELETE,
    START,
    COMMIT,
    ABORT,
    QUIT,
    UNKNOWN;

    static public Command toCommand(final String cmd) {
        if (cmd.equalsIgnoreCase("read")) {
            return READ;
        } else if (cmd.equalsIgnoreCase("write")) {
            return WRITE;
        } else if (cmd.equalsIgnoreCase("delete")) {
            return DELETE;
        } else if (cmd.equalsIgnoreCase("start")) {
            return START;
        } else if (cmd.equalsIgnoreCase("commit")) {
            return COMMIT;
        } else if (cmd.equalsIgnoreCase("abort")) {
            return ABORT;
        } else if (cmd.equalsIgnoreCase("quit")) {
            return QUIT;
        }
        return UNKNOWN;
    }
}
