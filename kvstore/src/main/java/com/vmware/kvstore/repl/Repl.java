package com.vmware.kvstore.repl;

import com.vmware.kvstore.core.Command;
import com.vmware.kvstore.core.Engine;

import java.io.IOException;
import java.util.Scanner;

public class Repl {
    private Engine e = new Engine(System.out);
    private String[] parseLine(final String line) {
        return line.split(" ");
    }

    public void run() {
        boolean doRun = true;
        final Scanner stdin = new Scanner(System.in);

        System.out.print("\n> ");
        while (doRun) {
            final String line = stdin.nextLine();
            final String[] tokens = parseLine(line);
            final Command cmd = Command.toCommand(tokens[0]);

            if (cmd == Command.UNKNOWN) {
                System.out.print("Unknown command\n");
            }
            if (cmd == Command.COMMIT || cmd == Command.START || cmd == Command.ABORT) {
                if (tokens.length < 1) {
                    System.out.print("Invalid command\n");
                }
                e.execute(cmd);
            } else if (cmd == Command.READ || cmd == Command.DELETE) {
                if (tokens.length < 2) {
                    System.out.print("Invalid command\n");
                }
                try {
                    e.execute(cmd, tokens[1]);
                } catch (IOException e) {
                    System.out.print("Error executing command\n");
                }
            } else if (cmd == Command.WRITE) {
                if (tokens.length < 3) {
                    System.out.print("Invalid command\n");
                }
                try {
                    e.execute(cmd, tokens[1], tokens[2]);
                } catch (IOException e) {
                    System.out.print("Error executing command");
                }
            } else if (cmd == Command.QUIT) {
                doRun = false;
            }
            if (!doRun) {
                System.out.println("Bye!");
            } else {
                System.out.print("> ");
            }
        }

    }
}
