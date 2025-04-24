package dev.gmathur.gossip;

import dev.gmathur.gossip.proto.User;
import java.util.Collection;
import java.util.Scanner;

public class GossipDemo {
    public static void main(String[] args) throws Exception {
        // Create first node
        GossipNode node1 = new GossipNode("localhost", 8081);
        node1.start();
        System.out.println("Node 1 started with ID: " + node1.getNodeId());

        // Create second node and join the network
        GossipNode node2 = new GossipNode("localhost", 8082);
        node2.start();
        node2.joinNetwork("localhost:8081");
        System.out.println("Node 2 started with ID: " + node2.getNodeId());

        // Create third node
        GossipNode node3 = new GossipNode("localhost", 8083);
        node3.start();
        node3.joinNetwork("localhost:8081");
        System.out.println("Node 3 started with ID: " + node3.getNodeId());

        // Interactive demo
        Scanner scanner = new Scanner(System.in);
        boolean running = true;

        while (running) {
            System.out.println("\n--- Gossip Protocol Demo ---");
            System.out.println("1. Register user on Node 1");
            System.out.println("2. Register user on Node 2");
            System.out.println("3. Register user on Node 3");
            System.out.println("4. List users on all nodes");
            System.out.println("5. Exit");
            System.out.print("Enter choice: ");

            int choice = scanner.nextInt();
            scanner.nextLine(); // Consume newline

            switch (choice) {
                case 1:
                case 2:
                case 3:
                    registerUser(choice == 1 ? node1 : (choice == 2 ? node2 : node3), scanner);
                    break;
                case 4:
                    listUsers(node1, node2, node3);
                    break;
                case 5:
                    running = false;
                    break;
                default:
                    System.out.println("Invalid choice!");
            }
        }

        // Shutdown nodes
        node1.stop();
        node2.stop();
        node3.stop();
        System.out.println("Demo ended. All nodes stopped.");
    }

    private static void registerUser(GossipNode node, Scanner scanner) {
        System.out.print("Enter user ID: ");
        String userId = scanner.nextLine();
        System.out.print("Enter user name: ");
        String name = scanner.nextLine();
        System.out.print("Enter user email: ");
        String email = scanner.nextLine();

        node.registerUser(userId, name, email);
        System.out.println("User registered. Waiting for gossip propagation...");

        // Give time for gossip to propagate
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void listUsers(GossipNode node1, GossipNode node2, GossipNode node3) {
        System.out.println("\n--- Users on Node 1 ---");
        printUsers(node1.listUsers());

        System.out.println("\n--- Users on Node 2 ---");
        printUsers(node2.listUsers());

        System.out.println("\n--- Users on Node 3 ---");
        printUsers(node3.listUsers());
    }

    private static void printUsers(Collection<User> users) {
        if (users.isEmpty()) {
            System.out.println("No users.");
            return;
        }

        for (User user : users) {
            System.out.printf("ID: %s, Name: %s, Email: %s%n",
                    user.getId(), user.getName(), user.getEmail());
        }
    }
}
