package dev.gmathur.gossip;

import dev.gmathur.gossip.proto.User;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;
        import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Enhanced demo for the Gossip Protocol with random user generation
 * and verification of complete propagation.
 */
public class GossipDemoEnh {
    private static final Logger logger = Logger.getLogger(GossipDemoEnh.class.getName());
    private static final SecureRandom random = new SecureRandom();

    // Track all users created across all nodes
    private static final Map<String, UserInfo> allUsers = new ConcurrentHashMap<>();

    // Node configurations
    private static final int NODE_COUNT = 5;
    private static final int BASE_PORT = 8080;
    private static final String HOST = "localhost";

    // User generation parameters
    private static final int MIN_USERS_PER_NODE = 5;
    private static final int MAX_USERS_PER_NODE = 15;
    private static final String[] FIRST_NAMES = {"Alice", "Bob", "Charlie", "Diana", "Edward",
            "Fiona", "George", "Hannah", "Ian", "Julia", "Kevin", "Laura", "Michael",
            "Natalie", "Oliver", "Penny", "Quentin", "Rachel", "Samuel", "Tina"};
    private static final String[] LAST_NAMES = {"Smith", "Johnson", "Williams", "Jones", "Brown",
            "Davis", "Miller", "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson",
            "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson"};
    private static final String[] EMAIL_DOMAINS = {"gmail.com", "yahoo.com", "hotmail.com",
            "outlook.com", "example.com", "company.com", "university.edu"};

    public static void main(String[] args) throws Exception {
        // Create and start nodes
        List<GossipNode> nodes = startNodes(NODE_COUNT);

        // Connect nodes to form a network
        connectNodes(nodes);

        // Generate and register random users on each node
        generateAndRegisterUsers(nodes);

        // Wait for gossip to fully propagate
        waitForGossipConvergence(nodes);

        // Verify that all users are present on all nodes
        verifyConsistency(nodes);

        // Display user statistics
        printUserStatistics(nodes);

        // Shutdown all nodes
        shutdownNodes(nodes);
    }

    /**
     * Start multiple gossip nodes
     */
    private static List<GossipNode> startNodes(int count) throws IOException {
        List<GossipNode> nodes = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            int port = BASE_PORT + i;
            GossipNode node = new GossipNode(HOST, port);
            node.start();
            nodes.add(node);

            logger.info("Started node " + node.getNodeId() + " on " + node.getAddress());
        }

        return nodes;
    }

    /**
     * Connect nodes in a mesh network where each node knows about all others
     */
    private static void connectNodes(List<GossipNode> nodes) {
        if (nodes.size() <= 1) {
            return; // Nothing to connect
        }

        // First node is the seed
        String seedAddress = nodes.get(0).getAddress();

        // Other nodes join through the seed
        for (int i = 1; i < nodes.size(); i++) {
            GossipNode node = nodes.get(i);
            boolean joined = node.joinNetwork(seedAddress);

            if (joined) {
                logger.info("Node " + node.getNodeId() + " joined the network");
            } else {
                logger.warning("Node " + node.getNodeId() + " failed to join the network");
            }
        }

        // Give time for the network to stabilize
        sleep(2000);
    }

    /**
     * Generate and register random users on each node
     */
    private static void generateAndRegisterUsers(List<GossipNode> nodes) {
        int totalUsers = 0;

        for (int i = 0; i < nodes.size(); i++) {
            GossipNode node = nodes.get(i);

            // Determine how many users to add to this node
            int usersToAdd = MIN_USERS_PER_NODE + random.nextInt(MAX_USERS_PER_NODE - MIN_USERS_PER_NODE + 1);

            logger.info("Adding " + usersToAdd + " users to node " + node.getNodeId());

            // Generate and register each user
            for (int j = 0; j < usersToAdd; j++) {
                String userId = generateRandomUID();
                String name = generateRandomName();
                String email = generateRandomEmail(name);

                // Register user on this node
                node.registerUser(userId, name, email);

                // Track user globally
                allUsers.put(userId, new UserInfo(userId, name, email, "Node " + (i + 1)));
                totalUsers++;

                logger.fine("Registered user: " + userId + ", " + name + ", " + email);
            }
        }

        logger.info("Total of " + totalUsers + " users added across " + nodes.size() + " nodes");
    }

    /**
     * Wait for gossip to fully propagate by periodically checking for convergence
     */
    private static void waitForGossipConvergence(List<GossipNode> nodes) {
        int totalUsers = allUsers.size();
        int maxAttempts = 10;
        int attempt = 0;
        boolean converged = false;

        logger.info("Waiting for gossip convergence of " + totalUsers + " users...");

        while (!converged && attempt < maxAttempts) {
            attempt++;

            // Sleep to allow gossip to propagate
            sleep(3000);

            // Check if all nodes have all users
            converged = true;
            for (GossipNode node : nodes) {
                int nodeUserCount = node.listUsers().size();
                if (nodeUserCount < totalUsers) {
                    converged = false;
                    logger.info("Node " + node.getNodeId() + " has " + nodeUserCount +
                            " of " + totalUsers + " users. Still waiting...");
                    break;
                }
            }

            if (converged) {
                logger.info("Gossip convergence achieved after " + attempt + " attempts!");
            } else if (attempt == maxAttempts) {
                logger.warning("Maximum attempts reached without full convergence.");
            }
        }
    }

    /**
     * Verify that all users are present on all nodes and information is consistent
     */
    private static void verifyConsistency(List<GossipNode> nodes) {
        int totalUsers = allUsers.size();

        for (GossipNode node : nodes) {
            Collection<User> nodeUsers = node.listUsers();

            logger.info("Node " + node.getNodeId() + " has " + nodeUsers.size() +
                    " of " + totalUsers + " users");

            // Check if any users are missing
            Set<String> nodeUserIds = new HashSet<>();
            for (User user : nodeUsers) {
                nodeUserIds.add(user.getId());
            }

            Set<String> missingUsers = new HashSet<>(allUsers.keySet());
            missingUsers.removeAll(nodeUserIds);

            if (!missingUsers.isEmpty()) {
                logger.warning("Node " + node.getNodeId() + " is missing " +
                        missingUsers.size() + " users: " + missingUsers);
            }

            // Verify user data consistency
            for (User user : nodeUsers) {
                UserInfo originalInfo = allUsers.get(user.getId());
                if (originalInfo != null) {
                    if (!user.getName().equals(originalInfo.name) ||
                            !user.getEmail().equals(originalInfo.email)) {
                        logger.warning("Data inconsistency for user " + user.getId() +
                                " on node " + node.getNodeId());
                    }
                }
            }
        }
    }

    /**
     * Print statistics about the user distribution
     */
    private static void printUserStatistics(List<GossipNode> nodes) {
        System.out.println("\n========== GOSSIP PROTOCOL RESULTS ==========");
        System.out.println("Total users created: " + allUsers.size());

        for (int i = 0; i < nodes.size(); i++) {
            GossipNode node = nodes.get(i);
            Collection<User> users = node.listUsers();

            System.out.println("\nNode " + (i + 1) + " (" + node.getNodeId() + "):");
            System.out.println("  - Has " + users.size() + " of " + allUsers.size() + " users");

            // Calculate how many users were originally created on this node
            int originatedUsers = 0;
            for (UserInfo info : allUsers.values()) {
                if (info.originNode.equals("Node " + (i + 1))) {
                    originatedUsers++;
                }
            }

            System.out.println("  - Originally created " + originatedUsers + " users");

            if (users.size() < allUsers.size()) {
                System.out.println("  - MISSING " + (allUsers.size() - users.size()) + " USERS!");
            }
        }

        System.out.println("\n============================================");
    }

    /**
     * Shutdown all nodes
     */
    private static void shutdownNodes(List<GossipNode> nodes) {
        final CountDownLatch shutdownLatch = new CountDownLatch(nodes.size());

        for (GossipNode node : nodes) {
            new Thread(() -> {
                try {
                    node.stop();
                    logger.info("Node " + node.getNodeId() + " stopped");
                } catch (InterruptedException e) {
                    logger.log(Level.WARNING, "Error stopping node", e);
                } finally {
                    shutdownLatch.countDown();
                }
            }).start();
        }

        try {
            shutdownLatch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.log(Level.WARNING, "Shutdown interrupted", e);
            Thread.currentThread().interrupt();
        }

        logger.info("All nodes stopped");
    }

    /**
     * Generate a random UID
     */
    private static String generateRandomUID() {
        byte[] bytes = new byte[8];
        random.nextBytes(bytes);

        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * Generate a random name
     */
    private static String generateRandomName() {
        String firstName = FIRST_NAMES[random.nextInt(FIRST_NAMES.length)];
        String lastName = LAST_NAMES[random.nextInt(LAST_NAMES.length)];
        return firstName + " " + lastName;
    }

    /**
     * Generate a random email based on name
     */
    private static String generateRandomEmail(String name) {
        String[] parts = name.toLowerCase().split(" ");
        String domain = EMAIL_DOMAINS[random.nextInt(EMAIL_DOMAINS.length)];
        return parts[0] + "." + parts[1] + "@" + domain;
    }

    /**
     * Sleep for the specified milliseconds
     */
    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Helper class to track user information across nodes
     */
    private static class UserInfo {
        final String id;
        final String name;
        final String email;
        final String originNode;

        UserInfo(String id, String name, String email, String originNode) {
            this.id = id;
            this.name = name;
            this.email = email;
            this.originNode = originNode;
        }
    }
}
