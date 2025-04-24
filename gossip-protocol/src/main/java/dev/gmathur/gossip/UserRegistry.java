package dev.gmathur.gossip;

import dev.gmathur.gossip.proto.User;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UserRegistry {
    private final Map<String, User> users = new ConcurrentHashMap<>();

    /**
     * Add or update a user in the registry
     */
    public void addUser(User user) {
        String userId = user.getId();
        User existingUser = users.get(userId);

        if (existingUser == null) {
            // New user, just add it
            users.put(userId, user);
            return;
        }

        // Compare version vectors to determine which is newer
        int comparison = compareVersionVectors(
                existingUser.getVersionVectorMap(),
                user.getVersionVectorMap()
        );

        switch (comparison) {
            case -1: // Updated user is newer
                users.put(userId, user);
                break;
            case 0: // Concurrent updates, resolve by timestamp
                if (user.getTimestamp() > existingUser.getTimestamp()) {
                    users.put(userId, user);
                }
                break;
            case 1: // Existing user is newer, keep it
                // No action needed
                break;
        }
    }

    /**
     * Create a new user with initial version vector
     */
    public User createUser(String userId, String name, String email, String nodeId) {
        Map<String, Long> versionVector = new HashMap<>();
        versionVector.put(nodeId, 1L);

        User user = User.newBuilder()
                .setId(userId)
                .setName(name)
                .setEmail(email)
                .putAllVersionVector(versionVector)
                .setTimestamp(System.currentTimeMillis())
                .build();

        users.put(userId, user);
        return user;
    }

    /**
     * Get all users in the registry
     */
    public Collection<User> getAllUsers() {
        return users.values();
    }

    /**
     * Get a specific user by ID
     */
    public User getUser(String userId) {
        return users.get(userId);
    }

    /**
     * Create a digest of the current state for anti-entropy
     */
    public Map<String, Map<String, Long>> createStateDigest() {
        Map<String, Map<String, Long>> digest = new HashMap<>();

        for (Map.Entry<String, User> entry : users.entrySet()) {
            digest.put(entry.getKey(), entry.getValue().getVersionVectorMap());
        }

        return digest;
    }

    /**
     * Compare two version vectors
     * Returns -1 if v2 > v1, 0 if concurrent, 1 if v1 > v2
     */
    public static int compareVersionVectors(Map<String, Long> v1, Map<String, Long> v2) {
        boolean v1Dominates = false;
        boolean v2Dominates = false;

        // Check all keys in v1
        for (Map.Entry<String, Long> entry : v1.entrySet()) {
            String node = entry.getKey();
            long count1 = entry.getValue();

            Long count2 = v2.get(node);
            if (count2 == null) {
                v1Dominates = true;
            } else if (count1 > count2) {
                v1Dominates = true;
            } else if (count2 > count1) {
                v2Dominates = true;
            }
        }

        // Check if v2 has keys that v1 doesn't have
        for (String node : v2.keySet()) {
            if (!v1.containsKey(node)) {
                v2Dominates = true;
            }
        }

        if (v1Dominates && !v2Dominates) {
            return 1;
        } else if (v2Dominates && !v1Dominates) {
            return -1;
        } else {
            return 0; // Concurrent updates
        }
    }
}
