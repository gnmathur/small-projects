package dev.gmathur.gossip;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import dev.gmathur.gossip.proto.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class GossipNodeOld extends GossipServiceGrpc.GossipServiceImplBase {
    private static final Logger logger = Logger.getLogger(GossipNode.class.getName());
    private static final SecureRandom random = new SecureRandom();

    private final String nodeId;
    private final String address;
    private final Server grpcServer;

    // User registry
    private final Map<String, User> users = new ConcurrentHashMap<>();

    // Known message IDs to avoid reprocessing (with expiration timestamps)
    private final Map<String, Long> processedMessages = new ConcurrentHashMap<>();

    // Known peers (nodeId -> address)
    private final Map<String, String> peers = new ConcurrentHashMap<>();

    // Gossip configuration
    private final int fanout;
    private final int maxHops;
    private final long gossipIntervalMs;

    // Scheduled executors for background tasks
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);

    public GossipNodeOld(String address, int port) {
        this.nodeId = generateId();
        this.address = address + ":" + port;
        this.fanout = 3; // Send to 3 random peers each round
        this.maxHops = 5; // Maximum of 5 hops for a message
        this.gossipIntervalMs = 5000; // Gossip every 5 seconds

        // Create and start gRPC server
        this.grpcServer = ServerBuilder.forPort(port)
                .addService(this)
                .build();
    }

    public void start() throws IOException {
        grpcServer.start();
        logger.info("Node " + nodeId + " started on " + address);

        // Start background tasks
        scheduler.scheduleAtFixedRate(this::gossipRoutine,
                gossipIntervalMs, gossipIntervalMs, TimeUnit.MILLISECONDS);

        scheduler.scheduleAtFixedRate(this::antiEntropyRoutine,
                30000, 30000, TimeUnit.MILLISECONDS);

        scheduler.scheduleAtFixedRate(this::cleanupProcessedMessages,
                3600000, 3600000, TimeUnit.MILLISECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("Shutting down node " + nodeId);
            try {
                GossipNodeOld.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }));
    }

    public void stop() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
        scheduler.shutdown();
    }

    public boolean joinNetwork(String seedAddress) {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(seedAddress)
                .usePlaintext()
                .build();

        try {
            GossipServiceGrpc.GossipServiceBlockingStub stub =
                    GossipServiceGrpc.newBlockingStub(channel);

            JoinRequest request = JoinRequest.newBuilder()
                    .setNodeId(nodeId)
                    .setAddress(address)
                    .build();

            JoinResponse response = stub.join(request);

            // Add known nodes from response
            for (NodeInfo nodeInfo : response.getKnownNodesList()) {
                if (!nodeInfo.getNodeId().equals(nodeId)) {
                    peers.put(nodeInfo.getNodeId(), nodeInfo.getAddress());
                }
            }

            logger.info("Successfully joined network through " + seedAddress);
            return true;
        } catch (StatusRuntimeException e) {
            logger.warning("Failed to join network: " + e.getStatus());
            return false;
        } finally {
            channel.shutdown();
        }
    }

    @Override
    public void join(JoinRequest request, StreamObserver<JoinResponse> responseObserver) {
        // Add the new node to our peers
        peers.put(request.getNodeId(), request.getAddress());

        // Prepare response with known peers
        JoinResponse.Builder responseBuilder = JoinResponse.newBuilder();

        // Add ourselves
        responseBuilder.addKnownNodes(NodeInfo.newBuilder()
                .setNodeId(nodeId)
                .setAddress(address)
                .build());

        // Add all peers
        for (Map.Entry<String, String> entry : peers.entrySet()) {
            responseBuilder.addKnownNodes(NodeInfo.newBuilder()
                    .setNodeId(entry.getKey())
                    .setAddress(entry.getValue())
                    .build());
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    // Register a new user locally and start gossip
    public void registerUser(String userId, String name, String email) {
        // Create version vector with our entry
        Map<String, Long> versionVector = new HashMap<>();
        versionVector.put(nodeId, 1L);

        // Create user
        User user = User.newBuilder()
                .setId(userId)
                .setName(name)
                .setEmail(email)
                .putAllVersionVector(versionVector)
                .setTimestamp(System.currentTimeMillis())
                .build();

        // Add to local registry
        users.put(userId, user);

        // Create gossip message
        GossipMessage message = GossipMessage.newBuilder()
                .setMessageId(generateId())
                .setSourceNodeId(nodeId)
                .addUsers(user)
                .setHopCount(0)
                .build();

        // Mark as processed
        markMessageProcessed(message.getMessageId());

        // Start gossip
        scheduler.execute(() -> gossipMessage(message));
    }

    @Override
    public void receiveGossip(GossipMessage request,
                              StreamObserver<Empty> responseObserver) {
        // Check if already processed
        if (isMessageProcessed(request.getMessageId())) {
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
            return;
        }

        // Mark as processed
        markMessageProcessed(request.getMessageId());

        // Process user updates
        for (User user : request.getUsersList()) {
            processUserUpdate(user);
        }

        // Forward if hop count permits
        if (request.getHopCount() < maxHops) {
            GossipMessage forwardMessage = GossipMessage.newBuilder()
                    .setMessageId(request.getMessageId())
                    .setSourceNodeId(request.getSourceNodeId())
                    .addAllUsers(request.getUsersList())
                    .setHopCount(request.getHopCount() + 1)
                    .build();

            scheduler.execute(() -> gossipMessage(forwardMessage));
        }

        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void sync(SyncRequest request, StreamObserver<SyncResponse> responseObserver) {
        List<User> missingUsers = new ArrayList<>();

        // Check what users we have that requester is missing or has old versions
        for (Map.Entry<String, User> entry : users.entrySet()) {
            String userId = entry.getKey();
            User user = entry.getValue();

            StateVectorEntry requesterState = request.getStateDigestOrDefault(userId, null);

            if (requesterState == null) {
                // Requester doesn't have this user at all
                missingUsers.add(user);
            } else {
                // Compare version vectors
                Map<String, Long> requesterVector = requesterState.getVersionVectorMap();

                switch (compareVersionVectors(requesterVector, user.getVersionVectorMap())) {
                    case -1: // Our version is newer
                    case 0:  // Concurrent updates, send ours too
                        missingUsers.add(user);
                        break;
                    case 1:  // Requester's version is newer, nothing to send
                        break;
                }
            }
        }

        SyncResponse response = SyncResponse.newBuilder()
                .addAllMissingUsers(missingUsers)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    // Process an incoming user update using version vectors
    private void processUserUpdate(User updatedUser) {
        User existingUser = users.get(updatedUser.getId());

        if (existingUser == null) {
            // New user, just add it
            users.put(updatedUser.getId(), updatedUser);
            return;
        }

        // Compare version vectors
        int comparison = compareVersionVectors(
                existingUser.getVersionVectorMap(),
                updatedUser.getVersionVectorMap()
        );

        switch (comparison) {
            case -1: // Updated user is newer
                users.put(updatedUser.getId(), updatedUser);
                break;
            case 0: // Concurrent updates, resolve by timestamp
                if (updatedUser.getTimestamp() > existingUser.getTimestamp()) {
                    users.put(updatedUser.getId(), updatedUser);
                }
                break;
            case 1: // Existing user is newer, keep it
                // No action needed
                break;
        }
    }

    // Compare two version vectors
    // Returns -1 if v2 > v1, 0 if concurrent, 1 if v1 > v2
    private int compareVersionVectors(Map<String, Long> v1, Map<String, Long> v2) {
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

    // Periodically gossip all users
    private void gossipRoutine() {
        if (users.isEmpty() || peers.isEmpty()) {
            return;
        }

        // Create gossip message with all users
        GossipMessage.Builder messageBuilder = GossipMessage.newBuilder()
                .setMessageId(generateId())
                .setSourceNodeId(nodeId)
                .setHopCount(0);

        // Add all users
        for (User user : users.values()) {
            messageBuilder.addUsers(user);
        }

        GossipMessage message = messageBuilder.build();

        // Mark as processed
        markMessageProcessed(message.getMessageId());

        // Send to random peers
        gossipMessage(message);
    }

    // Send a gossip message to random peers
    private void gossipMessage(GossipMessage message) {
        // Get random subset of peers
        List<String> selectedPeers = getRandomPeers(fanout);
        if (selectedPeers.isEmpty()) {
            return;
        }

        // Send to each selected peer
        for (String peerAddress : selectedPeers) {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(peerAddress)
                    .usePlaintext()
                    .build();

            try {
                GossipServiceGrpc.GossipServiceFutureStub stub =
                        GossipServiceGrpc.newFutureStub(channel);

                ListenableFuture<Empty> future = stub.receiveGossip(message);

                // Add callback to close channel when done
                Futures.addCallback(future, new FutureCallback<Empty>() {
                    @Override
                    public void onSuccess(Empty result) {
                        channel.shutdown();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        logger.warning("Failed to send gossip to " + peerAddress + ": " + t.getMessage());
                        channel.shutdown();
                    }
                }, MoreExecutors.directExecutor());

            } catch (Exception e) {
                logger.warning("Failed to create stub for " + peerAddress + ": " + e.getMessage());
                channel.shutdown();
            }
        }
    }

    // Periodically sync with a random peer (anti-entropy)
    private void antiEntropyRoutine() {
        if (peers.isEmpty()) {
            return;
        }

        // Select one random peer
        List<String> selectedPeers = getRandomPeers(1);
        if (selectedPeers.isEmpty()) {
            return;
        }

        String peerAddress = selectedPeers.get(0);

        // Create state digest
        Map<String, StateVectorEntry> stateDigest = new HashMap<>();

        for (Map.Entry<String, User> entry : users.entrySet()) {
            User user = entry.getValue();
            StateVectorEntry vectorEntry = StateVectorEntry.newBuilder()
                    .putAllVersionVector(user.getVersionVectorMap())
                    .build();

            stateDigest.put(user.getId(), vectorEntry);
        }

        // Create sync request
        SyncRequest request = SyncRequest.newBuilder()
                .setNodeId(nodeId)
                .putAllStateDigest(stateDigest)
                .build();

        // Send sync request
        ManagedChannel channel = ManagedChannelBuilder.forTarget(peerAddress)
                .usePlaintext()
                .build();

        try {
            GossipServiceGrpc.GossipServiceFutureStub stub =
                    GossipServiceGrpc.newFutureStub(channel);

            ListenableFuture<SyncResponse> future = stub.sync(request);

            // Process response
            Futures.addCallback(future, new FutureCallback<SyncResponse>() {
                @Override
                public void onSuccess(SyncResponse response) {
                    // Process missing users
                    for (User user : response.getMissingUsersList()) {
                        processUserUpdate(user);
                    }
                    channel.shutdown();
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.warning("Failed to sync with " + peerAddress + ": " + t.getMessage());
                    channel.shutdown();
                }
            }, MoreExecutors.directExecutor());

        } catch (Exception e) {
            logger.warning("Failed to create stub for " + peerAddress + ": " + e.getMessage());
            channel.shutdown();
        }
    }

    // Get random subset of peers
    private List<String> getRandomPeers(int count) {
        if (peers.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> peerAddresses = new ArrayList<>(peers.values());

        // Adjust count if we have fewer peers
        if (count >= peerAddresses.size()) {
            return new ArrayList<>(peerAddresses);
        }

        // Randomly select 'count' addresses
        List<String> selected = new ArrayList<>(count);
        List<String> available = new ArrayList<>(peerAddresses);

        for (int i = 0; i < count; i++) {
            int randomIndex = random.nextInt(available.size());
            selected.add(available.get(randomIndex));
            available.remove(randomIndex);
        }

        return selected;
    }

    // Mark a message as processed
    private void markMessageProcessed(String messageId) {
        processedMessages.put(messageId, System.currentTimeMillis());
    }

    // Check if a message has been processed
    private boolean isMessageProcessed(String messageId) {
        return processedMessages.containsKey(messageId);
    }

    // Clean up old processed message IDs
    private void cleanupProcessedMessages() {
        long expirationTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(24);

        processedMessages.entrySet().removeIf(entry -> entry.getValue() < expirationTime);
    }

    // Generate a random ID
    private static String generateId() {
        byte[] bytes = new byte[16];
        random.nextBytes(bytes);

        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    // Get all users in the registry
    public Collection<User> listUsers() {
        return new ArrayList<>(users.values());
    }

    // Main method for demonstration
    public static void main(String[] args) throws Exception {
        // Create first node
        GossipNode node1 = new GossipNode("localhost", 8081);
        node1.start();

        Thread.sleep(1000); // Give it time to start

        // Create second node and join the network
        GossipNode node2 = new GossipNode("localhost", 8082);
        node2.start();
        node2.joinNetwork("localhost:8081");

        Thread.sleep(1000);

        // Create third node
        GossipNode node3 = new GossipNode("localhost", 8083);
        node3.start();
        node3.joinNetwork("localhost:8081");

        // Register a user on node1
        node1.registerUser("user1", "Alice", "alice@example.com");

        // Register a user on node2
        node2.registerUser("user2", "Bob", "bob@example.com");

        // Wait for gossip to propagate
        Thread.sleep(10000);

        // Print users on each node
        System.out.println("Node 1 users: " + node1.listUsers());
        System.out.println("Node 2 users: " + node2.listUsers());
        System.out.println("Node 3 users: " + node3.listUsers());

        // Keep the program running
        Thread.sleep(Long.MAX_VALUE);
    }
}
