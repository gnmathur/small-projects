package dev.gmathur.gossip;

import dev.gmathur.gossip.proto.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class GossipNode extends GossipServiceGrpc.GossipServiceImplBase {
    private static final Logger logger = Logger.getLogger(GossipNode.class.getName());
    private static final SecureRandom random = new SecureRandom();

    private final String nodeId;
    private final String address;
    private final Server grpcServer;
    private final UserRegistry registry = new UserRegistry();

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

    public GossipNode(String host, int port) {
        this.nodeId = generateId();
        this.address = host + ":" + port;
        this.fanout = 3; // Send to 3 random peers each round
        this.maxHops = 5; // Maximum of 5 hops for a message
        this.gossipIntervalMs = 5000; // Gossip every 5 seconds

        // Create and start gRPC server
        this.grpcServer = ServerBuilder.forPort(port)
                .addService(this)
                .build();
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getAddress() {
        return address;
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
                GossipNode.this.stop();
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
            channel.shutdownNow();
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
        User user = registry.createUser(userId, name, email, nodeId);

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
            registry.addUser(user);
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

        // Build state vector entries from the request
        Map<String, Map<String, Long>> requesterDigest = new HashMap<>();
        for (Map.Entry<String, StateVectorEntry> entry : request.getStateDigestMap().entrySet()) {
            requesterDigest.put(entry.getKey(), entry.getValue().getVersionVectorMap());
        }

        // Check what users we have that requester is missing or has old versions
        for (User user : registry.getAllUsers()) {
            String userId = user.getId();
            Map<String, Long> requesterVector = requesterDigest.get(userId);

            if (requesterVector == null) {
                // Requester doesn't have this user at all
                missingUsers.add(user);
            } else {
                // Compare version vectors
                int comparison = UserRegistry.compareVersionVectors(
                        requesterVector, user.getVersionVectorMap());

                switch (comparison) {
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

    // Periodically gossip all users
    private void gossipRoutine() {
        Collection<User> users = registry.getAllUsers();
        if (users.isEmpty() || peers.isEmpty()) {
            return;
        }

        // Create gossip message with all users
        GossipMessage.Builder messageBuilder = GossipMessage.newBuilder()
                .setMessageId(generateId())
                .setSourceNodeId(nodeId)
                .setHopCount(0);

        // Add all users
        messageBuilder.addAllUsers(users);

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
            ManagedChannel channel = null;
            try {
                channel = ManagedChannelBuilder.forTarget(peerAddress)
                        .usePlaintext()
                        .build();

                GossipServiceGrpc.GossipServiceBlockingStub stub =
                        GossipServiceGrpc.newBlockingStub(channel)
                                .withDeadlineAfter(5, TimeUnit.SECONDS);

                stub.receiveGossip(message);
            } catch (Exception e) {
                logger.warning("Failed to send gossip to " + peerAddress + ": " + e.getMessage());
            } finally {
                if (channel != null) {
                    channel.shutdownNow();
                }
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
        Map<String, Map<String, Long>> stateDigest = registry.createStateDigest();

        // Convert to protobuf format
        Map<String, StateVectorEntry> protoDigest = new HashMap<>();
        for (Map.Entry<String, Map<String, Long>> entry : stateDigest.entrySet()) {
            StateVectorEntry vectorEntry = StateVectorEntry.newBuilder()
                    .putAllVersionVector(entry.getValue())
                    .build();

            protoDigest.put(entry.getKey(), vectorEntry);
        }

        // Create sync request
        SyncRequest request = SyncRequest.newBuilder()
                .setNodeId(nodeId)
                .putAllStateDigest(protoDigest)
                .build();

        // Send sync request
        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder.forTarget(peerAddress)
                    .usePlaintext()
                    .build();

            GossipServiceGrpc.GossipServiceBlockingStub stub =
                    GossipServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(5, TimeUnit.SECONDS);

            SyncResponse response = stub.sync(request);

            // Process missing users
            for (User user : response.getMissingUsersList()) {
                registry.addUser(user);
            }
        } catch (Exception e) {
            logger.warning("Failed to sync with " + peerAddress + ": " + e.getMessage());
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
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
        return registry.getAllUsers();
    }
}
