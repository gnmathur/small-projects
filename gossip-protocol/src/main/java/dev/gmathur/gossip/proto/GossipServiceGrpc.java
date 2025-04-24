package dev.gmathur.gossip.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * Gossip service for inter-node communication
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.71.0)",
    comments = "Source: gossip.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class GossipServiceGrpc {

  private GossipServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "gossip.GossipService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<dev.gmathur.gossip.proto.GossipMessage,
      dev.gmathur.gossip.proto.Empty> getReceiveGossipMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReceiveGossip",
      requestType = dev.gmathur.gossip.proto.GossipMessage.class,
      responseType = dev.gmathur.gossip.proto.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<dev.gmathur.gossip.proto.GossipMessage,
      dev.gmathur.gossip.proto.Empty> getReceiveGossipMethod() {
    io.grpc.MethodDescriptor<dev.gmathur.gossip.proto.GossipMessage, dev.gmathur.gossip.proto.Empty> getReceiveGossipMethod;
    if ((getReceiveGossipMethod = GossipServiceGrpc.getReceiveGossipMethod) == null) {
      synchronized (GossipServiceGrpc.class) {
        if ((getReceiveGossipMethod = GossipServiceGrpc.getReceiveGossipMethod) == null) {
          GossipServiceGrpc.getReceiveGossipMethod = getReceiveGossipMethod =
              io.grpc.MethodDescriptor.<dev.gmathur.gossip.proto.GossipMessage, dev.gmathur.gossip.proto.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReceiveGossip"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dev.gmathur.gossip.proto.GossipMessage.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dev.gmathur.gossip.proto.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new GossipServiceMethodDescriptorSupplier("ReceiveGossip"))
              .build();
        }
      }
    }
    return getReceiveGossipMethod;
  }

  private static volatile io.grpc.MethodDescriptor<dev.gmathur.gossip.proto.SyncRequest,
      dev.gmathur.gossip.proto.SyncResponse> getSyncMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Sync",
      requestType = dev.gmathur.gossip.proto.SyncRequest.class,
      responseType = dev.gmathur.gossip.proto.SyncResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<dev.gmathur.gossip.proto.SyncRequest,
      dev.gmathur.gossip.proto.SyncResponse> getSyncMethod() {
    io.grpc.MethodDescriptor<dev.gmathur.gossip.proto.SyncRequest, dev.gmathur.gossip.proto.SyncResponse> getSyncMethod;
    if ((getSyncMethod = GossipServiceGrpc.getSyncMethod) == null) {
      synchronized (GossipServiceGrpc.class) {
        if ((getSyncMethod = GossipServiceGrpc.getSyncMethod) == null) {
          GossipServiceGrpc.getSyncMethod = getSyncMethod =
              io.grpc.MethodDescriptor.<dev.gmathur.gossip.proto.SyncRequest, dev.gmathur.gossip.proto.SyncResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Sync"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dev.gmathur.gossip.proto.SyncRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dev.gmathur.gossip.proto.SyncResponse.getDefaultInstance()))
              .setSchemaDescriptor(new GossipServiceMethodDescriptorSupplier("Sync"))
              .build();
        }
      }
    }
    return getSyncMethod;
  }

  private static volatile io.grpc.MethodDescriptor<dev.gmathur.gossip.proto.JoinRequest,
      dev.gmathur.gossip.proto.JoinResponse> getJoinMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Join",
      requestType = dev.gmathur.gossip.proto.JoinRequest.class,
      responseType = dev.gmathur.gossip.proto.JoinResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<dev.gmathur.gossip.proto.JoinRequest,
      dev.gmathur.gossip.proto.JoinResponse> getJoinMethod() {
    io.grpc.MethodDescriptor<dev.gmathur.gossip.proto.JoinRequest, dev.gmathur.gossip.proto.JoinResponse> getJoinMethod;
    if ((getJoinMethod = GossipServiceGrpc.getJoinMethod) == null) {
      synchronized (GossipServiceGrpc.class) {
        if ((getJoinMethod = GossipServiceGrpc.getJoinMethod) == null) {
          GossipServiceGrpc.getJoinMethod = getJoinMethod =
              io.grpc.MethodDescriptor.<dev.gmathur.gossip.proto.JoinRequest, dev.gmathur.gossip.proto.JoinResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Join"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dev.gmathur.gossip.proto.JoinRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dev.gmathur.gossip.proto.JoinResponse.getDefaultInstance()))
              .setSchemaDescriptor(new GossipServiceMethodDescriptorSupplier("Join"))
              .build();
        }
      }
    }
    return getJoinMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static GossipServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<GossipServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<GossipServiceStub>() {
        @java.lang.Override
        public GossipServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new GossipServiceStub(channel, callOptions);
        }
      };
    return GossipServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static GossipServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<GossipServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<GossipServiceBlockingV2Stub>() {
        @java.lang.Override
        public GossipServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new GossipServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return GossipServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static GossipServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<GossipServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<GossipServiceBlockingStub>() {
        @java.lang.Override
        public GossipServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new GossipServiceBlockingStub(channel, callOptions);
        }
      };
    return GossipServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static GossipServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<GossipServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<GossipServiceFutureStub>() {
        @java.lang.Override
        public GossipServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new GossipServiceFutureStub(channel, callOptions);
        }
      };
    return GossipServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Gossip service for inter-node communication
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Receive gossip updates from other nodes
     * </pre>
     */
    default void receiveGossip(dev.gmathur.gossip.proto.GossipMessage request,
        io.grpc.stub.StreamObserver<dev.gmathur.gossip.proto.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReceiveGossipMethod(), responseObserver);
    }

    /**
     * <pre>
     * Anti-entropy sync to resolve inconsistencies
     * </pre>
     */
    default void sync(dev.gmathur.gossip.proto.SyncRequest request,
        io.grpc.stub.StreamObserver<dev.gmathur.gossip.proto.SyncResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSyncMethod(), responseObserver);
    }

    /**
     * <pre>
     * Join the network by contacting an existing node
     * </pre>
     */
    default void join(dev.gmathur.gossip.proto.JoinRequest request,
        io.grpc.stub.StreamObserver<dev.gmathur.gossip.proto.JoinResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getJoinMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service GossipService.
   * <pre>
   * Gossip service for inter-node communication
   * </pre>
   */
  public static abstract class GossipServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return GossipServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service GossipService.
   * <pre>
   * Gossip service for inter-node communication
   * </pre>
   */
  public static final class GossipServiceStub
      extends io.grpc.stub.AbstractAsyncStub<GossipServiceStub> {
    private GossipServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GossipServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GossipServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Receive gossip updates from other nodes
     * </pre>
     */
    public void receiveGossip(dev.gmathur.gossip.proto.GossipMessage request,
        io.grpc.stub.StreamObserver<dev.gmathur.gossip.proto.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReceiveGossipMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Anti-entropy sync to resolve inconsistencies
     * </pre>
     */
    public void sync(dev.gmathur.gossip.proto.SyncRequest request,
        io.grpc.stub.StreamObserver<dev.gmathur.gossip.proto.SyncResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSyncMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Join the network by contacting an existing node
     * </pre>
     */
    public void join(dev.gmathur.gossip.proto.JoinRequest request,
        io.grpc.stub.StreamObserver<dev.gmathur.gossip.proto.JoinResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getJoinMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service GossipService.
   * <pre>
   * Gossip service for inter-node communication
   * </pre>
   */
  public static final class GossipServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<GossipServiceBlockingV2Stub> {
    private GossipServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GossipServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GossipServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Receive gossip updates from other nodes
     * </pre>
     */
    public dev.gmathur.gossip.proto.Empty receiveGossip(dev.gmathur.gossip.proto.GossipMessage request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReceiveGossipMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Anti-entropy sync to resolve inconsistencies
     * </pre>
     */
    public dev.gmathur.gossip.proto.SyncResponse sync(dev.gmathur.gossip.proto.SyncRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSyncMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Join the network by contacting an existing node
     * </pre>
     */
    public dev.gmathur.gossip.proto.JoinResponse join(dev.gmathur.gossip.proto.JoinRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getJoinMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service GossipService.
   * <pre>
   * Gossip service for inter-node communication
   * </pre>
   */
  public static final class GossipServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<GossipServiceBlockingStub> {
    private GossipServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GossipServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GossipServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Receive gossip updates from other nodes
     * </pre>
     */
    public dev.gmathur.gossip.proto.Empty receiveGossip(dev.gmathur.gossip.proto.GossipMessage request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReceiveGossipMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Anti-entropy sync to resolve inconsistencies
     * </pre>
     */
    public dev.gmathur.gossip.proto.SyncResponse sync(dev.gmathur.gossip.proto.SyncRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSyncMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Join the network by contacting an existing node
     * </pre>
     */
    public dev.gmathur.gossip.proto.JoinResponse join(dev.gmathur.gossip.proto.JoinRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getJoinMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service GossipService.
   * <pre>
   * Gossip service for inter-node communication
   * </pre>
   */
  public static final class GossipServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<GossipServiceFutureStub> {
    private GossipServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GossipServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GossipServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Receive gossip updates from other nodes
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<dev.gmathur.gossip.proto.Empty> receiveGossip(
        dev.gmathur.gossip.proto.GossipMessage request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReceiveGossipMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Anti-entropy sync to resolve inconsistencies
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<dev.gmathur.gossip.proto.SyncResponse> sync(
        dev.gmathur.gossip.proto.SyncRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSyncMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Join the network by contacting an existing node
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<dev.gmathur.gossip.proto.JoinResponse> join(
        dev.gmathur.gossip.proto.JoinRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getJoinMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_RECEIVE_GOSSIP = 0;
  private static final int METHODID_SYNC = 1;
  private static final int METHODID_JOIN = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_RECEIVE_GOSSIP:
          serviceImpl.receiveGossip((dev.gmathur.gossip.proto.GossipMessage) request,
              (io.grpc.stub.StreamObserver<dev.gmathur.gossip.proto.Empty>) responseObserver);
          break;
        case METHODID_SYNC:
          serviceImpl.sync((dev.gmathur.gossip.proto.SyncRequest) request,
              (io.grpc.stub.StreamObserver<dev.gmathur.gossip.proto.SyncResponse>) responseObserver);
          break;
        case METHODID_JOIN:
          serviceImpl.join((dev.gmathur.gossip.proto.JoinRequest) request,
              (io.grpc.stub.StreamObserver<dev.gmathur.gossip.proto.JoinResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getReceiveGossipMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              dev.gmathur.gossip.proto.GossipMessage,
              dev.gmathur.gossip.proto.Empty>(
                service, METHODID_RECEIVE_GOSSIP)))
        .addMethod(
          getSyncMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              dev.gmathur.gossip.proto.SyncRequest,
              dev.gmathur.gossip.proto.SyncResponse>(
                service, METHODID_SYNC)))
        .addMethod(
          getJoinMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              dev.gmathur.gossip.proto.JoinRequest,
              dev.gmathur.gossip.proto.JoinResponse>(
                service, METHODID_JOIN)))
        .build();
  }

  private static abstract class GossipServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    GossipServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return dev.gmathur.gossip.proto.GossipProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("GossipService");
    }
  }

  private static final class GossipServiceFileDescriptorSupplier
      extends GossipServiceBaseDescriptorSupplier {
    GossipServiceFileDescriptorSupplier() {}
  }

  private static final class GossipServiceMethodDescriptorSupplier
      extends GossipServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    GossipServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (GossipServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new GossipServiceFileDescriptorSupplier())
              .addMethod(getReceiveGossipMethod())
              .addMethod(getSyncMethod())
              .addMethod(getJoinMethod())
              .build();
        }
      }
    }
    return result;
  }
}
