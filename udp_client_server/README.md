# UDP Client-Server Programs

This project contains a simple UDP server and two UDP client implementations in Java.

## Files

- `UDPServer.java` - UDP server that listens on port 9876 and prints received messages
- `UDPClient.java` - UDP client using connection-less datagram sockets
- `UDPClientWithConnect.java` - UDP client using connected datagram sockets

## Compilation

Compile all Java files:

```bash
javac UDPServer.java
javac UDPClient.java
javac UDPClientWithConnect.java
```

## Running the Programs

### 1. Start the UDP Server

Open a terminal and run:

```bash
java UDPServer
```

The server will start listening on port 9876 and display:
```
UDP Server is running on port 9876
```

### 2. Run the Connection-less UDP Client

In a new terminal, run:

```bash
java UDPClient
```

This will send a 128-byte message to the server. You should see:
- On the client: "Message sent to localhost:9876"
- On the server: The received message with client information

### 3. Run the Connected UDP Client

In a new terminal, run:

```bash
java UDPClientWithConnect
```

This client uses `socket.connect()` to establish a connection to the server before sending. The behavior is similar but uses a connected socket.

## Key Differences

1. **UDPClient.java**: Uses standard connection-less UDP where each packet must specify the destination address and port.

2. **UDPClientWithConnect.java**: Uses `socket.connect()` to "connect" to a specific remote address. After connecting:
   - Only packets from the connected address are received
   - `send()` can be called without specifying the destination
   - Provides some error checking (ICMP port unreachable errors)

## Notes

- Both clients send exactly 128 bytes as requested
- The server runs in an infinite loop, press Ctrl+C to stop it
- No external libraries are used, only Java standard library
- The server can handle messages from both client types