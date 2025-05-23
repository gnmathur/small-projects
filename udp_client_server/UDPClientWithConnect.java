import java.net.*;
import java.io.*;

public class UDPClientWithConnect {
    private static final String SERVER_HOST = "localhost";
    private static final int SERVER_PORT = 9876;
    private static final int MESSAGE_SIZE = 128;

    public static void main(String[] args) {
        DatagramSocket socket = null;
        
        try {
            // Create a UDP socket
            socket = new DatagramSocket();
            
            // Connect the socket to the server (using UDP connect)
            InetAddress serverAddress = InetAddress.getByName(SERVER_HOST);
            socket.connect(serverAddress, SERVER_PORT);
            System.out.println("Connected to " + SERVER_HOST + ":" + SERVER_PORT);
            
            // Create a 128-byte message
            String message = "Hello from UDP Client with Connect! This message uses connected UDP.";
            // Pad the message to exactly 128 bytes
            byte[] messageBytes = new byte[MESSAGE_SIZE];
            byte[] tempBytes = message.getBytes();
            System.arraycopy(tempBytes, 0, messageBytes, 0, Math.min(tempBytes.length, MESSAGE_SIZE));
            
            // Create datagram packet (no need to specify address/port since socket is connected)
            DatagramPacket sendPacket = new DatagramPacket(messageBytes, messageBytes.length);
            
            // Send the packet using the connected socket
            socket.send(sendPacket);
            System.out.println("Message sent using connected UDP socket");
            System.out.println("Message length: " + messageBytes.length + " bytes");
            
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
    }
}