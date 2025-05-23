import java.net.*;
import java.io.*;

public class UDPClient {
    private static final String SERVER_HOST = "gurukul";
    private static final int SERVER_PORT = 9876;
    private static final int MESSAGE_SIZE = 128;

    public static void main(String[] args) {
        DatagramSocket socket = null;
        
        try {
            // Create a UDP socket (connection-less)
            socket = new DatagramSocket();
            
            // Create a 128-byte message
            String message = "Hello from UDP Client! This is a test message.";
            // Pad the message to exactly 128 bytes
            byte[] messageBytes = new byte[MESSAGE_SIZE];
            byte[] tempBytes = message.getBytes();
            System.arraycopy(tempBytes, 0, messageBytes, 0, Math.min(tempBytes.length, MESSAGE_SIZE));
            
            // Get server address
            InetAddress serverAddress = InetAddress.getByName(SERVER_HOST);
            
            // Create datagram packet with the message
            DatagramPacket sendPacket = new DatagramPacket(
                messageBytes, 
                messageBytes.length, 
                serverAddress, 
                SERVER_PORT
            );
            
            // Send the packet
            socket.send(sendPacket);
            System.out.println("Message sent to " + SERVER_HOST + ":" + SERVER_PORT);
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
