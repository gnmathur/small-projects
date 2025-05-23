import java.net.*;
import java.io.*;

public class UDPServer {
    private static final int PORT = 9876;
    private static final int BUFFER_SIZE = 1024;

    public static void main(String[] args) {
        DatagramSocket socket = null;
        
        try {
            // Create a UDP socket bound to the specified port
            socket = new DatagramSocket(PORT);
            System.out.println("UDP Server is running on port " + PORT);
            
            byte[] buffer = new byte[BUFFER_SIZE];
            
            while (true) {
                // Create a datagram packet to receive data
                DatagramPacket receivePacket = new DatagramPacket(buffer, buffer.length);
                
                // Receive data from client
                socket.receive(receivePacket);
                
                // Extract the message
                String message = new String(receivePacket.getData(), 0, receivePacket.getLength());
                
                // Get client information
                InetAddress clientAddress = receivePacket.getAddress();
                int clientPort = receivePacket.getPort();
                
                // Print the received message
                System.out.println("Received from " + clientAddress + ":" + clientPort + " - " + message);
            }
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