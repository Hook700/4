package il.ac.kinneret.SWSender;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.zip.CRC32;

public class SlidingWindowSender {

    private static InetAddress dest;
    private static int port;
    private static String filePath;
    private static int windowSize;
    private static int packetSize;
    private static int rtt;
    private static int timeout;

    // The list of packet numbers to "corrupt"
    private static List<Integer> dropList = new ArrayList<>();

    // Window and resources
    private static DatagramSocket socket;
    private static BufferedInputStream inputStream;
    private static ArrayBlockingQueue<SlidingWindowPacket> window;

    // We'll keep a global sequence number
    private static int globalSequence = 0;
    private static int base = 0; // Base of the window
    private static int nextSeqNum = 0; // Next sequence number to send

    public static void main(String[] args) {
        // 1) Parse arguments quickly
        if (!parseArguments(args)) {
            System.err.println("Usage:\n" +
                    "  java SlidingWindowSender " +
                    " -dest=<IP> -port=<Port> -f=<FilePath> " +
                    " -packetsize=<PacketSize> -sws=<WindowSize> " +
                    " -rtt=<RTT> [-droplist=1,2,3]");
            System.exit(1);
        }

        try {
            // 2) Initialize
            socket = new DatagramSocket();
            timeout = 15 * rtt;           // For example, 15*rtt
            socket.setSoTimeout(timeout);

            File file = new File(filePath);
            if (!file.exists() || !file.canRead()) {
                System.err.println("[Sender] Error: File does not exist or cannot be read.");
                System.exit(1);
            }

            inputStream = new BufferedInputStream(new FileInputStream(file));
            window = new ArrayBlockingQueue<>(windowSize);

            // 3) Perform the sliding-window send
            runSender();

        } catch (IOException e) {
            System.err.println("[Sender] Exception: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException ignore) {}
            }
            if (socket != null) socket.close();
        }
    }

    private static void runSender() throws IOException {
        // Pre-load the window
        SlidingWindowPacket nextPacket;
        while (window.remainingCapacity() > 0 && (nextPacket = readNextPacket()) != null) {
            window.add(nextPacket);
            sendPacket(nextPacket);
            nextSeqNum++;
        }

        // While there's still data in flight
        while (base < nextSeqNum) {
            try {
                // Wait for ACK
                DatagramPacket ackDp = receiveAck();
                int ackNum = parseAckNumber(ackDp);

                // Slide the window if the ACK matches (or exceeds) the earliest unACKed
                if (ackNum >= base) {
                    base = ackNum + 1; // Update base to the next unacknowledged packet
                    // Remove acknowledged packets from the window
                    while (!window.isEmpty() && window.peek().getPacketNum() < base) {
                        window.poll();
                    }
                }

                // Fill the window with more packets if possible
                while (window.remainingCapacity() > 0 && (nextPacket = readNextPacket()) != null) {
                    window.add(nextPacket);
                    sendPacket(nextPacket);
                    nextSeqNum++;
                }

            } catch (SocketTimeoutException ste) {
                System.out.println("[Sender] Timeout waiting for ACK, resending all in window...");
                // Resend all packets in the window
                for (SlidingWindowPacket packetToResend : window) {
                    sendPacket(packetToResend);
                }
            }
        }

        // Send final "EOF" packet
        SlidingWindowPacket finalPacket = createEOFPacket();
        sendPacket(finalPacket);
        System.out.println("[Sender] Sent final packet for " + filePath);

        // Optionally wait for final ACK
        try {
            DatagramPacket ackDp = receiveAck();
            int ackNum = parseAckNumber (ackDp);
            System.out.println("[Sender] Final ACK received for packet number: " + ackNum);
        } catch (SocketTimeoutException ste) {
            System.out.println("[Sender] Timeout waiting for final ACK.");
        }
    }

    private static SlidingWindowPacket readNextPacket() throws IOException {
        byte[] data = new byte[packetSize];
        int bytesRead = inputStream.read(data);
        if (bytesRead == -1) {
            return null; // End of file
        }
        // Calculate CRC
        CRC32 crc = new CRC32();
        crc.update(data, 0, bytesRead);
        long crcValue = crc.getValue();
        return new SlidingWindowPacket(globalSequence++, crcValue, Arrays.copyOf(data, bytesRead));
    }

    private static void sendPacket(SlidingWindowPacket packet) throws IOException {
        byte[] packetBytes = packet.toByteArray();
        DatagramPacket dp = new DatagramPacket(packetBytes, packetBytes.length, dest, port);
        socket.send(dp);
        System.out.println("[Sender] Sent packet number: " + packet.getPacketNum());
    }

    private static DatagramPacket receiveAck() throws IOException {
        byte[] ackBuffer = new byte[4]; // Assuming ACK is just an int
        DatagramPacket ackPacket = new DatagramPacket(ackBuffer, ackBuffer.length);
        socket.receive(ackPacket);
        return ackPacket;
    }

    private static int parseAckNumber(DatagramPacket ackPacket) {
        return ByteBuffer.wrap(ackPacket.getData()).getInt();
    }

    private static SlidingWindowPacket createEOFPacket() {
        // Create a special EOF packet
        return new SlidingWindowPacket(globalSequence++, 0, new byte[0]); // Empty data for EOF
    }

    private static boolean parseArguments(String[] args) {
        if (args == null || args.length < 6) return false; // Ensure enough arguments are provided
        for (String arg : args) {
            if (arg.startsWith("-dest=")) {
                try {
                    dest = InetAddress.getByName(arg.substring(6));
                } catch (UnknownHostException e) {
                    System.err.println("[Sender] Invalid destination IP: " + e.getMessage());
                    return false;
                }
            } else if (arg.startsWith("-port=")) {
                port = Integer.parseInt(arg.substring(6));
            } else if (arg.startsWith("-f=")) {
                filePath = arg.substring(3);
            } else if (arg.startsWith("-packetsize=")) {
                packetSize = Integer.parseInt(arg.substring(12));
            } else if (arg.startsWith("-sws=")) {
                windowSize = Integer.parseInt(arg.substring(5));
            } else if (arg.startsWith("-rtt=")) {
                rtt = Integer.parseInt(arg.substring(5));
            } else if (arg.startsWith("-droplist=")) {
                String[] drops = arg.substring(10).split(",");
                for (String drop : drops) {
                    dropList.add(Integer.parseInt(drop));
                }
            }
        }
        return (dest != null && port > 0 && filePath != null && packetSize > 0 && windowSize > 0 && rtt > 0);
    }

}