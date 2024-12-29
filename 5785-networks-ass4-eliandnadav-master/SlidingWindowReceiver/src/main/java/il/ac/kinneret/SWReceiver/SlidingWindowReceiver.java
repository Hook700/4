package il.ac.kinneret.SWReceiver;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

public class SlidingWindowReceiver {

    private static String ip;
    private static int port;
    private static String outFile;
    private static int rws; // receiver window size

    private static int LFR = -1; // Last Frame Received
    private static int LAF; // Last Acceptable Frame

    private static Map<Integer, byte[]> bufferMap = new HashMap<>();

    public static void main(String[] args) {
        if (!parseArguments(args)) {
            System.err.println("Usage:\n  java SlidingWindowReceiver " +
                    " -ip=<IP> -port=<Port> -outfile=<OutputFile> -rws=<WindowSize>");
            return;
        }

        DatagramSocket socket = null;
        BufferedOutputStream bos = null;
        try {
            InetAddress address = InetAddress.getByName(ip);
            socket = new DatagramSocket(port, address);
            bos = new BufferedOutputStream(new FileOutputStream(outFile));
            LAF = LFR + rws;

            System.out.println("[Receiver] Listening on " + ip + ":" + port);

            while (true) {
                byte[] receiveData = new byte[4096];
                DatagramPacket dp = new DatagramPacket(receiveData, receiveData.length);
                socket.receive(dp);

                SlidingWindowPacket packet = new SlidingWindowPacket(Arrays.copyOf(dp.getData(), dp.getLength()));
                int seqNum = packet.getPacketNum();
                byte[] data = packet.getPacketData();

                boolean isEOF = (data.length == 1 && data[0] == -1);

                if (isEOF && validateCRC(packet)) {
                    LFR++;
                    sendAck(socket, dp.getAddress(), dp.getPort(), LFR);
                    System.out.println("[Receiver] Got EOF. Acked " + LFR);
                    break;
                }

                if (!validateCRC(packet)) {
                    System.out.printf("[Receiver] Packet %d has invalid CRC\n", seqNum);
                    sendAck(socket, dp.getAddress(), dp.getPort(), LFR); // Re-ACK last good frame
                    continue;
                }

                if (seqNum > LFR && seqNum <= LAF) {
                    bufferMap.put(seqNum, data);
                    System.out.printf("[Receiver] Received packet %d (valid CRC) within window.\n", seqNum);

                    while (bufferMap.containsKey(LFR + 1)) {
                        LFR++;
                        byte[] consecutiveData = bufferMap.remove(LFR);
                        bos.write(consecutiveData);
                        sendAck(socket, dp.getAddress(), dp.getPort(), LFR);
                        System.out.println("Acked: " + LFR);
                    }

                    LAF = LFR + rws; // Update LAF
                } else {
                    System.out.printf("[Receiver] Packet %d outside window (LFR=%d, LAF=%d)\n", seqNum, LFR, LAF);
                    sendAck(socket, dp.getAddress(), dp.getPort(), LFR); // Re-ACK last good frame
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bos != null) {
                try { bos.close(); } catch (IOException ignore) {}
            }
            if (socket != null) {
                socket.close();
            }
        }
    }

    private static boolean parseArguments(String[] args) {
        if (args == null || args.length < 4) return false;
        for (String arg : args) {
            if (arg.startsWith("-ip=")) {
                ip = arg.substring(4);
            } else if (arg.startsWith("-port=")) {
                port = Integer.parseInt(arg.substring(6));
            } else if (arg.startsWith("-outfile=")) {
                outFile = arg.substring(9);
            } else if (arg.startsWith("-rws=")) {
                rws = Integer.parseInt(arg.substring(5));
            }
        }
        return (ip != null && port > 0 && outFile != null && rws > 0);
    }

    private static boolean validateCRC(SlidingWindowPacket packet) {
        CRC32 crc32 = new CRC32();
        crc32.update(packet.getPacketData());
        return (int) crc32.getValue() == (int) packet.getCrcValue();
    }

    private static void sendAck(DatagramSocket socket, InetAddress address, int port, int seqNum) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(seqNum);
        byte[] ackData = bb.array();
        DatagramPacket dp = new DatagramPacket(ackData, ackData.length, address, port);
        socket.send(dp);
    }
}