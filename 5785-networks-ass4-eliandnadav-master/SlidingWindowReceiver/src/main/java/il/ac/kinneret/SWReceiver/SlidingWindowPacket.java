package il.ac.kinneret.SWReceiver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represents a packet for sending via UDP in a sliding window protocol with a CRC32 checksum.  Note that the CRC checksum produced
 * by Crc32 in Java is 4 bytes, but is returned as a long.  To ensure compatibility, the methods here take and return longs, even
 * though internally the CRC32 value is sent as a 4 byte integer.
 *
 * @author Michael J. May
 * @version 1.0
 * @see java.util.zip.CRC32
 */
public class SlidingWindowPacket {
    /**
     * The packet number - can wrap if you handle the wrapping case
     */
    private int packetNum;
    /**
     * The bytes of the packet - the body
     */
    private byte[] packetData;
    /**
     * The CRC value calculated externally.
     */
    private long crcValue;

    /**
     * Create a new packet with the CRC value, packet data, and packet number.  Note that the CRC value must be provided externally.
     * @param crcValue The CRC value to send
     * @param packetData The data to send in the packet, its body
     * @param packetNum The number of the packet
     */
    public SlidingWindowPacket(int packetNum, long crcValue, byte[] packetData) {
        this.crcValue = crcValue;
        this.packetData = packetData;
        this.packetNum = packetNum;
    }

    /**
     * Creates a new packet based on a provided raw packet body. The packet is assumed to be in the format produced by this class - packet number
     * (int), CRC value (4 bytes), packet body.
     * @param rawBytes The packet raw body.  Assumed to be in the correct format
     */
    public SlidingWindowPacket(byte[] rawBytes) {
        byte[] packetNumberRaw = Arrays.copyOf(rawBytes, 4);
        byte[] crcRaw = Arrays.copyOfRange(rawBytes, 4, 8);
        packetData = Arrays.copyOfRange(rawBytes, 8, rawBytes.length);

        packetNum = ByteBuffer.wrap(packetNumberRaw).getInt();
        crcValue = ByteBuffer.wrap(crcRaw).getInt();
    }

    /**
     * Converts the packet to a byte array that can be sent in a UDP packet body
     * @return The packet's contents in a byte array in the following format: packet number
     * (int), CRC value (4 bytes), packet body.
     */
    public byte[] toByteArray() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            baos.writeBytes(ByteBuffer.allocate(4).putInt(packetNum).array());
            baos.write(ByteBuffer.allocate(4).putInt((int)crcValue).array());
            baos.write(packetData);
            return baos.toByteArray();
        } catch (IOException e) {
            System.out.println("Error converting packet to byte array: " + e.getMessage());
            return new byte[0];
        }
    }

    /**
     * Gets the CRC value for the packet
     * @return The CRC value
     */
    public long getCrcValue() {
        return crcValue;
    }

    /**
     * Gets the packet body
     * @return The packet's body
     */
    public byte[] getPacketData() {
        return packetData;
    }

    /**
     * Gets the packet number
     * @return The packet number
     */
    public int getPacketNum() {
        return packetNum;
    }
}
