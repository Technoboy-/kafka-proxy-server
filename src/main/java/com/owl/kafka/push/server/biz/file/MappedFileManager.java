package com.owl.kafka.push.server.biz.file;

import com.owl.kafka.client.transport.protocol.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: Tboy
 */
public class MappedFileManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MappedFileManager.class);

    private final long mapedFileSize = 100 * 1024 * 1024; //100M

    private final int maxMsgSize = 8 * 1024 * 1024; //8M

    private final String storePath;

    private final List<MappedFile> mappedFiles = new ArrayList<>();

    private final ByteBuffer store;

    private final ByteBuffer idStore;

    public MappedFileManager(String storePath){
        this.storePath = storePath;
        this.store = ByteBuffer.allocate(maxMsgSize);
        this.idStore = ByteBuffer.allocate(8 + 8);
    }

    public void load(){
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            Arrays.sort(files);
            for (File file : files) {
                if (file.length() != this.mapedFileSize) {
                    LOGGER.warn(file + " length not matched message store config value, ignore it");
                    continue;
                }
                try {
                    MappedFile mappedFile = new MappedFile(file.getPath(), mapedFileSize);
                    mappedFile.setWritePosition(this.mapedFileSize);
                    this.mappedFiles.add(mappedFile);
                    LOGGER.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    LOGGER.error("load file " + file + " error", e);
                }
            }
        }
    }

    private void recovery(){
        if(!mappedFiles.isEmpty()){

        }
    }

    public static String createMessageId(final ByteBuffer input, final ByteBuffer addr, final long offset) {
        input.flip();
        input.limit(8 + 8);

        input.put(addr);
        input.putLong(offset);

        return bytes2string(input.array());
    }

    public static String bytes2string(byte[] src) {
        StringBuilder sb = new StringBuilder();
        if (src == null || src.length <= 0) {
            return null;
        }
        for (int i = 0; i < src.length; i++) {
            int v = src[i] & 0xFF;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                sb.append(0);
            }
            sb.append(hv.toUpperCase());
        }
        return sb.toString();
    }

    public static byte[] string2bytes(String hexString) {
        if (hexString == null || hexString.equals("")) {
            return null;
        }
        hexString = hexString.toUpperCase();
        int length = hexString.length() / 2;
        char[] hexChars = hexString.toCharArray();
        byte[] d = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return d;
    }

    private static byte charToByte(char c) {
        return (byte) "0123456789ABCDEF".indexOf(c);
    }

    public void write(long fileFromOffset, ByteBuffer byteBuffer, Packet packet){
        long wroteOffset = fileFromOffset + byteBuffer.position();
        String messageId = createMessageId(idStore, host(new InetSocketAddress(90)), wroteOffset);
        int length = calculate(packet);
        reset(store, length);
        store.put(packet.getVersion());
        store.put(packet.getCmd());
        store.putLong(packet.getMsgId());
        store.putInt(packet.getHeader().length);
        store.put(packet.getHeader());
        store.putInt(packet.getKey().length);
        store.put(packet.getKey());
        store.putInt(packet.getValue().length);
        store.put(packet.getValue());

    }

    public static ByteBuffer host(SocketAddress socketAddress) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        byteBuffer.put(inetSocketAddress.getAddress().getAddress());
        byteBuffer.putInt(inetSocketAddress.getPort());
        byteBuffer.flip();
        return byteBuffer;
    }

    private int calculate(Packet packet) {
        return 1 + 1 + 8 + 4 + packet.getHeader().length + 4 + packet.getKey().length + 4 + packet.getValue().length;
    }

    private void reset(ByteBuffer buffer, int limit){
        buffer.flip();
        buffer.limit(limit);
    }
}
