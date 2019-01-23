package com.owl.kafka.proxy.server.biz.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author: Tboy
 */
public class MappedFile {

    private static final Logger LOGGER = LoggerFactory.getLogger(MappedFile.class);

    private final String fileName;

    private final long fileSize;

    private final File file;

    private FileChannel fileChannel;

    private MappedByteBuffer mappedByteBuffer;

    private final AtomicLong writePosition = new AtomicLong(0);

    private final long fileFromOffset;

    public MappedFile(final String fileName, final long fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(fileName);
        init();
    }

    private void init() throws IOException {
        boolean init = false;
        mkDirs(this.file.getParent());
        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            init = true;
        } catch (FileNotFoundException e) {
            LOGGER.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            LOGGER.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!init && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public boolean append(final byte[] data){
        final long currentPos = this.writePosition.get();
        if(currentPos + data.length <= this.fileSize){
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
                this.mappedByteBuffer.force();
            } catch (Throwable ex){
                LOGGER.error("Error occurred when append message to mappedFile.", ex);
            }
            this.writePosition.addAndGet(data.length);
            return true;
        }
        return false;
    }

    public ByteBuffer select(int pos, int size){
        if(pos + size <= this.writePosition.get()){
            ByteBuffer positionBuffer = this.mappedByteBuffer.slice();
            positionBuffer.position(pos);
            ByteBuffer sizeBuffer = positionBuffer.slice();
            sizeBuffer.limit(size);
            return sizeBuffer;
        }
        return null;
    }

    public void close(){
        try {
            clean(this.mappedByteBuffer);
            this.fileChannel.close();
        } catch (Exception ex) {
            LOGGER.error("Error occurred when close channel.", ex);
        }
    }

    public void setWritePosition(long position) {
        writePosition.set(position);
    }

    private static void mkDirs(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                LOGGER.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    private static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }
        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }
}
