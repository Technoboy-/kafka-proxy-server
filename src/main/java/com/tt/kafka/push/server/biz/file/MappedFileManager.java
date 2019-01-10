package com.tt.kafka.push.server.biz.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: Tboy
 */
public class MappedFileManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MappedFileManager.class);

    private final long mapedFileSize = 100 * 1024 * 1024; //100M

    private final String storePath;

    private final List<MappedFile> mappedFiles = new ArrayList<>();

    public MappedFileManager(String storePath){
        this.storePath = storePath;
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

}
