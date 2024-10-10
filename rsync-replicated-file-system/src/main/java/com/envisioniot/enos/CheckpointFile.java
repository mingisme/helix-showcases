package com.envisioniot.enos;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class CheckpointFile {

    private static final String TEMP = ".bak";
    private static final String CHECKPOINT_FILE_NAME = "checkpoint.dat";

    private String dirPath;

    public CheckpointFile(String dirPath) {
        this.dirPath = dirPath;
        File file = new File(dirPath);
        if (!file.exists() && !file.mkdirs()) {
            throw new RuntimeException("create checkpoint dir fail");
        }
    }

    public void checkpoint(ChangeRecord lastRecordProcessed) throws IOException {
        File tempFile = new File(dirPath, CHECKPOINT_FILE_NAME + TEMP);
        if(tempFile.exists() && !tempFile.delete()){
            throw new RuntimeException("delete temp checkpoint file fail");
        }

        String checkpointFileName = dirPath + File.separator + CHECKPOINT_FILE_NAME;
        File checkpointFile = new File(checkpointFileName);
        if(checkpointFile.exists() && !checkpointFile.renameTo(tempFile)){
            System.err.println("unable to backup checkpoint file");
        }
        if(!checkpointFile.createNewFile()){
            System.err.println("unable to create new checkpoint file");
        }
        FileUtils.writeStringToFile(checkpointFile, lastRecordProcessed.toString(), StandardCharsets.UTF_8);
    }

    public ChangeRecord getCheckpoint() throws IOException {
        File checkpointFile = new File(dirPath, CHECKPOINT_FILE_NAME);
        if(!checkpointFile.exists()){
            return null;
        }
        String content = FileUtils.readFileToString(checkpointFile, StandardCharsets.UTF_8);
        return ChangeRecord.fromString(content);
    }


}
