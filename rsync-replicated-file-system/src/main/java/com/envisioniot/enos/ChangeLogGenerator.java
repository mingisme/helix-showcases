package com.envisioniot.enos;

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;

import java.io.*;
import java.util.concurrent.locks.Lock;

public class ChangeLogGenerator extends FileAlterationListenerAdaptor {

    private final String directory;

    private int currentSeq;
    private int currentGen;

    private DataOutputStream out;
    private Lock lock;
    private int entriesLogged;

    public ChangeLogGenerator(String directory, int startGen, int startSeq) {
        this.directory = directory;
        this.currentSeq = startGen;
        this.currentGen = startSeq;
    }

    private void setLogFile() throws FileNotFoundException {
        File file = new File(directory);
        String[] list = file.list();
        if (list == null) {
            list = new String[]{};
        }
        int max = -1;
        for (String name : list) {
            String[] split = name.split("\\.");
            if (split.length == 2) {
                try {
                    int index = Integer.parseInt(split[1]);
                    if (index > max) {
                        max = index;
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Invalid transaction log file found:" + name);
                }
            }
        }
        String transLogFile = directory + "/" + "log." + (++max);
        System.out.println("Current file name:" + transLogFile);
        out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(transLogFile, true)));
    }


    @Override
    public void onFileChange(File file) {
        appendChange(file.getName(), ChangeType.MODIFY);
    }

    @Override
    public void onFileCreate(File file) {
        appendChange(file.getName(), ChangeType.CREATE);
    }

    @Override
    public void onFileDelete(File file) {
        appendChange(file.getName(), ChangeType.DELETE);
    }

    public boolean appendChange(String path, ChangeType type) {
        lock.lock();
        if (new File(path).isDirectory()) {
            return true;
        }
        try {
            ChangeRecord record = new ChangeRecord();
            record.file = path;
            record.timestamp = System.currentTimeMillis();
            currentSeq++;
            long txnId = (((long) currentGen) << 32) + ((long) currentSeq);
            record.txid = txnId;
            record.type = (short) type.ordinal();
            write(record);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            lock.unlock();
        }
        return true;
    }

    private void write(ChangeRecord record) throws Exception {
        out.writeLong(record.txid);
        out.writeShort(record.type);
        out.writeLong(record.timestamp);
        out.writeUTF(record.file);
        out.flush();
        entriesLogged++;
        if (entriesLogged == 10000) {
            entriesLogged = 0;
            out.close();
            setLogFile();
        }
    }

    enum ChangeType {
        CREATE,
        DELETE,
        MODIFY
    };
}
