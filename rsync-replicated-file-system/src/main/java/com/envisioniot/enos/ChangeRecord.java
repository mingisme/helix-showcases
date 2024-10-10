package com.envisioniot.enos;

public class ChangeRecord {

    long txid;
    String file;
    long timestamp;
    short type;
    transient String changeLogFileName;
    transient long startOffset;
    transient long endOffset;

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(txid);
        sb.append("|");
        sb.append(timestamp);
        sb.append("|");
        sb.append(file);
        sb.append("|");
        sb.append(changeLogFileName);
        sb.append("|");
        sb.append(startOffset);
        sb.append("|");
        sb.append(endOffset);
        return sb.toString();
    }

    public static ChangeRecord fromString(String line) {
        ChangeRecord record = null;
        if (line != null) {
            String[] split = line.split("\\|");
            if (split.length == 6) {
                record = new ChangeRecord();
                record.txid = Long.parseLong(split[0]);
                record.timestamp = Long.parseLong(split[1]);
                record.file = split[2];
                record.changeLogFileName = split[3];
                record.startOffset = Long.parseLong(split[4]);
                record.endOffset = Long.parseLong(split[5]);
            }
        }
        return record;
    }
}
