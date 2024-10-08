package com.envisioniot.enos;

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

import java.io.File;

public class FileAlterationMonitorDemo {
    public static void main(String[] args) throws Exception {
        // Step 1: Define the directory to monitor
        File directoryToWatch = new File("/Users/ming.wang4/tmp");

        // Step 2: Create a FileAlterationObserver for the directory (this will monitor the directory)
        FileAlterationObserver observer = new FileAlterationObserver(directoryToWatch);

        // Step 3: Add a custom FileAlterationListenerAdaptor to handle file events
        observer.addListener(new FileAlterationListenerAdaptor() {

            @Override
            public void onFileCreate(File file) {
                System.out.println("File created: " + file.getName());
            }

            @Override
            public void onFileDelete(File file) {
                System.out.println("File deleted: " + file.getName());
            }

            @Override
            public void onFileChange(File file) {
                System.out.println("File modified: " + file.getName());
            }

            @Override
            public void onDirectoryCreate(File directory) {
                System.out.println("Directory created: " + directory.getName());
            }

            @Override
            public void onDirectoryDelete(File directory) {
                System.out.println("Directory deleted: " + directory.getName());
            }

            @Override
            public void onDirectoryChange(File directory) {
                System.out.println("Directory modified: " + directory.getName());
            }
        });

        // Step 4: Create a FileAlterationMonitor with a polling interval (in milliseconds)
        long pollingInterval = 2000;
        FileAlterationMonitor monitor = new FileAlterationMonitor(pollingInterval);

        // Step 5: Add the observer to the monitor
        monitor.addObserver(observer);

        // Step 6: Start monitoring the directory
        monitor.start();
        System.out.println("Monitoring directory: " + directoryToWatch.getAbsolutePath());

        // Step 7: Stop the monitor
        System.in.read();
        monitor.stop();
    }
}
