package com.envisioniot.enos;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ExternalCommandDemo {
    public static void main(String[] args) throws InterruptedException, IOException {
        String home = System.getProperty("user.home");
        FileUtils.write(new File(home, "/tmp/1"), "hello", StandardCharsets.UTF_8);
        FileUtils.createParentDirectories(new File(home, "/tmp/dir1"));

        //good
//        ProcessBuilder processBuilder = new ProcessBuilder("ls", "-l", home + "/tmp");

        //bad
        ProcessBuilder processBuilder = new ProcessBuilder("ls", "aa", home + "/tmp");

        ExternalCommand externalCommand = new ExternalCommand(processBuilder);
        externalCommand.start();
        int code = externalCommand.waitFor();
        if (code != 0) {
            System.out.println("command failed with code: " + code);
            System.out.println(externalCommand.getErrorAsString(StandardCharsets.UTF_8));
        } else {
            System.out.println(externalCommand.getOutputAsString(StandardCharsets.UTF_8));
        }
    }
}
