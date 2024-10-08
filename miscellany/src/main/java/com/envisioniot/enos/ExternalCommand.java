package com.envisioniot.enos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;

public class ExternalCommand {
    public static final String MODULE = ExternalCommand.class.getName();
    public static final Logger LOG = LoggerFactory.getLogger(MODULE);


    private final ProcessBuilder processBuilder;

    private Process process;
    private InputReader out;
    private InputReader err;

    public ExternalCommand(ProcessBuilder processBuilder) {
        this.processBuilder = processBuilder;
    }

    public void start() throws IOException {
        process = processBuilder.start();
        out = new InputReader(new BufferedInputStream(process.getInputStream()));
        err = new InputReader(new BufferedInputStream(process.getErrorStream()));

        out.start();
        err.start();
    }

    public int waitFor() throws InterruptedException {
        int exitCode = process.waitFor();
        out.join();
        err.join();
        return exitCode;
    }

    public byte[] getOutput() throws InterruptedException {
        waitFor();
        return out.getOutput();
    }

    public byte[] getError() throws InterruptedException {
        waitFor();
        return err.getOutput();
    }

    public String getOutputAsString(Charset encoding) throws InterruptedException {
        return new String(getOutput(),encoding);
    }

    public String getErrorAsString(Charset encoding) throws InterruptedException {
        return new String(getError(), encoding);
    }

    public void destroy() {
        process.destroy();
    }

    private static class InputReader extends Thread {
        private static final int BUFFER_SIZE = 2048;

        private final InputStream in;
        private final ByteArrayOutputStream out;
        private boolean running = false;

        InputReader(InputStream in) {
            this.in = in;
            out = new ByteArrayOutputStream();
        }

        @Override
        public void run() {
            running = true;

            byte[] buf = new byte[BUFFER_SIZE];
            int n = 0;
            try {
                while ((n = in.read(buf)) != -1)
                    out.write(buf, 0, n);
            } catch (IOException e) {
                LOG.error("error while reading external command", e);
            }

            running = false;
        }

        public byte[] getOutput() {
            if (running)
                throw new IllegalStateException("wait for process to be completed");

            return out.toByteArray();
        }
    }

}
