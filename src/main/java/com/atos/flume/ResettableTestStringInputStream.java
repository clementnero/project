package com.atos.flume;

import java.io.IOException;

import org.apache.flume.serialization.ResettableInputStream;

public class ResettableTestStringInputStream extends ResettableInputStream {

    private String str;
    int markPos = 0;
    int curPos = 0;

    /**
     * Warning: This test class does not handle character/byte conversion at all!
     * @param str String to use for testing
     */
    public ResettableTestStringInputStream(String str) {
        this.str = str;
    }

    @Override
    public int readChar() throws IOException {
        if (curPos >= str.length()) {
            return -1;
        }
        return str.charAt(curPos++);
    }

    @Override
    public void mark() throws IOException {
        markPos = curPos;
    }

    @Override
    public void reset() throws IOException {
        curPos = markPos;
    }

    @Override
    public void seek(long position) throws IOException {
        curPos = (int)position;
    }

    @Override
    public long tell() throws IOException {
        return curPos;
    }

    @Override
    public int read() throws IOException {
        throw new UnsupportedOperationException("This test class doesn't return " +
                "bytes!");
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        throw new UnsupportedOperationException("This test class doesn't return " +
                "bytes!");
    }

    @Override
    public void close() throws IOException {
        // no-op
    }
}