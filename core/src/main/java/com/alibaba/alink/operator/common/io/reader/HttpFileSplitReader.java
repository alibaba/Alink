package com.alibaba.alink.operator.common.io.reader;

import com.alibaba.alink.operator.common.io.csv.CsvFileInputSplit;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Readers for reading part of a http file.
 */
public class HttpFileSplitReader implements FileSplitReader {

    private String path;
    private transient HttpURLConnection connection;
    private transient InputStream stream;

    public HttpFileSplitReader(String path) {
        this.path = path;
    }

    @Override
    public void open(CsvFileInputSplit split, long start, long end) throws IOException {
        assert start >= 0;
        URL url = new URL(path);
        this.connection = (HttpURLConnection) url.openConnection();
        this.connection.setDoInput(true);
        this.connection.setConnectTimeout(5000);
        this.connection.setReadTimeout(60000);
        this.connection.setRequestMethod("GET");
        this.connection.setRequestProperty("Range", String.format("bytes=%d-%d", start, end));
        this.connection.connect();
        this.stream = this.connection.getInputStream();
    }

    @Override
    public void close() throws IOException {
        if (this.stream != null) {
            this.stream.close();
        }
        if (this.connection != null) {
            this.connection.disconnect();
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return this.stream.read(b, off, len);
    }

    @Override
    public long getFileLength() {
        HttpURLConnection headerConnection = null;
        try {
            URL url = new URL(path);
            headerConnection = (HttpURLConnection) url.openConnection();
            headerConnection.setConnectTimeout(5000);
            headerConnection.setRequestMethod("HEAD");

            headerConnection.connect();
            long contentLength = headerConnection.getContentLengthLong();
            String acceptRanges = headerConnection.getHeaderField("Accept-Ranges");
            boolean splitable = acceptRanges != null && acceptRanges.equalsIgnoreCase("bytes");

            if (contentLength < 0) {
                throw new RuntimeException("The content length can't be determined.");
            }

            // If the http server does not accept ranges, then we quit the program.
            // This is because 'accept ranges' is required to achieve robustness (through re-connection),
            // and efficiency (through concurrent read).
            if (!splitable) {
                throw new RuntimeException("The http server does not support range reading.");
            }

            return contentLength;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (headerConnection != null) {
                headerConnection.disconnect();
            }
        }
    }
}
