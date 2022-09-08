package com.alibaba.alink.common.dl.utils;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.flink.ml.util.ShellExec;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;


public class ZipFileUtil {

    final static Logger LOG = LoggerFactory.getLogger(ZipFileUtil.class);

    public static void unZip(File inFile, File targetDir) throws IOException {
        if (new File("/usr/bin/unzip").canExecute()) {
            System.out.println("unzipping with /usr/bin/unzip");
            boolean ok = ShellExec.run(String.format("/usr/bin/unzip -q -d %s %s", targetDir.getPath(), inFile.getPath()), LOG::info);
            AkPreconditions.checkState(ok, "Fail to unzip " + inFile.getPath() + " to " + targetDir.getPath());
            System.out.println("unzip done.");
            return;
        }

        System.out.println("unzipping using java");
        try (ZipFile file = new ZipFile(inFile.getPath())) {
            FileSystem fileSystem = FileSystems.getDefault();
            //Get file entries
            Enumeration<? extends ZipEntry> entries = file.entries();

            //We will unzip files in this folder
            final String uncompressedDirectory = targetDir.getPath();
            AkPreconditions.checkArgument(targetDir.isDirectory());

            //Iterate over entries
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                //If directory then create a new directory in uncompressed folder
                if (entry.isDirectory()) {
                    Files.createDirectories(fileSystem.getPath(uncompressedDirectory + File.separator + entry.getName()));
                }
                //Else create the file
                else {
                    InputStream is = file.getInputStream(entry);
                    String uncompressedFileName = uncompressedDirectory + File.separator + entry.getName();
                    org.apache.commons.io.FileUtils.copyInputStreamToFile(is, new File(uncompressedFileName));
                    is.close();
                }
            }
        }
    }

    private static void safeMakeDir(String dirName) {
        File dir = new File(dirName);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    public static void unzipStreamToDirectory(InputStream rawIn, String dstDirName) throws IOException {
        safeMakeDir(dstDirName);
        File dstDir = new File(dstDirName);
        ZipInputStream zin = new ZipInputStream(rawIn);
        ZipEntry ze = zin.getNextEntry();

        while(ze != null) {
            String fileName = ze.getName();
            if (ze.isDirectory()) {
                new File(dstDir.getAbsolutePath() + File.separator + fileName).mkdirs();
            } else {
                File newFile = new File(dstDir.getAbsolutePath() + File.separator + fileName);
                File ppFile = new File(newFile.getParent());
                ppFile.mkdirs();
                FileOutputStream fos = new FileOutputStream(newFile);
                IOUtils.copyLarge(zin, fos);
                IOUtils.closeQuietly(fos);
            }
            ze = zin.getNextEntry();
        }
        zin.closeEntry();
        zin.close();
    }

    public static void unzipFileIntoDirectory(File srcFile, File dstDir) throws IOException {
        try {
            unzipFileIntoDirectory(srcFile.getAbsolutePath(), dstDir.getAbsolutePath());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AkUnclassifiedErrorException("Error. ",e);
        }
    }

    public static void unzipFileIntoDirectory(String srcZipFile, String dstDirName) throws IOException, InterruptedException {
        safeMakeDir(dstDirName);
        ProcessBuilder pb = new ProcessBuilder(Arrays.asList(
            "unzip", "-o", "-q",
            new File(srcZipFile).getAbsolutePath(),
            "-d", new File(dstDirName).getAbsolutePath()));
        int code;
        try {
            code = pb.start().waitFor();
        } catch (IOException e) {
            LOG.warn("Failed to use unzip command to decompress file {}, fallback to use Java library.", srcZipFile, e);
            code = 1;
        }
        if (code != 0) {
            unzipStreamToDirectory(new FileInputStream(srcZipFile), dstDirName);
        }
    }
}
