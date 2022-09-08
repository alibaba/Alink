package com.alibaba.alink.common.pyrunner;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

public class TarFileUtil {
    private static final Logger LOG = LoggerFactory.getLogger(TarFileUtil.class);

    private static void safeMakeDir(File dir) throws IOException {
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException("Failed to create directory: " + dir.toString());
            }
        }
    }

    public static void unTar(File inFile, File untarDir) throws IOException {
        safeMakeDir(untarDir);
        boolean gzipped = inFile.toString().endsWith("gz");
        unTarUsingJava(inFile, untarDir, gzipped);
    }

    public static void unTar(File inFile, File untarDir, boolean gzipped) throws IOException {
        safeMakeDir(untarDir);
        unTarUsingJava(inFile, untarDir, gzipped);
    }

    public static void unTar(InputStream in, File untarDir, boolean gzipped) throws IOException {
        safeMakeDir(untarDir);
        final String base = untarDir.getCanonicalPath();
        InputStream inputStream = null;
        try {
            if (gzipped) {
                inputStream = new BufferedInputStream(new GZIPInputStream(in));
            } else {
                inputStream = new BufferedInputStream(in);
            }
            List<Path[]> cachedSymbolicLinks = new ArrayList<>();
            try (TarArchiveInputStream tis = new TarArchiveInputStream(inputStream)) {
                for (TarArchiveEntry entry = tis.getNextTarEntry(); entry != null; ) {
                    unpackEntries(tis, entry, untarDir, base, cachedSymbolicLinks);
                    entry = tis.getNextTarEntry();
                }
            }
            List<Path[]> remainLinks = new ArrayList<>();
            while(!cachedSymbolicLinks.isEmpty() && cachedSymbolicLinks.size() != remainLinks.size()) {
                remainLinks.clear();
                for(Path[] x: cachedSymbolicLinks) {
                    if (Files.exists(x[0].getParent().resolve(x[1]))) {
                        Files.createSymbolicLink(x[0], x[1]);
                    } else {
                        remainLinks.add(x);
                    }
                }
                //System.out.println("delay count " + remainLinks.size());
                List<Path[]> tmp = cachedSymbolicLinks;
                cachedSymbolicLinks = remainLinks;
                remainLinks = tmp;
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

    private static void unTarUsingJava(File inFile, File untarDir,
                                       boolean gzipped) throws IOException {
        try (InputStream in = new FileInputStream(inFile)) {
            unTar(in, untarDir, gzipped);
        }
    }

    private static void unpackEntries(TarArchiveInputStream tis,
                                      TarArchiveEntry entry, File outputDir, final String base,
                                      List<Path[]> cachedSymbolicLinks) throws IOException {
        File target = new File(outputDir, entry.getName());
        String found = target.getCanonicalPath();
        if (!found.startsWith(base)) {
            LOG.error("Invalid location {} is outside of {}", found, base);
            return;
        }
        if (entry.isDirectory()) {
            safeMakeDir(target);
            for (TarArchiveEntry e : entry.getDirectoryEntries()) {
                unpackEntries(tis, e, target, base, cachedSymbolicLinks);
            }
        } else if (entry.isSymbolicLink()) {
            Path src = target.toPath();
            if (!Files.isDirectory(src.getParent())) {
                src.getParent().toFile().mkdirs();
            }
            Path dest = Paths.get(entry.getLinkName());
            if (Files.exists(src.getParent().resolve(dest))) {
                Files.createSymbolicLink(src, dest);
            } else {
                cachedSymbolicLinks.add(new Path[] {src, dest});
            }
            //LOG.trace("Extracting sym link {} to {}", target, dest);
            //// Create symbolic link relative to tar parent dir
            //Files.createSymbolicLink(src, dest);
        } else if (entry.isFile()) {
            safeMakeDir(target.getParentFile());
            writeToFile(tis, target);
        } else {
            LOG.error("{} is not a currently supported tar entry type.", entry);
        }

        Path p = target.toPath();
        if (Files.exists(p)) {
            try {
                //We created it so lets chmod it properly
                int mode = entry.getMode();
                Files.setPosixFilePermissions(p, parsePerms(mode));
            } catch (UnsupportedOperationException e) {
                //Ignored the file system we are on does not support this, so don't do it.
            } catch (AkUnsupportedOperationException e){
                //Ignored the file system we are on does not support this, so don't do it.
            }
        }
    }

    static void writeToFile(InputStream in, File outFile) {
        try(FileOutputStream fos = new FileOutputStream(outFile)) {
            byte[] buf = new byte[64 * 1024];
            while(true) {
                int n = in.read(buf);
                if (n < 0) {
                    break;
                }
                if (n > 0) {
                    fos.write(buf, 0, n);
                }
            }
        } catch(IOException e) {
            throw new AkUnclassifiedErrorException("Error. ",e);
        }
    }


    private static Set<PosixFilePermission> parsePerms(int mode) {
        Set<PosixFilePermission> ret = new HashSet<>();
        if ((mode & 0001) > 0) {
            ret.add(PosixFilePermission.OTHERS_EXECUTE);
        }
        if ((mode & 0002) > 0) {
            ret.add(PosixFilePermission.OTHERS_WRITE);
        }
        if ((mode & 0004) > 0) {
            ret.add(PosixFilePermission.OTHERS_READ);
        }
        if ((mode & 0010) > 0) {
            ret.add(PosixFilePermission.GROUP_EXECUTE);
        }
        if ((mode & 0020) > 0) {
            ret.add(PosixFilePermission.GROUP_WRITE);
        }
        if ((mode & 0040) > 0) {
            ret.add(PosixFilePermission.GROUP_READ);
        }
        if ((mode & 0100) > 0) {
            ret.add(PosixFilePermission.OWNER_EXECUTE);
        }
        if ((mode & 0200) > 0) {
            ret.add(PosixFilePermission.OWNER_WRITE);
        }
        if ((mode & 0400) > 0) {
            ret.add(PosixFilePermission.OWNER_READ);
        }
        return ret;
    }
}
