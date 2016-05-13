/*
 * Copyright (c) 2014-2015 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.vmware.xenon.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class FileUtils {

    public static class ContentRange {
        long start;
        long end;
        long fileSize;

        public static final int CHUNK_SIZE = 512 * 1024;
        public static final int MAX_IN_FLIGHT_CHUNKS = 10;

        // Content-Range: bytes 21010-47021/47022
        private static final String CONTENT_RANGE_PATTERN = " (\\d*)[-](\\d*)[/](\\d*)";
        private static final Pattern contentRangePattern = Pattern.compile(CONTENT_RANGE_PATTERN);

        // "Range: bytes=0-2048"
        private static final String RANGE_PATTERN = "=(\\d*)[-](\\d*)";
        private static final Pattern rangePattern = Pattern.compile(RANGE_PATTERN);

        ContentRange() {
            this.start = 0;
            this.end = CHUNK_SIZE;
            this.fileSize = 0;
        }

        ContentRange(String headerString) {
            if (headerString == null || headerString.isEmpty()) {
                return;
            }
            Matcher m = contentRangePattern.matcher(headerString);
            if (!m.find() || (m.groupCount() != 3)) {
                throw new IllegalArgumentException("no content-range field found");
            }

            this.start = Long.parseLong(m.group(1));
            this.end = Long.parseLong(m.group(2));
            this.fileSize = Long.parseLong(m.group(3));
        }

        ContentRange(int start, int end, int fileSize) {
            this.start = start;
            this.end = Integer.min(end, fileSize);
            this.fileSize = fileSize;
        }

        ContentRange(int fileSize) {
            this.start = 0;
            this.end = Integer.min(fileSize, CHUNK_SIZE);
            this.fileSize = fileSize;
        }

        public static ContentRange fromRangeHeader(String headerString, long fileSize) {
            if (headerString == null || headerString.isEmpty()) {
                throw new IllegalArgumentException("no header set");
            }

            Matcher m = rangePattern.matcher(headerString);
            if (!m.find() || (m.groupCount() != 2)) {
                throw new IllegalArgumentException("no range field found");
            }

            ContentRange r = new ContentRange();
            r.fileSize = fileSize;
            r.start = Integer.parseInt(m.group(1));
            r.end = Long.min(Integer.parseInt(m.group(2)), fileSize);
            Logger.getAnonymousLogger().log(Level.INFO,
                    String.format("header=%s start=%d end=%d size=%d",
                            headerString, r.start,
                            r.end,
                            r.fileSize));
            return r;
        }

        public ContentRange nextChunk() {
            return new ContentRange((int) this.end,
                    (int) this.end + CHUNK_SIZE,
                    (int) this.fileSize);
        }

        public boolean isDone() {
            return this.end >= this.fileSize;
        }

        public String toContentRangeHeader() {
            return String.format("%s: bytes %d-%d/%d",
                    Operation.CONTENT_RANGE_HEADER, this.start, this.end,
                    this.fileSize);
        }

        public String toRangeHeader() {
            return String.format("%s: bytes=%d-%d",
                    Operation.RANGE_HEADER, this.start, this.end);
        }
    }

    /*
     * Finds a resource in a specified search path.
     */
    public static URL findResource(List<Path> searchPath, Path resource)
            throws MalformedURLException {
        for (Path p : searchPath) {
            URI u = p.resolve(resource).toUri();
            File f = new File(p.resolve(resource).toString());
            if (f.exists()) {
                return u.toURL();
            }
        }

        return (FileUtils.class).getClassLoader().getResource(resource.toString());
    }

    public static class ResourceEntry {
        public URL url;
        public Path suffix;
    }

    public static List<ResourceEntry> findResources(Class<?> clazz, String prefix)
            throws URISyntaxException, IllegalArgumentException, IOException {
        prefix = prefix.replace('\\', '/');
        URL url = clazz.getClassLoader().getResource(prefix);
        if (url != null && url.getProtocol().equals("file")) {
            File f = new File(url.toURI());
            if (f.isDirectory()) {
                return findResources(f, Paths.get(""), new LinkedList<>());
            }
        }

        // Find prefix in JAR that contains the specified service
        String className = clazz.getName().replace('.', '/');
        className += ".class";
        URL classURL = clazz.getClassLoader().getResource(className);

        if (classURL == null) {
            // handle runtime-generated classes not found in a jar
            // still not perfect but doesn't break anything
            int i = className.lastIndexOf('$');
            if (i > 0) {
                className = className.substring(0, i);
                className += ".class";
                classURL = clazz.getClassLoader().getResource(className);
            }

            if (classURL == null) {
                throw new RuntimeException("Expected to find class resource for specified class");
            }
        }

        if (!classURL.getProtocol().equals("jar")) {
            return new LinkedList<>();
        }

        // URL is of the form: "jar:file:/path/to/jar.jar!resource/path/in/jar.txt"
        // First, get path to JAR including the "file" schema, so it can be reused later.
        JarURLConnection jarCon = (JarURLConnection) classURL.openConnection();
        jarCon.setUseCaches(false);
        JarFile jar = jarCon.getJarFile();
        try {
            // Enumerate entries in JAR and filter those that match the specified prefix.
            Enumeration<JarEntry> entries = jar.entries();
            List<ResourceEntry> result = new LinkedList<>();
            while (entries.hasMoreElements()) {
                String name = entries.nextElement().getName();

                // Skip directories
                if (name.endsWith("/")) {
                    continue;
                }

                if (name.startsWith(prefix)) {
                    ResourceEntry entry = new ResourceEntry();
                    String entryUrl = jarCon.getJarFileURL() + "!/" + name;
                    if (entryUrl.startsWith("file:")) {
                        entryUrl = "jar:" + entryUrl;
                    }
                    entry.url = new URL(entryUrl);
                    entry.suffix = Paths.get(name.substring(prefix.length() + 1));
                    result.add(entry);
                }
            }
            return result;
        } finally {
            if (jar != null) {
                jar.close();
            }
        }
    }

    private static List<ResourceEntry> findResources(File f, Path suffix, List<ResourceEntry> result)
            throws MalformedURLException {
        File[] files = f.listFiles();
        if (files == null) {
            return Collections.emptyList();
        }
        for (File file : files) {
            if (file.isDirectory()) {
                findResources(file, suffix.resolve(file.getName()), result);
                continue;
            }
            ResourceEntry entry = new ResourceEntry();
            entry.url = file.toURI().toURL();
            entry.suffix = suffix.resolve(file.getName());
            result.add(entry);
        }
        return result;
    }

    public static List<File> findFiles(Path rootPath, Set<String> fileNames, boolean isExactMatch) {
        return findFiles(rootPath, fileNames, new ArrayList<>(), isExactMatch);
    }

    private static List<File> findFiles(Path rootPath, Set<String> fileNames, List<File> files,
            boolean isExactMatch) {
        DirectoryStream.Filter<Path> filter = (file) -> {
            if (file.toFile().isDirectory()) {
                return true;
            }

            if (fileNames.isEmpty()) {
                return true;
            }

            String thisFileName = file.getFileName().toString();
            if (fileNames.contains(thisFileName)) {
                return true;
            }

            if (isExactMatch) {
                return false;
            }
            for (String fileName : fileNames) {
                if (thisFileName.contains(fileName)) {
                    return true;
                }
            }
            return false;
        };

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootPath, filter)) {
            for (Path file : stream) {
                File f = file.toFile();
                if (f.isDirectory()) {
                    findFiles(f.toPath(), fileNames, files, isExactMatch);
                    continue;
                }
                files.add(f);
            }
        } catch (Throwable e) {
            Logger.getAnonymousLogger().warning(Utils.toString(e));
        }
        return files;
    }

    public static void deleteFiles(File directory) {
        moveOrDeleteFiles(directory, null);
    }

    public static void moveOrDeleteFiles(File directory, File newDirectory) {
        moveOrDeleteFiles(directory, newDirectory, true);
    }

    public static void moveOrDeleteFiles(File oldDirectory, File newDirectory,
            boolean deleteOldDirectory) {
        Path newDir = newDirectory != null ? newDirectory.toPath() : null;

        try {
            Path oldPath = oldDirectory.toPath();
            Files.walkFileTree(oldPath,
                    new FileVisitor<Path>() {

                        @Override
                        public FileVisitResult preVisitDirectory(Path dir,
                                BasicFileAttributes attrs) throws IOException {
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                                throws IOException {
                            if (newDir == null) {
                                Files.deleteIfExists(file);
                            } else {
                                Files.move(file, newDir.resolve(file.getFileName()),
                                        StandardCopyOption.REPLACE_EXISTING);
                            }
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFileFailed(Path file, IOException exc)
                                throws IOException {
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                                throws IOException {
                            if (deleteOldDirectory || !dir.equals(oldPath)) {
                                Files.deleteIfExists(dir);
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
        } catch (IOException e) {
            Logger.getAnonymousLogger().warning(Utils.toString(e));
        }
    }

    public static void copyFiles(File oldDirectory, File newDirectory) {
        Path newDir = newDirectory != null ? newDirectory.toPath() : null;

        try {
            Path oldPath = oldDirectory.toPath();
            Files.walkFileTree(oldPath,
                    new FileVisitor<Path>() {

                        @Override
                        public FileVisitResult preVisitDirectory(Path dir,
                                BasicFileAttributes attrs) throws IOException {
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                                throws IOException {
                            if (newDir != null) {
                                Files.copy(file, newDir.resolve(file.getFileName()),
                                        StandardCopyOption.REPLACE_EXISTING);
                            }
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFileFailed(Path file, IOException exc)
                                throws IOException {
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                                throws IOException {
                            return FileVisitResult.CONTINUE;
                        }
                    });
        } catch (IOException e) {
            Logger.getAnonymousLogger().warning(Utils.toString(e));
        }
    }

    public static String getContentType(URI uri) {
        String mediaType;
        String uriPathLower = uri.getPath().toLowerCase();
        if (uriPathLower.endsWith("css")) {
            mediaType = Operation.MEDIA_TYPE_TEXT_CSS;
        } else if (uriPathLower.endsWith("json")) {
            mediaType = Operation.MEDIA_TYPE_APPLICATION_JSON;
        } else if (uriPathLower.endsWith("js")) {
            mediaType = Operation.MEDIA_TYPE_APPLICATION_JAVASCRIPT;
        } else if (uriPathLower.endsWith("html")
                || uriPathLower.endsWith("htm")) {
            mediaType = Operation.MEDIA_TYPE_TEXT_HTML;
        } else if (uriPathLower.endsWith("txt")) {
            mediaType = Operation.MEDIA_TYPE_TEXT_PLAIN;
        } else if (uriPathLower.endsWith("svg")) {
            mediaType = Operation.MEDIA_TYPE_IMAGE_SVG_XML;
        } else if (uriPathLower.endsWith("woff2")) {
            mediaType = Operation.MEDIA_TYPE_APPLICATION_FONT_WOFF2;
        } else {
            mediaType = Operation.MEDIA_TYPE_APPLICATION_OCTET_STREAM;
        }
        return mediaType;
    }

    public static void readFileAndComplete(final Operation op, File f) throws IOException {

        final AsynchronousFileChannel ch = AsynchronousFileChannel.open(f.toPath(),
                StandardOpenOption.READ);

        final ByteBuffer bb = ByteBuffer.allocate((int) f.length());

        ch.read(bb, 0L, null,
                new CompletionHandler<Integer, Void>() {

                    @Override
                    public void completed(Integer arg0, Void v) {
                        try {
                            bb.flip();
                            close(bb, ch);
                            String contentType = FileUtils.getContentType(f.toURI());
                            if (contentType != null) {
                                op.setContentType(contentType);
                            }

                            String body = Utils.decodeIfText(bb, contentType);
                            if (body != null) {
                                op.setBody(body);
                            } else {
                                op.setBody(bb.array());
                                op.setContentLength(bb.limit());
                            }
                            op.complete();
                        } catch (Throwable e) {
                            failed(e, v);
                        }
                    }

                    @Override
                    public void failed(Throwable arg0, Void v) {
                        close(null, ch);
                        op.fail(arg0);
                    }

                    private void close(ByteBuffer buffer, AsynchronousFileChannel channel) {
                        try {
                            channel.close();
                        } catch (Throwable e) {
                        }
                    }
                });
    }

    /**
     * GET a file.
     *
     * @param h  ServiceClient used to connect to the host
     * @param get The Operation used to GET the given file URI
     * @param f File to write to
     * @throws IOException
     */
    public static void getFile(ServiceClient h, final Operation get, File f) throws IOException {
        final AsynchronousFileChannel ch = AsynchronousFileChannel.open(f.toPath(),
                StandardOpenOption.WRITE);
        ch.force(true);

        final AtomicInteger bytesWritten = new AtomicInteger(0);
        // Do a single GET of the file.  If there's a content-range header, do subsequent GET
        // operations until the file is complete.
        final Operation.CompletionHandler getCompletion = (o, e) -> {
            if (e != null) {
                get.fail(e);
                return;
            }

            try {
                Logger.getAnonymousLogger().log(
                        Level.INFO,
                        String.format("starting download of %s to %s", get.getUri().toString(),
                                f.toString()));
                // no content ranges
                writeBody(get, o, ch, bytesWritten);
            } catch (Throwable ex) {
                get.fail(ex);
            }

            String contentRangeHeader = o.getResponseHeader(Operation.CONTENT_RANGE_HEADER);
            if (contentRangeHeader == null) {
                // done, get operation will be completed by writeBody() above
                return;
            }

            // send a bunch of ranges.
            ContentRange nextRange = new ContentRange(contentRangeHeader);
            getChunks(h, nextRange, get, ch, bytesWritten);
        };

        h.send(Operation.createGet(get.getUri())
                .transferRefererFrom(get)
                .addHeader(new ContentRange().toRangeHeader(), false)
                .setExpiration(get.getExpirationMicrosUtc())
                .setCompletion(getCompletion));

    }

    private static void getChunks(ServiceClient h, ContentRange nextRange, Operation parentGet,
            AsynchronousFileChannel ch,
            AtomicInteger bytesWritten) {

        final ContentRange[] range = { nextRange };
        for (int chunksInFlight = 0; (chunksInFlight < ContentRange.MAX_IN_FLIGHT_CHUNKS)
                && !range[0].isDone(); chunksInFlight++) {

            range[0] = range[0].nextChunk();

            final boolean getNextSet = (chunksInFlight == ContentRange.MAX_IN_FLIGHT_CHUNKS - 1);
            Operation nextGet = Operation
                    .createGet(parentGet.getUri())
                    .transferRefererFrom(parentGet)
                    .addHeader(range[0].toRangeHeader(), false)
                    .setExpiration(parentGet.getExpirationMicrosUtc())
                    .setCompletion(
                            (ox, ex) -> {
                                if (ex != null) {
                                    parentGet.fail(ex);
                                    return;
                                }
                                try {
                                    writeBody(parentGet, ox, ch, bytesWritten);
                                    if (!range[0].isDone() && getNextSet) {
                                        getChunks(h, range[0], parentGet, ch,
                                                bytesWritten);
                                    }
                                } catch (Throwable exx) {
                                    parentGet.fail(exx);
                                }
                            });

            h.send(nextGet);
        }

    }

    private static void writeBody(Operation parentOp, Operation o, AsynchronousFileChannel ch,
            AtomicInteger bytesWritten) throws Throwable {

        byte[] b = (byte[]) o.getBodyRaw();
        if (b == null || b.length == 0) {
            parentOp.fail(new IllegalStateException("no data"));
            return;
        }

        final ByteBuffer buf = ByteBuffer.wrap(b);
        String contentRangeHeader = o.getResponseHeader(Operation.CONTENT_RANGE_HEADER);
        final ContentRange range = new ContentRange(contentRangeHeader);

        ch.write(buf, range.start, bytesWritten,
                new CompletionHandler<Integer, AtomicInteger>() {

                    @Override
                    public void completed(Integer result, AtomicInteger bytesWritten) {
                        int total = bytesWritten.addAndGet(result);

                        if (total >= range.fileSize) {
                            try {
                                ch.close();
                                Logger.getAnonymousLogger().log(Level.INFO, String.format(
                                        "done download of %s (bytes %s)",
                                        parentOp.getUri().toString(), total));

                                parentOp.complete();
                            } catch (Exception e) {
                                parentOp.fail(e);
                            }
                        }
                    }

                    @Override
                    public void failed(Throwable exc, AtomicInteger bytesWritten) {
                        parentOp.fail(exc);
                    }
                });

    }

    /**
     * Given a POST operation and a File, post the file to the URI.
     *
     * @param h ServiceClient
     * @param put Operation used to PUT the file at the given URL
     * @param f File to put
     * @throws IOException
     */
    public static void putFile(ServiceClient h, final Operation put, File f) throws IOException {
        final AsynchronousFileChannel ch = AsynchronousFileChannel.open(f.toPath(),
                StandardOpenOption.READ);

        AtomicInteger completionCount = new AtomicInteger(0);
        String contentType = FileUtils.getContentType(f.toURI());

        final boolean[] fileIsDone = { false };

        putChunks(h, put, ch, contentType, f.length(), 0, completionCount, fileIsDone);
    }

    private static void putChunks(ServiceClient h, final Operation put, AsynchronousFileChannel ch,
            String contentType, long fileLength,
            int off, AtomicInteger completionCount, boolean[] fileIsDone) {

        ContentRange range = new ContentRange(off, off + ContentRange.CHUNK_SIZE, (int) fileLength);

        for (int chunksInFlight = 0; chunksInFlight < ContentRange.MAX_IN_FLIGHT_CHUNKS; chunksInFlight++, range = range
                .nextChunk()) {

            completionCount.incrementAndGet();
            ByteBuffer bb = ByteBuffer.allocate((int) (range.end - range.start));
            fileIsDone[0] = range.isDone();
            final boolean startNextChunk = (chunksInFlight == ContentRange.MAX_IN_FLIGHT_CHUNKS - 1);
            ContentRange rangeToStartNextChunk = range.nextChunk();

            ch.read(bb, range.start, range,
                    new CompletionHandler<Integer, ContentRange>() {

                        @Override
                        public void completed(Integer arg0, ContentRange r) {
                            try {
                                bb.flip();
                                byte[] buf = bb.array();

                                Operation rangePut = Operation
                                        .createPut(put.getUri())
                                        .setBodyNoCloning(buf)
                                        .setContentLength(bb.limit())
                                        .setRetryCount(0)
                                        .transferRefererFrom(put)
                                        .setExpiration(put.getExpirationMicrosUtc())
                                        .setCompletion(
                                                (o, e) -> {
                                                    if (e != null) {
                                                        put.fail(e);
                                                        return;
                                                    }

                                                    if (completionCount.decrementAndGet() == 0
                                                            && fileIsDone[0]) {
                                                        close(ch);
                                                        put.complete();
                                                        return;
                                                    }

                                                    if (startNextChunk) {
                                                        putChunks(h, put, ch, contentType,
                                                                fileLength,
                                                                (int) rangeToStartNextChunk.start,
                                                                completionCount, fileIsDone);
                                                    }
                                                });

                                rangePut.addHeader(r.toContentRangeHeader(), false);

                                if (contentType != null) {
                                    rangePut.setContentType(contentType);
                                }

                                h.send(rangePut);
                            } catch (Throwable e) {
                                failed(e, r);
                            }
                        }

                        @Override
                        public void failed(Throwable arg0, ContentRange r) {
                            close(ch);
                            put.fail(arg0);
                        }

                        private void close(AsynchronousFileChannel channel) {
                            try {
                                channel.close();
                            } catch (Throwable e) {
                                Logger.getAnonymousLogger().log(Level.WARNING,
                                        String.format("PUT of file failed %s",
                                                e.toString()));
                            }
                        }
                    });

            if (range.isDone()) {
                return;
            }
        }
    }

    /**
     * Given a list of files, zip them into a single archive.
     *
     * Infrastructure use only!!!  Do not use in production without spinning your own thread.
     */
    public static URI zipFiles(List<URI> inFiles, String outFileName) throws Exception {
        byte[] buffer = new byte[4096]; // Create a buffer for copying
        int bytes_read;

        File zipFile = File.createTempFile(outFileName, ".zip", null);

        // Create a stream to compress data and write it to the zipfile
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zipFile));

        try {
            // Loop through all entries in the directory
            for (URI file : inFiles) {
                File f = new File(file);
                if (f.isDirectory()) {
                    throw new IllegalArgumentException("can't compress a directory:  " + f);
                }
                FileInputStream in = new FileInputStream(f);

                // every file gets a new entry
                ZipEntry entry = new ZipEntry(f.getName());
                out.putNextEntry(entry); // Store entry
                while ((bytes_read = in.read(buffer)) != -1) {
                    // Copy bytes
                    out.write(buffer, 0, bytes_read);
                }
                try {
                    in.close();
                } catch (IOException ignore) {
                }
            }
        } finally {
            try {
                out.close();
            } catch (IOException ignore) {
            }
        }

        Logger.getAnonymousLogger().info(
                String.format("backup written to %s (bytes:%s md5sum:%s)", zipFile,
                        zipFile.length(), md5sum(zipFile)));

        return zipFile.toURI();
    }

    public static void extractZipArchive(final File zipFile, Path destDir) throws IOException,
            IllegalArgumentException {
        if (!destDir.toFile().exists() || !destDir.toFile().isDirectory()) {
            throw new IllegalArgumentException("can only unzip to directory");
        }

        if (!zipFile.exists()) {
            throw new IllegalArgumentException("zip file not found");
        }

        try (FileSystem zipFileSystem = FileSystems.newFileSystem(zipFile.toPath(), null)) {
            final Path root = zipFileSystem.getPath("/");

            // walk the zip file tree and copy files to the destination
            Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file,
                        BasicFileAttributes attrs) throws IOException {
                    final Path destFile = Paths.get(destDir.toString(),
                            file.toString());

                    Logger.getAnonymousLogger().info("Extracting file " + destFile);
                    Files.copy(file, destFile, StandardCopyOption.REPLACE_EXISTING);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult preVisitDirectory(Path dir,
                        BasicFileAttributes attrs) throws IOException {
                    final Path dirToCreate = Paths.get(destDir.toString(),
                            dir.toString());
                    if (Files.notExists(dirToCreate)) {
                        Logger.getAnonymousLogger().info("Creating directory %s" + dirToCreate);
                        Files.createDirectory(dirToCreate);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }

    public static String md5sum(File f) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] bytes = new byte[1 << 13];
        int numBytes;
        try (InputStream is = Files.newInputStream(f.toPath())) {
            while ((numBytes = is.read(bytes)) != -1) {
                md.update(bytes, 0, numBytes);
            }
        }
        BigInteger bigInt = new BigInteger(1, md.digest());
        return bigInt.toString(16);
    }

    public static Properties readPropertiesFromResource(
            Class<? extends ServiceHost> typeAssociatedWithResource,
            String resourceName) throws IOException {
        URL url = typeAssociatedWithResource
                .getClassLoader()
                .getResource(resourceName);
        if (url == null) {
            return null;
        }

        Properties properties = new Properties();
        try {
            properties.load(url.openStream());
        } catch (Throwable e) {
            String message = String.format("Unable to load properties from %s", url.toString());
            throw new IOException(message, e);
        }
        return properties;
    }

    public static List<URI> filesToUris(String parentDir, Collection<String> files) {
        ArrayList<URI> fileUris = new ArrayList<>();
        for (String f : files) {
            fileUris.add(new File(parentDir, f).toURI());
        }
        return fileUris;
    }
}
