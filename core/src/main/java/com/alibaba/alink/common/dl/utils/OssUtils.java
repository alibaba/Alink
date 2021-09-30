package com.alibaba.alink.common.dl.utils;

import org.apache.flink.util.StringUtils;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.DownloadFileRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OssUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OssUtils.class);

    public static void downloadFile(File targetFile, String endpoint, String bucketName, String ossPath,
                                    String accessKeyId, String accessKeySecret, String securityToken) {
        OSS client = StringUtils.isNullOrWhitespaceOnly(securityToken) ?
            (new OSSClient(endpoint, accessKeyId, accessKeySecret)) :
            (new OSSClient(endpoint, accessKeyId, accessKeySecret, securityToken));

        DownloadFileRequest downloadFileRequest = new DownloadFileRequest(bucketName, ossPath);
        // Sets the local file to download to
        downloadFileRequest.setDownloadFile(targetFile.getAbsolutePath());
        // Enable checkpoint. By default it's false.
        downloadFileRequest.setEnableCheckpoint(true);

        System.out.println("downloading " + ossPath + " ...");
        try {
            client.downloadFile(downloadFileRequest);
        } catch (OSSException oe) {
            System.out.println("Caught an OSSException, which means your request made it to OSS, "
                + "but was rejected with an error response for some reason.");
            System.out.println("Error Message: " + oe.getErrorCode());
            System.out.println("Error Code:       " + oe.getErrorCode());
            System.out.println("Request ID:      " + oe.getRequestId());
            System.out.println("Host ID:           " + oe.getHostId());
        } catch (ClientException ce) {
            System.out.println("Caught an ClientException, which means the client encountered "
                + "a serious internal problem while trying to communicate with OSS, "
                + "such as not being able to access the network.");
            System.out.println("Error Message: " + ce.getMessage());
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
        }
        System.out.println(String.format("downloaded %s to %s", ossPath, targetFile.getAbsolutePath()));
        client.shutdown();
    }

    public static String downloadFilesToDirectory(String targetDir, String endpoint, String bucketName, String ossPath,
                                                  String accessKeyId, String accessKeySecret, String securityToken) {
        OSS client = StringUtils.isNullOrWhitespaceOnly(securityToken) ?
            (new OSSClient(endpoint, accessKeyId, accessKeySecret)) :
            (new OSSClient(endpoint, accessKeyId, accessKeySecret, securityToken));

        List<String> allFileNames = new ArrayList<>();
        ObjectListing objectListing = null;
        String nextMarker = null;
        final int maxKeys = 100;
        List<OSSObjectSummary> sums = null;

        do {
            objectListing = client.listObjects(new ListObjectsRequest(bucketName).
                withPrefix(ossPath).withMarker(nextMarker).withMaxKeys(maxKeys));
            sums = objectListing.getObjectSummaries();
            for (OSSObjectSummary s : sums) {
                allFileNames.add(s.getKey());
            }
            nextMarker = objectListing.getNextMarker();
        } while (objectListing.isTruncated());

        if (allFileNames.size() <= 0) {
            throw new RuntimeException(String.format("oss://%s/%s not exist", bucketName, ossPath));
        }

        for (String key : allFileNames) {
            String outFn = targetDir + "/" + key;
            System.out.println("downloading " + key + " ...");
            try {
                if (key.endsWith("/")) {
                    new File(outFn).mkdirs();
                    continue;
                } else {
                    int lastPos = outFn.lastIndexOf('/');
                    new File(outFn.substring(0, lastPos)).mkdirs();
                }
                DownloadFileRequest downloadFileRequest = new DownloadFileRequest(bucketName, key);
                // Sets the local file to download to
                downloadFileRequest.setDownloadFile(outFn);
                // Enable checkpoint. By default it's false.
                downloadFileRequest.setEnableCheckpoint(true);

                client.downloadFile(downloadFileRequest);
            } catch (OSSException oe) {
                System.out.println("Caught an OSSException, which means your request made it to OSS, "
                    + "but was rejected with an error response for some reason.");
                System.out.println("Error Message: " + oe.getErrorCode());
                System.out.println("Error Code:       " + oe.getErrorCode());
                System.out.println("Request ID:      " + oe.getRequestId());
                System.out.println("Host ID:           " + oe.getHostId());
            } catch (ClientException ce) {
                System.out.println("Caught an ClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with OSS, "
                    + "such as not being able to access the network.");
                System.out.println("Error Message: " + ce.getMessage());
            } catch (Throwable e) {
                e.printStackTrace();
            } finally {
            }
        }

        client.shutdown();
        return targetDir + "/" + ossPath;
    }

    public static void main(String[] args) throws Exception {
        String endpoint = "cn-hangzhou.oss.aliyun-inc.com";
        String accessKeyId = args[0];
        String accessKeySecret = args[1];
        String bucketName = "pai-cj";
        String ossPath = "text_cnn_checkpoint/summary/";

        downloadFilesToDirectory("/tmp/test_oss", endpoint, bucketName, ossPath,
            accessKeyId, accessKeySecret, null);
    }

    /**
     * @param path
     * @param targetDir
     * @return
     */
    public static String downloadOssDirectory(String path, String targetDir) {
        /**
         * The format is:
         * oss://<bucket>/<path>/?host=<host>&access_key_id=<access_key_id>&access_key_secret=<access_key_secret>
         *     &security_token=<security_token>
         */
        int pos = path.indexOf('?');
        String url = path.substring(0, pos);
        if (!url.endsWith("/")) {
            url = url + "/";
        }
        url = url.substring("oss://".length());
        String bucket = url.substring(0, url.indexOf('/'));
        String ossPath = url.substring(url.indexOf('/') + 1);
        String params = path.substring(pos + 1);
        LOG.info("bucket: {}", bucket);
        LOG.info("ossPath: {}", ossPath);
        Map <String, String> kv = new HashMap <>();
        String[] kvPairs = params.split("&");
        for (int i = 0; i < kvPairs.length; i++) {
            int p = kvPairs[i].indexOf('=');
            kv.put(kvPairs[i].substring(0, p), kvPairs[i].substring(p + 1));
        }
        downloadFilesToDirectory(targetDir, kv.get("host"), bucket, ossPath,
            kv.get("access_key_id"), kv.get("access_key_secret"), kv.get("security_token"));
        return ossPath;
    }
}
