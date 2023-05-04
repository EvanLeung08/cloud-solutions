package com.eshare.cloud.largefile;


import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Handle the file upload for large size files to avoid OutOfMemory issue
 */
public class GcsLargeFIleUploadExample {


    public static void main(String[] args) throws IOException {

        List<String> scopes = new ArrayList<>(
                Arrays.asList("https://www.googleapis.com/auth/devstorage.full_control",
                        "https://www.googleapis.com/auth/devstorage.read_write")
        );
        //GCP Json Key Path
        String jsonKeyPath = "";
        //File used to be uploaded
        File sourceFile = new File("filePath");
        //GCS BucketName
        String bucketName = "";
        //GCS Target ObjectName
        String objectName=sourceFile.getName();

        //Prepare connection details
        //https://cloud.google.com/storage/docs/authentication
        GoogleCredentials apiCredentials = GoogleCredentials.fromStream(new FileInputStream(jsonKeyPath)).createScoped(scopes);
        Storage storage = StorageOptions.newBuilder().setCredentials(apiCredentials).build().getService();

        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/json;charset=UTF-8").build();
        uploadToBucket(storage, sourceFile, blobInfo);
    }

    public static void uploadToBucket(Storage storage, File sourceFIle, BlobInfo blobInfo) throws IOException {
        //For small files, we can upload the file in one go
        // less than 10 MB
        if(sourceFIle.length() < 10000000){
            byte[] bytes = Files.readAllBytes(sourceFIle.toPath());
            storage.create(blobInfo,bytes);
            return;
        }

        //For big files , we need to split it into multiple chunks
        try(WriteChannel writer = storage.writer(blobInfo)){
            //Don't read the whole file because it will cause OutOfMemory issue
            byte[] buffer = new byte[10240];
            try(InputStream input = Files.newInputStream(sourceFIle.toPath())){
                int limit;
                while((limit = input.read(buffer))>=0){
                    writer.write(ByteBuffer.wrap(buffer,0,limit));
                }
            }

        }


    }


}
