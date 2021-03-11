
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.BasicAWSCredentials;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ResponseHeaderOverrides;
import com.amazonaws.services.s3.model.S3Object;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.lang.annotation.Documented;

public class Test {
    public static void main(String[] args) throws IOException {
        S3Test();
        MongoTest();

    }

    public static void HDFSTest() throws IOException {
        Configuration configuration = new Configuration();
        String filePath = "/test/real_100m.csv";
        String file = "real_100m.csv";
        String hdfsPath = "hdfs://test/"+file;
        configuration.set("fs.default.name", hdfsPath);
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(filePath);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exists");
            return;
        }
        long start = System.currentTimeMillis();
        FSDataInputStream in = fileSystem.open(path);

        String filename = file.substring(file.lastIndexOf('/') + 1,
                file.length());

        OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(filename)));
        byte[] b = new byte[1024];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("MongoDB Elapsed Time : " + timeElapsed);
    }

    public static void MongoTest(){
        Integer PORT = 20000;

        long start = System.currentTimeMillis();
        MongoClient mongoClient = new MongoClient("localhost", PORT);
        MongoDatabase db = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = db.getCollection("test");
        MongoCursor<Document> cursor = collection.find().iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("MongoDB Elapsed Time : " + timeElapsed);
    }


    public static void S3Test(){
        Regions clientRegion = Regions.DEFAULT_REGION;
        String bucketName = "geobenchmark";
        String KEY_ID = "AKIAYGNFNBXEOT7N5POH";
        String SECRET_ACCESS_KEY = "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";

        S3Object fullObject = null, objectPortion = null, headerOverrideObject = null;
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                            KEY_ID,
                            SECRET_ACCESS_KEY)
                    ))
                    .withRegion(Regions.AP_NORTHEAST_2)
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();

            // Get an object and print its contents.
            System.out.println("Downloading an object");
            long start = System.currentTimeMillis();
            fullObject = s3Client.getObject(bucketName);
            long finish = System.currentTimeMillis();
            long timeElapsed = finish - start;
            System.out.println("S3 Elapsed Time : " + timeElapsed);
            System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());
            System.out.println("Content: ");

            /*
            // Get a range of bytes from an object and print the bytes.
            GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, key)
                    .withRange(0, 9);
            objectPortion = s3Client.getObject(rangeObjectRequest);
            System.out.println("Printing bytes retrieved.");
            displayTextInputStream(objectPortion.getObjectContent());

            // Get an entire object, overriding the specified response headers, and print the object's content.
            ResponseHeaderOverrides headerOverrides = new ResponseHeaderOverrides()
                    .withCacheControl("No-cache")
                    .withContentDisposition("attachment; filename=example.txt");
            GetObjectRequest getObjectRequestHeaderOverride = new GetObjectRequest(bucketName, key)
                    .withResponseHeaders(headerOverrides);
            headerOverrideObject = s3Client.getObject(getObjectRequestHeaderOverride);
            displayTextInputStream(headerOverrideObject.getObjectContent());
             */
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
    }
}
