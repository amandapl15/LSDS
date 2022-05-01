/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package upf.edu.uploader;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.commons.codec.Charsets;

public class S3Uploader implements Uploader {

    private String BucketName;
    private String Prefix;
    private String CredencialsProfileName;

    public S3Uploader(String BucketName, String Prefix, String CredencialsProfileName) {
        this.BucketName = BucketName;
        this.Prefix = Prefix;
        this.CredencialsProfileName = CredencialsProfileName;
    }

    public String getBucketName() {
        return BucketName;
    }

    public String getPrefix() {
        return Prefix;
    }

    public String getCredencialsProfileName() {
        return CredencialsProfileName;
    }

    @Override
    public void upload(List<String> files) {

        AWSCredentials credentials = null;
        try {
            //Credenciales del archivo .aws (key_id, acces_key, session_token)
            credentials = new ProfileCredentialsProvider(getCredencialsProfileName()).getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. ", e);
        }

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();

        for (String file : files) {

            try {
                System.out.println("\nLoading...");
                String path = getBucketName() + "/" + getPrefix();
                //Añade el fichero output.txt al bucket del S3 AWS
                s3Client.putObject(path, file, new File(file));
            } catch (AmazonServiceException e) {
                System.err.println("Error updating the file: "+e.getErrorMessage());
                System.exit(1);
            }
            System.out.println("Done!");
            System.out.println("Uploaded bucket (" + getBucketName() + ") successfully.");
        }     
    }
}
