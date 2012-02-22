package hudson.plugins.s3;

import hudson.FilePath;
import hudson.FilePath.FileCallable;
import hudson.model.BuildListener;
import hudson.model.AbstractBuild;
import hudson.model.Fingerprint;
import hudson.model.FingerprintMap;
import hudson.model.Run;
import hudson.remoting.VirtualChannel;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import jenkins.model.Jenkins;

import org.kohsuke.stapler.DataBoundConstructor;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;

public class S3Profile {
    private String name;
    private String accessKey;
    private String secretKey;
    private static final AtomicReference<AmazonS3Client> client = new AtomicReference<AmazonS3Client>(null);

    public S3Profile() {
    }

    @DataBoundConstructor
    public S3Profile(String name, String accessKey, String secretKey) {
        this.name = name;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        client.set(new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey)));
    }

    public final String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public final String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public final String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public AmazonS3Client getClient() {
        if (client.get() == null) {
            client.set(new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey)));
        }
        return client.get();
    }

    public void check() throws Exception {
        getClient().listBuckets();
    }
    
    
   
    public FingerprintRecord upload(AbstractBuild<?,?> build, final BuildListener listener, String bucketName, FilePath filePath, boolean uploadFromSlave) throws IOException, InterruptedException {
        if (filePath.isDirectory()) {
            throw new IOException(filePath + " is a directory");
        }
        
        String buildName = build.getWorkspace().getName();
        int buildID = build.getNumber();
        Destination dest = new Destination(bucketName,"jobs/" + buildName + "/" + buildID + "/" + filePath.getName());
        boolean produced = build.getTimeInMillis() <= filePath.lastModified()+2000;
        try {
          S3UploadCallable callable = new S3UploadCallable(produced, accessKey, secretKey, dest);
          if (uploadFromSlave) {
            return filePath.act(callable);
          } else {
            return callable.invoke(filePath);
          }
        } catch (Exception e) {
            listener.getLogger().println("error: " + e.getMessage());
            e.printStackTrace(listener.getLogger());
            throw new IOException("put " + dest + ": " + e);
        }
    }

    public static class S3UploadCallable implements FileCallable<FingerprintRecord> 
    {
      private static final long serialVersionUID = 1L;
      final private String accessKey, secretKey;
      final private Destination dest;
      final private boolean produced;
      
      public S3UploadCallable(boolean produced, String accessKey, String secretKey, Destination dest)
      {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.dest = dest;
        this.produced = produced;
      }
      
      /**
       * Remote on slave variant
       */
      public FingerprintRecord invoke(File file, VirtualChannel channel) throws IOException, InterruptedException
      {
        PutObjectResult result = getClient().putObject(dest.bucketName, dest.objectName, file);
        URL url = getClient().generatePresignedUrl(dest.bucketName, dest.objectName, new Date(System.currentTimeMillis() + 1000 * 86400 * 365));
        return new FingerprintRecord(produced, url.toExternalForm(), dest.bucketName, file.getName(), result.getETag());
      }
      
      /**
       * Stream from slave, upload on master variant
       */
      public FingerprintRecord invoke(FilePath file) throws IOException, InterruptedException
      {
        ObjectMetadata md = new ObjectMetadata();
        md.setContentLength(file.length());
        md.setLastModified(new Date(file.lastModified()));
        PutObjectResult result = getClient().putObject(dest.bucketName, dest.objectName, file.read(), md);
        URL url = getClient().generatePresignedUrl(dest.bucketName, dest.objectName, new Date(System.currentTimeMillis() + 1000 * 86400 * 365));
        return new FingerprintRecord(produced, url.toExternalForm(), dest.bucketName, file.getName(), result.getETag());
      }

      private AmazonS3Client getClient()
      {
        return new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
      }
    }
    
}
