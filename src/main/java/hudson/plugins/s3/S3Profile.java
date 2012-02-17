package hudson.plugins.s3;

import hudson.FilePath;
import hudson.FilePath.FileCallable;
import hudson.model.BuildListener;
import hudson.model.AbstractBuild;
import hudson.remoting.VirtualChannel;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

import org.kohsuke.stapler.DataBoundConstructor;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ProgressEvent;
import com.amazonaws.services.s3.model.ProgressListener;
import com.amazonaws.services.s3.model.PutObjectRequest;

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
    
    
   
    public void upload(AbstractBuild<?,?> build, final BuildListener listener, String bucketName, FilePath filePath) throws IOException, InterruptedException {
        if (filePath.isDirectory()) {
            throw new IOException(filePath + " is a directory");
        }
        
        String buildName = build.getWorkspace().getName();
        int buildID = build.getNumber();
        Destination dest = new Destination(bucketName,"jobs/" + buildName + "/" + buildID + "/" + filePath.getName());
        try {
          filePath.act(new S3UploadCallable(name, accessKey, secretKey, dest));
        } catch (Exception e) {
            listener.getLogger().println("error: " + e.getMessage());
            e.printStackTrace(listener.getLogger());
            throw new IOException("put " + dest + ": " + e);
        }
    }
    
    public static class S3UploadCallable implements FileCallable<Void> 
    {
      private String name, accessKey, secretKey;
      private Destination dest;
      
      public S3UploadCallable(String name, String accessKey, String secretKey, Destination dest)
      {
        this.name = name;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.dest = dest;
      }
      
      public Void invoke(File file, VirtualChannel channel) throws IOException, InterruptedException
      {
        AmazonS3Client client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
        client.putObject(dest.bucketName, dest.objectName, file);
        return null;
      }
      
    }
    
}
