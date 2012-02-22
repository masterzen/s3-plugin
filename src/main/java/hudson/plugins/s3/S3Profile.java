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
    
    public void download(Run build, String name, FilePath target) throws IOException, InterruptedException 
    {
      String buildName = build.getDisplayName();
      int buildID = build.getNumber();
      Destination dest = new Destination(name,"jobs/" + buildName + "/" + buildID + "/" + name);
      target.act(new S3DownloadCallable(accessKey, secretKey, dest));
    }

    public static class S3DownloadCallable implements FileCallable<FingerprintRecord> 
    {
      private static final long serialVersionUID = 1L;
      final private String accessKey, secretKey;
      final private Destination dest;
      
      public S3DownloadCallable(String accessKey, String secretKey, Destination dest)
      {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.dest = dest;
      }
      
      /**
       * Remote on slave variant
       */
      public FingerprintRecord invoke(File file, VirtualChannel channel) throws IOException, InterruptedException
      {
        GetObjectRequest req = new GetObjectRequest(dest.bucketName, dest.objectName);
        ObjectMetadata md = getClient().getObject(req, file);

        return new FingerprintRecord(true, null, dest.bucketName, file.getName(), md.getETag());
       }
      
      private AmazonS3Client getClient()
      {
        return new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
      }
    }

    public int downloadAll(Run build, String name, String filter, FilePath targetDir) throws IOException, InterruptedException {
      String buildName = build.getDisplayName();
      int buildID = build.getNumber();
      Destination dest = new Destination(name,"jobs/" + buildName + "/" + buildID + "/" + name);
      targetDir.mkdirs();
      return targetDir.act(new S3DownloadDirCallable(accessKey, secretKey, filter, dest));
    }

    public static class S3DownloadDirCallable implements FileCallable<Integer> 
    {
      private static final long serialVersionUID = 1L;
      final private String accessKey, secretKey;
      final private Destination dest;
      final private String filter;
      
      public S3DownloadDirCallable(String accessKey, String secretKey, String filter, Destination dest)
      {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.filter = filter;
        this.dest = dest;
      }
      
      /**
       * Remote on slave variant
       */
      public Integer invoke(File file, VirtualChannel channel) throws IOException, InterruptedException
      {
        AmazonS3Client s3client = getClient();        

        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
        .withBucketName(dest.bucketName)
        .withPrefix(dest.objectName);
        
        int count = 0;
        ObjectListing objectListing;
        do {
          objectListing = s3client.listObjects(listObjectsRequest);
          for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            File dst = new File(file, new File(summary.getKey()).getName());
            GetObjectRequest req = new GetObjectRequest(dest.bucketName, summary.getKey());
            s3client.getObject(req, dst);
            count++;
          }
          listObjectsRequest.setMarker(objectListing.getNextMarker());
        } while (objectListing.isTruncated());        
        return count;
       }
      
      private AmazonS3Client getClient()
      {
        return new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
      }
    }

    public List<String> list(Run build, String bucket, String expandedFilter) {
      AmazonS3Client s3client = getClient();        

      String buildName = build.getDisplayName();
      int buildID = build.getNumber();
      Destination dest = new Destination(bucket, "jobs/" + buildName + "/" + buildID + "/" + name);

      ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
      .withBucketName(dest.bucketName)
      .withPrefix(dest.objectName);

      List<String> files = Lists.newArrayList();
      
      ObjectListing objectListing;
      do {
        objectListing = s3client.listObjects(listObjectsRequest);
        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
          GetObjectRequest req = new GetObjectRequest(dest.bucketName, summary.getKey());
          files.add(req.getKey());
        }
        listObjectsRequest.setMarker(objectListing.getNextMarker());
      } while (objectListing.isTruncated());        
      return files;
    }

    public List<FingerprintRecord> downloadAll(Run build, List<FingerprintRecord> artifacts, String expandedFilter, FilePath targetDir, boolean flatten) {
        String projectName = build.getParent().getName();
        int buildID = build.getNumber();

        List<FingerprintRecord> fingerprints = Lists.newArrayList();
        for(FingerprintRecord record : artifacts) {
            S3Artifact artifact = record.artifact;
            Destination dest = new Destination(artifact.getBucket(), "jobs/" + projectName + "/" + buildID + "/" + artifact.getName());
            FilePath target = new FilePath(targetDir, artifact.getName());
            try {
                fingerprints.add(target.act(new S3DownloadCallable(accessKey, secretKey, dest)));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return fingerprints;
    }
}
