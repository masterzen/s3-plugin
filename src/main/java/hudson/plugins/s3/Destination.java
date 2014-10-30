package hudson.plugins.s3;

import hudson.model.AbstractBuild;
import hudson.model.Run;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Provides a way to construct a destination bucket name and object name based
 * on the bucket name provided by the user.
 * 
 * The convention implemented here is that a / in a bucket name is used to
 * construct a structure in the object name.  That is, a put of file.txt to bucket name
 * of "mybucket/v1" will cause the object "v1/file.txt" to be created in the mybucket.
 * 
 */
public class Destination implements Serializable {
  private static final long serialVersionUID = 2L;
  private final String bucketName; 
  private final String objectName; 
  private String projectName;
  private String buildNumber;
  
  public Destination(final String userBucketName, final String fileName) {
    this(userBucketName, fileName, "", null);
  }

  public Destination(final String userBucketName, final String fileName, final String projectName, final String buildNumber) {
    
    if (userBucketName == null || fileName == null) 
      throw new IllegalArgumentException("Not defined for null parameters: "+userBucketName+","+fileName);
    
    final String[] bucketNameArray = userBucketName.split("/", 2);
    
    bucketName = bucketNameArray[0];
    
    if (bucketNameArray.length > 1) {
        objectName = bucketNameArray[1] + "/" + fileName;
    } else {
        objectName = fileName;
    }
    this.projectName = projectName;
    this.buildNumber = buildNumber;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getObjectName() {
    return buildNumber != null ? "jobs/" + projectName + "/" + buildNumber + "/" + objectName : objectName;
  }
  
  public String getRelativeName() {
    return objectName;
  }

  // Backwards compatibility for older builds
  public Object readResolve() {
      if (projectName == null || buildNumber == null) {
        // try to fix
        Pattern p = Pattern.compile("^jobs/(.*?)/(\\d+)/(.*)$");
        Matcher m = p.matcher(objectName);
        if (m.matches()) {
          String fileName = m.group(2);
          String project = m.group(1);
          String build = m.group(3);
          return new Destination(bucketName, fileName, project, build);
        } else {
          return this;
        }
      }
      return this;
  }
  
   @Override
   public String toString() {
     return "Destination [bucketName="+bucketName+", objectName="+objectName+"]";
   }
  

  public static Destination newFromRun(Run run, String bucketName, String fileName)
  {
    String projectName = run.getParent().getFullName();
    int buildID = run.getNumber();
    return new Destination(bucketName, fileName, projectName, String.valueOf(buildID));
  }

  public static Destination newFromRun(Run run, S3Artifact artifact) 
  {
    return newFromRun(run, artifact.getBucket(), artifact.getName());
  }
    
  public static Destination newFromBuild(AbstractBuild<?, ?> build, String bucketName, String fileName)
  {
    String projectName = build.getParent().getFullName();
    int buildID =build.getNumber();
    return new Destination(bucketName, fileName, projectName, String.valueOf(buildID));
  }
}
