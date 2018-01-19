package hudson.plugins.s3;

import java.util.Arrays;
import java.util.Comparator;

import com.amazonaws.regions.Regions;

public final class Entry {
    /**
     * Destination bucket for the copy. Can contain macros.
     */
    public String bucket;
    /**
     * File name relative to the workspace root to upload.
     * Can contain macros and wildcards.
     */
    public String sourceFile;
    /**
     * options for x-amz-storage-class can be STANDARD or REDUCED_REDUNDANCY
     */
    public static final String[] storageClasses = {"STANDARD", "REDUCED_REDUNDANCY"};
    /**
     * what x-amz-storage-class is currently set
     */
    public String storageClass;
    /**
     * Regions Values
     */
    public static final Regions[] regions;
    
    /**
     * Stores the Region Value
     */
    public String selectedRegion;
    
    /**
     * Do not publish the artifacts when build fails
     */
    public Boolean noUploadOnFailure = true;
    //public boolean noUploadOnFailure = !uploadOnFailure;

    /**
     * Upload either from the slave or the master
     */
    public boolean uploadFromSlave = true;

    /**
     * Let Jenkins manage the S3 uploaded artifacts
     */
    public Boolean managedArtifacts = true;
    //public boolean managedArtifacts = !unManagedArtifacts;
    
    /**
     * Use S3 server side encryption when uploading the artifacts
     */
    public boolean useServerSideEncryption;

    /**
     * Flatten directories
     */
    public Boolean flatten = true;
    
    static {
      regions = Regions.values().clone();
      Arrays.sort(regions, new Comparator() {

        public int compare(Object o1, Object o2)
        {
          if (o1 instanceof Regions && o2 instanceof Regions) {
            return o1 == Regions.US_EAST_1 ? -1 : 0;
          }
          return 0;
        }
      });
    }
    
    // Backwards compatibility for older builds
    public Object readResolve() {
        if (noUploadOnFailure == null) {
            this.noUploadOnFailure = Boolean.TRUE;
        }
        if (managedArtifacts == null) {
          this.managedArtifacts = Boolean.TRUE;
        }
        if (flatten == null) {
          this.flatten = Boolean.TRUE;
        }
        if (selectedRegion == null) {
          this.selectedRegion = Regions.US_EAST_1.name();
        }
        return this;
    }
}
