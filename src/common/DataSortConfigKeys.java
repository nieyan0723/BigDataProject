package common;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * <p>
 * DataSort configurations.
 * </p>
 */
@Private
@Unstable
public enum DataSortConfigKeys {

  NUM_ROWS("mapreduce.datasort.num-rows", "Number of rows to generate during teragen."),

  NUM_PARTITIONS("mapreduce.datasort.num.partitions", "Number of partitions used for sampling."),

  SAMPLE_SIZE("mapreduce.datasort.partitions.sample", "Sample size for each partition."),

  FINAL_SYNC_ATTRIBUTE("mapreduce.datasort.final.sync", "Perform a disk-persisting hsync at end of every file-write."),

  USE_DATA_SCHEDULER("mapreduce.datasort.use.terascheduler",
      "Use TeraScheduler for computing input split distribution."),

  USE_SIMPLE_PARTITIONER("mapreduce.datasort.simplepartitioner",
      "Use SimplePartitioner instead of TotalOrderPartitioner."),

  OUTPUT_REPLICATION("mapreduce.datasort.output.replication", "Replication factor to use for output data files.");

  private String confName;
  private String description;

  DataSortConfigKeys(String configName, String description) {
    this.confName = configName;
    this.description = description;
  }

  public String key() {
    return this.confName;
  }

  public String toString() {
    return "<" + confName + ">     " + description;
  }

  public static final long DEFAULT_NUM_ROWS = 0L;
  public static final int DEFAULT_NUM_PARTITIONS = 10;
  public static final long DEFAULT_SAMPLE_SIZE = 100000L;
  public static final boolean DEFAULT_FINAL_SYNC_ATTRIBUTE = true;
  public static final boolean DEFAULT_USE_DATA_SCHEDULER = true;
  public static final boolean DEFAULT_USE_SIMPLE_PARTITIONER = false;
  public static final int DEFAULT_OUTPUT_REPLICATION = 1;

}
