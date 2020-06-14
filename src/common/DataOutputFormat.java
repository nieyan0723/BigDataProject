package common;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A streamlined text output format that writes key, value, and "\r\n".
 */
public class DataOutputFormat extends TextOutputFormat<Text, Text> {
  static final String FINAL_SYNC_ATTRIBUTE = "datasort.final.sync";

  /**
   * Set the requirement for a final sync before the stream is closed.
   */
  public static void setFinalSync(JobContext job, boolean newValue) {
    job.getConfiguration().setBoolean(FINAL_SYNC_ATTRIBUTE, newValue);
  }

  /**
   * Does the user want a final sync at close?
   */
  public static boolean getFinalSync(JobContext job) {
    return job.getConfiguration().getBoolean(FINAL_SYNC_ATTRIBUTE, false);
  }

  public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
    Configuration conf = job.getConfiguration();
    boolean isCompressed = getCompressOutput(job);
    CompressionCodec codec = null;
    String extension = "";
    if (isCompressed) {
      Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
      extension = codec.getDefaultExtension();
    }
    Path file = getDefaultWorkFile(job, extension);
    FileSystem fs = file.getFileSystem(conf);
    if (!isCompressed) {
      FSDataOutputStream fileOut = fs.create(file, false);
      return new DataRecordWriter(fileOut, job);
    } else {
      FSDataOutputStream fileOut = fs.create(file, false);
      return new DataRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), job);
    }
  }

  static class DataRecordWriter extends LineRecordWriter<Text, Text> {
    private static final byte[] newLine = "\r\n".getBytes();
    private boolean finalSync = false;

    public DataRecordWriter(DataOutputStream out, JobContext job) {
      super(out);
      finalSync = getFinalSync(job);
    }

    @Override
    public synchronized void write(Text key, Text value) throws IOException {
      out.write(key.getBytes(), 0, key.getLength());
      out.write(value.getBytes(), 0, value.getLength());
      out.write(newLine, 0, newLine.length);
    }

    public void close() throws IOException {
      if (finalSync) {
        ((FSDataOutputStream) out).sync();
      }
      super.close(null);
    }
  }

}