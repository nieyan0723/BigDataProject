package validate;

import java.io.IOException;
import java.util.zip.Checksum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import common.Unsigned16;
import common.DataInputFormat;

public class DataValidate extends Configured implements Tool {
    private static final Text ERROR = new Text("error");
    private static final Text CHECKSUM = new Text("checksum");

    private static String textifyBytes(Text t) {
        BytesWritable b = new BytesWritable();
        b.set(t.getBytes(), 0, t.getLength());
        return b.toString();
    }

    static class ValidateMapper extends Mapper<Text, Text, Text, Text> {
        private Text lastKey;
        private String filename;
        private Unsigned16 checksum = new Unsigned16();
        private Unsigned16 tmp = new Unsigned16();
        private Checksum crc32 = new PureJavaCrc32();

        /**
         * Get the final part of the input name
         * 
         * @param split the input split
         * @return the "part-r-00000" for the input
         */
        private String getFilename(FileSplit split) {
            return split.getPath().getName();
        }

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if (lastKey == null) {
                FileSplit fs = (FileSplit) context.getInputSplit();
                filename = getFilename(fs);
                context.write(new Text(filename + ":begin"), key);
                lastKey = new Text();
            } else {
                if (key.compareTo(lastKey) < 0) {
                    context.write(ERROR, new Text("misorder in " + filename + " between " + textifyBytes(lastKey)
                            + " and " + textifyBytes(key)));
                }
            }
            // compute the crc of the key and value and add it to the sum
            crc32.reset();
            crc32.update(key.getBytes(), 0, key.getLength());
            crc32.update(value.getBytes(), 0, value.getLength());
            tmp.set(crc32.getValue());
            checksum.add(tmp);
            lastKey.set(key);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            if (lastKey != null) {
                context.write(new Text(filename + ":end"), lastKey);
                context.write(CHECKSUM, new Text(checksum.toString()));
            }
        }
    }

    /**
     * Check the boundaries between the output files by making sure that the
     * boundary keys are always increasing. Also passes any error reports along
     * intact.
     */
    static class ValidateReducer extends Reducer<Text, Text, Text, Text> {
        private boolean firstKey = true;
        private Text lastKey = new Text();
        private Text lastValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (ERROR.equals(key)) {
                for (Text val : values) {
                    context.write(key, val);
                }
            } else if (CHECKSUM.equals(key)) {
                Unsigned16 tmp = new Unsigned16();
                Unsigned16 sum = new Unsigned16();
                for (Text val : values) {
                    tmp.set(val.toString());
                    sum.add(tmp);
                }
                context.write(CHECKSUM, new Text(sum.toString()));
            } else {
                Text value = values.iterator().next();
                if (firstKey) {
                    firstKey = false;
                } else {
                    if (value.compareTo(lastValue) < 0) {
                        context.write(ERROR, new Text("bad key partitioning:\n  file " + lastKey + " key "
                                + textifyBytes(lastValue) + "\n  file " + key + " key " + textifyBytes(value)));
                    }
                }
                lastKey.set(key);
                lastValue.set(value);
            }
        }

    }

    private static void usage() throws IOException {
        System.err.println("datavalidate <out-dir> <report-dir>");
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        if (args.length != 2) {
            usage();
            return 1;
        }
        DataInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJobName("DataValidate");
        job.setJarByClass(DataValidate.class);
        job.setMapperClass(ValidateMapper.class);
        job.setReducerClass(ValidateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // force a single reducer
        job.setNumReduceTasks(1);
        // force a single split
        FileInputFormat.setMinInputSplitSize(job, Long.MAX_VALUE);
        job.setInputFormatClass(DataInputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DataValidate(), args);
        System.exit(res);
    }

}