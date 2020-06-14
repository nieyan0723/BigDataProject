package validate;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import common.DataInputFormat;

public class DataValidate extends Configured implements Tool {
    private static final Text error = new Text("error");

    static class ValidateMapper extends Mapper<Text, Text, Text, Text> {
        private Text lastKey;
        private String filename;

        private String getFilename(final FileSplit split) {
            return split.getPath().getName();
        }

        @Override
        public void map(final Text key, final Text value, final Context context)
                throws IOException, InterruptedException {
            if (lastKey == null) {
                filename = getFilename((FileSplit) context.getInputSplit());
                context.write(new Text(filename + ":begin"), key);
                lastKey = new Text();
            } else {
                if (key.compareTo(lastKey) < 0) {
                    context.write(error,
                            new Text("misorder in " + filename + " last: '" + lastKey + "' current: '" + key + "'"));
                }
            }
            lastKey.set(key);
        }

        @Override
        public void cleanup(final Context context) throws IOException, InterruptedException {
            if (lastKey != null) {
                context.write(new Text(filename + ":end"), lastKey);
            }
        }

    }

    static class ValidateReducer extends Reducer<Text, Text, Text, Text> {
        private boolean firstKey = true;
        private final Text lastKey = new Text();
        private final Text lastValue = new Text();

        public void reduce(final Text key, final Iterator<Text> values, final Context context)
                throws IOException, InterruptedException {
            if (error.equals(key)) {
                while (values.hasNext()) {
                    context.write(key, values.next());
                }
            } else {
                final Text value = values.next();
                if (firstKey) {
                    firstKey = false;
                } else {
                    if (value.compareTo(lastValue) < 0) {
                        context.write(error, new Text("misordered keys last: " + lastKey + " '" + lastValue
                                + "' current: " + key + " '" + value + "'"));
                    }
                }
                lastKey.set(key);
                lastValue.set(value);
            }
        }

    }

    public int run(final String[] args) throws Exception {
        if (args == null || args.length < 2) {
            System.err.println("Usage: bin/hadoop jar " + "big-data-project.jar datavalidate out-dir report-dir");
            return -1;
        }

        final Job job = new Job(getConf());
        final Configuration conf = job.getConfiguration();

        DataInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJobName("DataValidate");
        job.setJarByClass(DataValidate.class);
        job.setMapperClass(ValidateMapper.class);
        job.setReducerClass(ValidateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        conf.setLong("mapred.min.split.size", Long.MAX_VALUE);
        job.setInputFormatClass(DataInputFormat.class);

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(final String[] args) throws Exception {
        final int res = ToolRunner.run(new Configuration(), new DataValidate(), args);
        System.exit(res);
    }

}