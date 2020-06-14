package generate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import common.DataOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The format of the data is:
 * <ul>
 * <li>(10 bytes key) (10 bytes rowid) (78 bytes filler) \r \n
 * <li>The keys are random characters from the set ' ' .. '~'.
 * <li>The rowid is the right justified row id as a int.
 * <li>The filler consists of 7 runs of 10 characters from 'A' to 'Z'.
 * </ul>
 */
public class DataGen extends Configured implements Tool {

    static class RangeInputFormat extends InputFormat<LongWritable, NullWritable> {

        static class RangeInputSplit extends InputSplit implements Writable {
            long firstRow;
            long rowCount;

            public RangeInputSplit() {
            }

            public RangeInputSplit(long offset, long length) {
                firstRow = offset;
                rowCount = length;
            }

            @Override
            public long getLength() throws IOException {
                return 0;
            }

            @Override
            public String[] getLocations() throws IOException {
                return new String[] {};
            }

            @Override
            public void readFields(DataInput in) throws IOException {
                firstRow = WritableUtils.readVLong(in);
                rowCount = WritableUtils.readVLong(in);
            }

            @Override
            public void write(DataOutput out) throws IOException {
                WritableUtils.writeVLong(out, firstRow);
                WritableUtils.writeVLong(out, rowCount);
            }
        }

        static class RangeRecordReader extends RecordReader<LongWritable, NullWritable> {
            long startRow;
            long finishedRows;
            long totalRows;
            private LongWritable currentKey = new LongWritable();

            public RangeRecordReader(RangeInputSplit split) {
                startRow = split.firstRow;
                finishedRows = 0;
                totalRows = split.rowCount;
            }

            @Override
            public void close() throws IOException {
                // NOTHING
            }

            @Override
            public float getProgress() throws IOException {
                return finishedRows / (float) totalRows;
            }

            @Override
            public LongWritable getCurrentKey() throws IOException, InterruptedException {
                return currentKey;
            }

            @Override
            public NullWritable getCurrentValue() throws IOException, InterruptedException {
                return NullWritable.get();
            }

            @Override
            public void initialize(InputSplit split, TaskAttemptContext context)
                    throws IOException, InterruptedException {
                // NOTHING
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                if (finishedRows < totalRows) {
                    currentKey.set(startRow + finishedRows);
                    finishedRows += 1;
                    return true;
                } else {
                    return false;
                }
            }

        }

        @Override
        public RecordReader<LongWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            return new RangeRecordReader((RangeInputSplit) split);
        }

        @Override
        public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
            long totalRows = getNumberOfRows(context.getConfiguration());
            int numSplits = context.getConfiguration().getInt("mapred.map.tasks", 2);
            long rowsPerSplit = totalRows / numSplits;

            System.out
                    .println("Generating " + totalRows + " using " + numSplits + " maps with step of " + rowsPerSplit);

            List<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
            long currentRow = 0;
            for (int split = 0; split < numSplits - 1; ++split) {
                splits.add(new RangeInputSplit(currentRow, rowsPerSplit));
                currentRow += rowsPerSplit;
            }
            splits.add(new RangeInputSplit(currentRow, totalRows - currentRow));
            return splits;
        }

    }

    static long getNumberOfRows(Configuration job) {
        return job.getLong("datasort.num-rows", 0);
    }

    static void setNumberOfRows(Configuration job, long numRows) {
        job.setLong("datasort.num-rows", numRows);
    }

    static class RandomGenerator {
        private long seed = 0;
        private static final long mask32 = (1l << 32) - 1;
        /**
         * The number of iterations separating the precomputed seeds.
         */
        private static final int seedSkip = 128 * 1024 * 1024;
        /**
         * The precomputed seed values after every seedSkip iterations. There should be
         * enough values so that a 2**32 iterations are covered.
         */
        private static final long[] seeds = new long[] { 0L, 4160749568L, 4026531840L, 3892314112L, 3758096384L,
                3623878656L, 3489660928L, 3355443200L, 3221225472L, 3087007744L, 2952790016L, 2818572288L, 2684354560L,
                2550136832L, 2415919104L, 2281701376L, 2147483648L, 2013265920L, 1879048192L, 1744830464L, 1610612736L,
                1476395008L, 1342177280L, 1207959552L, 1073741824L, 939524096L, 805306368L, 671088640L, 536870912L,
                402653184L, 268435456L, 134217728L, };

        RandomGenerator(long initalIteration) {
            int baseIndex = (int) ((initalIteration & mask32) / seedSkip);
            seed = seeds[baseIndex];
            for (int i = 0; i < initalIteration % seedSkip; ++i) {
                next();
            }
        }

        RandomGenerator() {
            this(0);
        }

        long next() {
            seed = (seed * 3141592621l + 663896637) & mask32;
            return seed;
        }
    }

    public static class SortGenMapper extends Mapper<LongWritable, NullWritable, Text, Text> {

        private Text key = new Text();
        private Text value = new Text();
        private RandomGenerator rand;
        private byte[] keyBytes = new byte[12];
        private byte[] spaces = "          ".getBytes();
        private byte[][] filler = new byte[26][];
        {
            for (int i = 0; i < 26; ++i) {
                filler[i] = new byte[10];
                for (int j = 0; j < 10; ++j) {
                    filler[i][j] = (byte) ('A' + i);
                }
            }
        }

        private void addKey() {
            for (int i = 0; i < 3; i++) {
                long temp = rand.next() / 52;
                keyBytes[3 + 4 * i] = (byte) (' ' + (temp % 95));
                temp /= 95;
                keyBytes[2 + 4 * i] = (byte) (' ' + (temp % 95));
                temp /= 95;
                keyBytes[1 + 4 * i] = (byte) (' ' + (temp % 95));
                temp /= 95;
                keyBytes[4 * i] = (byte) (' ' + (temp % 95));
            }
            key.set(keyBytes, 0, 10);
        }

        private void addRowId(long rowId) {
            byte[] rowid = Integer.toString((int) rowId).getBytes();
            int padSpace = 10 - rowid.length;
            if (padSpace > 0) {
                value.append(spaces, 0, 10 - rowid.length);
            }
            value.append(rowid, 0, Math.min(rowid.length, 10));
        }

        private void addFiller(long rowId) {
            int base = (int) ((rowId * 8) % 26);
            for (int i = 0; i < 7; ++i) {
                value.append(filler[(base + i) % 26], 0, 10);
            }
            value.append(filler[(base + 7) % 26], 0, 8);
        }

        /**
         * Add the required filler bytes. Each row consists of 7 blocks of 10 characters
         * and 1 block of 8 characters.
         */
        protected void map(LongWritable row, NullWritable ignored, Context context)
                throws IOException, InterruptedException {
            long rowId = row.get();
            if (rand == null) {
                // we use 3 random numbers per a row
                rand = new RandomGenerator(rowId * 3);
            }
            addKey();
            value.clear();
            addRowId(rowId);
            addFiller(rowId);
            context.write(key, value);
        }

    }

    public int run(String[] args) throws IOException {

        if (args == null || args.length < 2) {
            System.err.println("Usage: bin/hadoop jar " + "big-data-project.jar datagen num_rows dir");
            return -1;
        }

        Job job = new Job(getConf());
        setNumberOfRows(job.getConfiguration(), Long.parseLong(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJobName("DataGen");
        job.setJarByClass(DataGen.class);
        job.setMapperClass(SortGenMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(RangeInputFormat.class);
        job.setOutputFormatClass(DataOutputFormat.class);

        try {
            job.waitForCompletion(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DataGen(), args);
        System.exit(res);
    }

}