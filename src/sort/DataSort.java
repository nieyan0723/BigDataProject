package sort;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import common.DataOutputFormat;
import common.DataInputFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DataSort extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(DataSort.class);

    static class TotalOrderPartitioner extends Partitioner<Text, Text> {
        private TrieNode trie;
        private Text[] splitPoints;

        static abstract class TrieNode {
            private final int level;

            TrieNode(final int level) {
                this.level = level;
            }

            abstract int findPartition(Text key);

            abstract void print(PrintStream strm) throws IOException;

            int getLevel() {
                return level;
            }
        }

        static class InnerTrieNode extends TrieNode {
            private final TrieNode[] child = new TrieNode[256];

            InnerTrieNode(final int level) {
                super(level);
            }

            int findPartition(final Text key) {
                final int level = getLevel();
                if (key.getLength() <= level) {
                    return child[0].findPartition(key);
                }
                return child[key.getBytes()[level]].findPartition(key);
            }

            void setChild(final int idx, final TrieNode child) {
                this.child[idx] = child;
            }

            void print(final PrintStream strm) throws IOException {
                for (int ch = 0; ch < 255; ++ch) {
                    for (int i = 0; i < 2 * getLevel(); ++i) {
                        strm.print(' ');
                    }
                    strm.print(ch);
                    strm.println(" ->");
                    if (child[ch] != null) {
                        child[ch].print(strm);
                    }
                }
            }
        }

        static class LeafTrieNode extends TrieNode {
            int lower;
            int upper;
            Text[] splitPoints;

            LeafTrieNode(final int level, final Text[] splitPoints, final int lower, final int upper) {
                super(level);
                this.splitPoints = splitPoints;
                this.lower = lower;
                this.upper = upper;
            }

            int findPartition(final Text key) {
                for (int i = lower; i < upper; ++i) {
                    if (splitPoints[i].compareTo(key) >= 0) {
                        return i;
                    }
                }
                return upper;
            }

            void print(final PrintStream strm) throws IOException {
                for (int i = 0; i < 2 * getLevel(); ++i) {
                    strm.print(' ');
                }
                strm.print(lower);
                strm.print(", ");
                strm.println(upper);
            }
        }

        private static Text[] readPartitions(final FileSystem fs, final Path p, final Configuration conf)
                throws IOException {
            final SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
            final List<Text> parts = new ArrayList<Text>();
            Text key = new Text();
            final NullWritable value = NullWritable.get();
            while (reader.next(key, value)) {
                parts.add(key);
                key = new Text();
            }
            reader.close();
            return parts.toArray(new Text[parts.size()]);
        }

        private static TrieNode buildTrie(final Text[] splits, int lower, final int upper, final Text prefix,
                final int maxDepth) {
            final int depth = prefix.getLength();
            if (depth >= maxDepth || lower == upper) {
                return new LeafTrieNode(depth, splits, lower, upper);
            }
            final InnerTrieNode result = new InnerTrieNode(depth);
            final Text trial = new Text(prefix);
            // append an extra byte on to the prefix
            trial.append(new byte[1], 0, 1);
            int currentBound = lower;
            for (int ch = 0; ch < 255; ++ch) {
                trial.getBytes()[depth] = (byte) (ch + 1);
                lower = currentBound;
                while (currentBound < upper) {
                    if (splits[currentBound].compareTo(trial) >= 0) {
                        break;
                    }
                    currentBound += 1;
                }
                trial.getBytes()[depth] = (byte) ch;
                result.child[ch] = buildTrie(splits, lower, currentBound, trial, maxDepth);
            }
            // pick up the rest
            trial.getBytes()[depth] = 127;
            result.child[255] = buildTrie(splits, currentBound, upper, trial, maxDepth);
            return result;
        }

        public void configure(final Configuration conf) {
            try {
                final FileSystem fs = FileSystem.getLocal(conf);
                final Path partFile = new Path(DataInputFormat.PARTITION_FILENAME);
                splitPoints = readPartitions(fs, partFile, conf);
                trie = buildTrie(splitPoints, 0, splitPoints.length, new Text(), 2);
            } catch (final IOException ie) {
                throw new IllegalArgumentException("can't read paritions file", ie);
            }
        }

        public TotalOrderPartitioner() {
            configure(new Configuration());
        }

        public int getPartition(final Text key, final Text value, final int numPartitions) {
            return trie.findPartition(key);
        }

    }

    public int run(final String[] args) throws Exception {
        if (args == null || args.length < 2) {
            System.err.println("Usage: bin/hadoop jar " + "big-data-project.jar datasort in-dir out-dir");
            return -1;
        }

        final Job job = new Job(getConf());
        final Configuration conf = job.getConfiguration();
        Path inputDir = new Path(args[0]);
        inputDir = inputDir.makeQualified(inputDir.getFileSystem(conf));
        final Path partitionFile = new Path(inputDir, DataInputFormat.PARTITION_FILENAME);
        final URI partitionUri = new URI(partitionFile.toString() + "#" + DataInputFormat.PARTITION_FILENAME);
        DataInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJobName("DataSort");
        job.setJarByClass(DataSort.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(DataInputFormat.class);
        job.setOutputFormatClass(DataOutputFormat.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        DataInputFormat.writePartitionFile(job, partitionFile);
        DistributedCache.addCacheFile(partitionUri, conf);
        DistributedCache.createSymlink(conf);
        conf.setInt("dfs.replication", 1);
        DataOutputFormat.setFinalSync(job, true);

        job.waitForCompletion(true);

        LOG.info("done");
        return 0;
    }

    public static void main(final String[] args) throws Exception {
        final int res = ToolRunner.run(new Configuration(), new DataSort(), args);
        System.exit(res);
    }

}