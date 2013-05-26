package org.gramr.kernel;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SelfSimilarityGraphMatching extends Configured implements Tool {
	public static final Log LOG = LogFactory
			.getLog(SelfSimilarityGraphMatching.class);

	static class NodePairInputFormat extends
			FileInputFormat<LongWritable, int[]> {

		@Override
		public RecordReader<LongWritable, int[]> createRecordReader(
				InputSplit is, TaskAttemptContext tac) {
			return new TwoLineRecordReader();
		}

		static class TwoLineRecordReader extends
				RecordReader<LongWritable, int[]> {

			private long start, pos, end;
			private FSDataInputStream fdin;
			private LineReader in;
			private LongWritable key = null;
			private int[] value = null;

			public void initialize(InputSplit genericSplit,
					TaskAttemptContext context) throws IOException {
				FileSplit split = (FileSplit) genericSplit;
				Configuration job = context.getConfiguration();
				pos = start = split.getStart();
				end = start + split.getLength();
				final Path file = split.getPath();

				// open the file and seek to the start of the split
				FSDataInputStream fdin = file.getFileSystem(job).open(
						split.getPath());
				fdin.seek(start);
				in = new LineReader(fdin, job);
			}

			public boolean nextKeyValue() throws IOException {
				// pos = fdin.getPos();
				if (pos >= end)
					return false;

				key = new LongWritable(pos);
				int[] nodes = new int[2];

				int i = 0;
				for (i = 0; i < 2; i++) {
					Text str = new Text();
					if (pos < end) {
						int newSize = in.readLine(str);
						if (newSize > 0) {
							StringTokenizer strTokens = new StringTokenizer(str
									.toString());
							nodes[i] = Integer.parseInt(strTokens.nextToken());
							pos += newSize;
						} else {
							break;
						}
					} else {
						break;
					}
				}

				if (i == 0) {
					key = null;
					value = null;
					return false;
				} else {

					if (i == 2)
						value = nodes;
					else {
						value = new int[i];
						System.arraycopy(nodes, 0, value, 0, i);
					}
					return true;
				}
			}

			@Override
			public LongWritable getCurrentKey() {
				return key;
			}

			@Override
			public int[] getCurrentValue() {
				return value;
			}

			/**
			 * Get the progress within the split
			 */
			public float getProgress() {
				if (start == end) {
					return 0.0f;
				} else {
					return Math
							.min(1.0f, (pos - start) / (float) (end - start));
				}
			}

			public synchronized void close() throws IOException {
				if (in != null) {
					in.close();
				}
			}

		}
	}

	public static class PairNodesMap extends
			Mapper<LongWritable, int[], IntWritable, IntWritable> {

		@Override
		public void map(LongWritable key, int[] value, Context context)
				throws IOException, InterruptedException {
			if (value.length == 2) {
				context.write(new IntWritable(value[0]), new IntWritable(
						value[1]));
			} else {
				context.write(new IntWritable(value[0]), new IntWritable(
						value[0]));
			}
		}
	}

	private int printUsage() {
		LOG.info("SelfSimilarityGraphMatching <input-dir> <output-dir>");
		return 0;
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			printUsage();
			System.exit(0);
		}

		Configuration conf = new Configuration(getConf());
		String input = args[0];
		String output = args[1];

		Job job = null;
		try {
			job = new Job(conf, "Self Similarity Graph Matching");
			job.setJarByClass(SelfSimilarityGraphMatching.class);

			job.setInputFormatClass(NodePairInputFormat.class);
			job.setMapperClass(PairNodesMap.class);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));

			job.setNumReduceTasks(0);
			job.waitForCompletion(true);
		} finally {
			if (job != null) {
				FileUtil.fullyDelete(new File(job.getConfiguration().get(
						"hadoop.tmp.dir")));
			}
		}

		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new SelfSimilarityGraphMatching(), args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
}
