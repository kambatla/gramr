package org.gramr.kernel;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.gramr.common.RankedAdjList;

public class SortRankedGraphByRank extends Configured implements Tool {
	public static final Log LOG = LogFactory
			.getLog(SortRankedGraphByRank.class);

	@SuppressWarnings("unchecked")
	private static class Rank implements WritableComparable {
		private float rank;

		Rank() {

		}

		Rank(float rank) {
			this.rank = rank;
		}

		public float getRank() {
			return this.rank;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.rank = in.readFloat();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeFloat(this.rank);
		}

		@Override
		public int compareTo(Object other) {
			Rank otherRank = (Rank) other;
			if (this.rank > otherRank.getRank())
				return -1;
			else if (this.rank < otherRank.getRank())
				return 1;
			else
				return 0;
		}
	}

	public static class SortVecMapper extends
			Mapper<LongWritable, Text, Rank, IntWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			RankedAdjList adj = RankedAdjList.parseRankedAdjList(value
					.toString());
			context.write(new Rank(adj.getRank()),
					new IntWritable(adj.getSrc()));
		}
	}

	public static class SortVecReducer extends
			Reducer<Rank, IntWritable, IntWritable, FloatWritable> {
		public void reduce(Rank key, Iterable<IntWritable> nodes,
				Context context) throws IOException, InterruptedException {
			for (IntWritable node : nodes)
				context.write(node, new FloatWritable(key.getRank()));
		}
	}

	private static int printUsage() {
		System.out
				.println("Usage: org.gramr.kernel.SortRankedGraphByRank <Graph> <SortedVector>");
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
			job = new Job(conf, "Sort RankedGraph by Rank");
			job.setJarByClass(SortRankedGraphByRank.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setMapperClass(SortVecMapper.class);
			job.setMapOutputKeyClass(Rank.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setReducerClass(SortVecReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(LongWritable.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));

			// job.setNumReduceTasks(new JobClient(new JobConf(conf))
			// .getClusterStatus().getMaxReduceTasks());
			job.setNumReduceTasks(1);
			job.waitForCompletion(true);
		} finally {
			if (job != null) {
				FileUtil.fullyDelete(new File(job.getConfiguration().get(
						"hadoop.tmp.dir")));
			}
		}

		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new SortRankedGraphByRank(),
					args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
}
