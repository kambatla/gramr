package org.gramr.kernel;


import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
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

public class ComputeMaxRankDifference extends Configured implements Tool {
	public static final Log LOG = LogFactory.getLog(NaiveMatVec.class);

	public static class ExtractRank extends
			Mapper<LongWritable, Text, IntWritable, FloatWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			RankedAdjList adj = RankedAdjList.parseRankedAdjList(value
					.toString());

			context.write(new IntWritable(adj.getSrc()), new FloatWritable(adj
					.getRank()));
		}
	}

	public static class ExtractDeltas extends
			Mapper<LongWritable, Text, IntWritable, FloatWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer tokens = new StringTokenizer(value.toString());

			int src;
			float rank;
			try {
				src = Integer.parseInt(tokens.nextToken());
				rank = Float.parseFloat(tokens.nextToken());
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}

			context.write(new IntWritable(1), new FloatWritable(rank));
		}
	}

	public static class ComputeDelta extends
			Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {

		public void reduce(IntWritable key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float rank = -1f;

			for (FloatWritable val : values) {
				if (rank == -1f) {
					rank = val.get();
				} else {
					rank = Math.abs(rank - val.get());
				}
			}

			context.write(key, new FloatWritable(rank));
		}
	}

	public static class MaxDelta extends
			Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {

		public void reduce(IntWritable key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float rank = Float.MIN_VALUE;

			for (FloatWritable val : values) {
				float valRank = val.get();
				if (valRank > rank)
					rank = valRank;
			}

			context.write(new IntWritable(1), new FloatWritable(rank));
		}
	}

	private static int printUsage() {
		System.out.println("Usage: naive.MatVec <src> <dst> <iterations>");
		return 0;
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			printUsage();
			System.exit(0);
		}

		Configuration conf = new Configuration(getConf());

		String input1 = args[0];
		String input2 = args[1];
		String output = args[2];

		Job job1 = null;
		try {
			job1 = new Job(conf, "ComputeMaxDifference: compute deltas");
			job1.setJarByClass(ComputeMaxRankDifference.class);
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setMapperClass(ExtractRank.class);
			job1.setMapOutputKeyClass(IntWritable.class);
			job1.setMapOutputValueClass(FloatWritable.class);
			job1.setReducerClass(ComputeDelta.class);
			job1.setOutputKeyClass(IntWritable.class);
			job1.setOutputValueClass(FloatWritable.class);
			job1.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job1, new Path(input1));
			FileInputFormat.addInputPath(job1, new Path(input2));
			FileOutputFormat.setOutputPath(job1, new Path("./tmp/" + output));

			job1.setNumReduceTasks(new JobClient(new JobConf(conf))
					.getClusterStatus().getMaxReduceTasks());

			job1.waitForCompletion(true);
		} finally {
			if (job1 != null) {
				FileUtil.fullyDelete(new File(job1.getConfiguration().get(
						"hadoop.tmp.dir")));
			}
		}

		Job job2 = null;
		try {
			job2 = new Job(conf, "ComputeMaxDifference: finding max of deltas");
			job2.setJarByClass(ComputeMaxRankDifference.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setMapperClass(ExtractDeltas.class);
			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(FloatWritable.class);
			job2.setCombinerClass(MaxDelta.class);
			job2.setReducerClass(MaxDelta.class);
			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(FloatWritable.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job2, new Path("./tmp/" + output));
			FileOutputFormat.setOutputPath(job2, new Path(output));

			job2.setNumReduceTasks(new JobClient(new JobConf(conf))
					.getClusterStatus().getMaxReduceTasks());

			job2.waitForCompletion(true);
		} finally {
			if (job2 != null) {
				FileUtil.fullyDelete(new File(job1.getConfiguration().get(
						"hadoop.tmp.dir")));
			}
		}

		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new ComputeMaxRankDifference(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
