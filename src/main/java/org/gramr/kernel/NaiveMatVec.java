package org.gramr.kernel;


import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import org.gramr.common.RankedAdjList;

import org.gramr.utils.GenericOptionsParser;

public class NaiveMatVec extends MatVec {
	public static final String thresholdConfValue = "MATVEC_THRESHOLD_VALUE";
	public static final Log LOG = LogFactory.getLog(NaiveMatVec.class);

	public static class MatVecMapper extends
			Mapper<LongWritable, Text, IntWritable, MatVecMapOutputValue> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			RankedAdjList adj = RankedAdjList.parseRankedAdjList(value
					.toString());
			int numOutLinks = adj.getNumOutLinks();
			context.write(new IntWritable(adj.getSrc()), MatVecMapOutputValue
					.createAdjListValue(adj.getRank(), numOutLinks, adj
							.getDst()));

			if (numOutLinks == 0) {
				return;
			}

			float rank = adj.getRank();
			for (int dst : adj.getDst()) {
				context.write(new IntWritable(dst), MatVecMapOutputValue
						.createEdgeValue(rank / numOutLinks));
			}
		}
	}

	
	protected int printUsage() {
		System.out
				.println("Usage: org.gramr.kernel.NaiveMatVec <src> <dst> [-iter <#iterations (int)>] [-threshold <threshold (float)>]");
		return 0;
	}

	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			printUsage();
			return -1;
		}

		String input = args[0];
		String output = args[1];

		int numIterations = 0;
		float threshold = -1f;

		try {
			String iterString = GenericOptionsParser.getOptionValue(args,
					"iter");
			if (iterString != null)
				numIterations = Integer.parseInt(iterString);

			String thresholdString = GenericOptionsParser.getOptionValue(args,
					"threshold");
			if (thresholdString != null) {
				threshold = Float.parseFloat(thresholdString);
				if (threshold < 0) {
					threshold = 0;
					System.out
							.println("Threshold can't be negative, using zero instead.");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			printUsage();
			return -1;
		}

		if (numIterations == 0) {
			if (threshold == -1f)
				numIterations = 1;
			else if (threshold >= 0)
				numIterations = Integer.MAX_VALUE;
		}

		System.out.println("MatVec #Iterations = " + numIterations
				+ " Threshold = " + threshold);

		Configuration conf = new Configuration(getConf());
		conf.setFloat(thresholdConfValue, threshold);
		for (int i = 1; i <= numIterations; i++) {
			Job job = null;
			try {
				job = new Job(conf, "NaiveMatVec-" + i);
				job.setJarByClass(NaiveMatVec.class);
				job.setInputFormatClass(TextInputFormat.class);
				job.setMapperClass(MatVecMapper.class);
				job.setMapOutputKeyClass(IntWritable.class);
				job.setMapOutputValueClass(MatVecMapOutputValue.class);
				job.setCombinerClass(MatVecCombiner.class);
				job.setReducerClass(MatVecReducer.class);
				job.setOutputKeyClass(LongWritable.class);
				job.setOutputValueClass(Text.class);
				job.setOutputFormatClass(TextOutputFormat.class);

				if (i == 1) {
					FileInputFormat.addInputPath(job, new Path(input));
				} else {
					FileInputFormat.addInputPath(job, new Path(output + "/iter"
							+ (i - 1)));
				}
				FileOutputFormat.setOutputPath(job, new Path(output + "/iter"
						+ i));

				job.setNumReduceTasks(new JobClient(new JobConf(conf))
						.getClusterStatus().getMaxReduceTasks());

				LOG.info("\tIteration: " + i);
				job.waitForCompletion(true);

				if (checkConvergence(job))
					break;
			} finally {
				if (job != null) {
					FileUtil.fullyDelete(new File(job.getConfiguration().get(
							"hadoop.tmp.dir")));
				}
			}
		}

		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new NaiveMatVec(), args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
}
