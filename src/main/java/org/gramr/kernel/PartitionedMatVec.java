package org.gramr.kernel;

import org.gramr.io.PartitionInputFormat;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import org.gramr.common.RankedAdjList;

import org.gramr.utils.GenericOptionsParser;

public class PartitionedMatVec extends MatVec {
	public static final Log LOG = LogFactory.getLog(PartitionedMatVec.class);

	public static class MatVecPartitioner extends
			Partitioner<IntWritable, MatVecMapOutputValue> {

		@Override
		public int getPartition(IntWritable src, MatVecMapOutputValue value,
				int numPartitions) {
			int partitionNumber = src.get() / 262144;
			return (partitionNumber & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static class MatVecMapper
			extends
			Mapper<IntWritable, List<RankedAdjList>, IntWritable, MatVecMapOutputValue> {

		public void map(IntWritable key, List<RankedAdjList> listAdjList,
				Context context) throws IOException, InterruptedException {
			int numLocalIterations = context.getConfiguration().getInt(
					"NUM_LOCAL_ITERATIONS", 1);
			LOG.info("# rows = " + listAdjList.size() + " starting from "
					+ listAdjList.get(0).getSrc() + "\t#LocalIterations = "
					+ numLocalIterations);

			HashSet<Integer> localNodes = new HashSet<Integer>();
			Hashtable<Integer, Float> localRanks = new Hashtable<Integer, Float>();

			for (RankedAdjList adj : listAdjList) {
				localNodes.add(adj.getSrc());
			}

			// local iterations
			for (int i = 1; i <= numLocalIterations; i++) {
				LOG.info("Local Iteration: " + i);
				for (RankedAdjList adj : listAdjList) {
					float rank = adj.getRank();
					int degree = adj.getNumOutLinks();
					for (int dst : adj.getDst()) {
						if (!localNodes.contains(dst))
							continue;

						Float oldrank = localRanks.remove(dst);
						if (oldrank == null) {
							oldrank = 0.0f;
						}
						localRanks.put(dst, oldrank + rank / degree);
					}
				}
			}

			for (RankedAdjList adj : listAdjList) {
				int src = adj.getSrc();
				int degree = adj.getNumOutLinks();
				Float rank = localRanks.remove(src);
				if (rank == null) {
					rank = adj.getRank();
				}
				context.write(new IntWritable(adj.getSrc()),
						MatVecMapOutputValue.createAdjListValue(adj.getRank(),
								degree, adj.getDst()));

				for (int dst : adj.getDst()) {
					if (localNodes.contains(dst))
						continue;

					context.write(new IntWritable(dst), MatVecMapOutputValue
							.createEdgeValue(rank / degree));
				}
			}
		}
	}

	protected int printUsage() {
		System.out
				.println("Usage: org.gramr.kernel.PartitionedMatVec <src> <dst> <-subgraph <size>> "
						+ " [-outer <iterations>] [-inner <iterations>] [-threshold <iterations>");
		return 0;
	}

	public int run(String[] args) throws Exception {
		if (args.length < 4) {
			printUsage();
			System.exit(0);
		}

		Configuration conf = new Configuration(getConf());
		String input = args[0];
		String output = args[1];

		int subgraphSize = 0, numIterations = 0, numLocalIterations = 1;
		float threshold = -1f;

		try {
			String subgraphString = GenericOptionsParser.getOptionValue(args,
					"subgraph");
			if (subgraphString != null)
				subgraphSize = Integer.parseInt(subgraphString);

			String innerString = GenericOptionsParser.getOptionValue(args,
					"inner");
			if (innerString != null)
				numLocalIterations = Integer.parseInt(innerString);

			String outerString = GenericOptionsParser.getOptionValue(args,
					"outer");
			if (outerString != null)
				numIterations = Integer.parseInt(outerString);

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
		
		if (subgraphSize == 0)
			printUsage();

		if (numIterations == 0) {
			if (threshold == -1f)
				numIterations = 1;
			else if (threshold >= 0)
				numIterations = Integer.MAX_VALUE;
		}

		conf.setInt("GRAPH_SUBGRAPH_SIZE", subgraphSize);
		conf.setInt("NUM_LOCAL_ITERATIONS", numLocalIterations);
		conf.setFloat(thresholdConfValue, threshold);
		System.out.println("# Iterations = " + numIterations + "Threshold = " + threshold);

		for (int i = 1; i <= numIterations; i++) {

			Job job = null;
			try {
				job = new Job(conf, "PartitionedMatVec - " + i);
				job.setJarByClass(PartitionedMatVec.class);

				job.setInputFormatClass(PartitionInputFormat.class);
				job.setMapperClass(MatVecMapper.class);
				job.setMapOutputKeyClass(IntWritable.class);
				job.setMapOutputValueClass(MatVecMapOutputValue.class);
				job.setCombinerClass(MatVecCombiner.class);
				job.setPartitionerClass(MatVecPartitioner.class);
				job.setReducerClass(MatVecReducer.class);
				job.setOutputKeyClass(NullWritable.class);
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
			ToolRunner.run(new Configuration(), new PartitionedMatVec(), args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
}
