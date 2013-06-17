package org.gramr.utils.webgraph;

import org.gramr.io.RandomInputFormat;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import org.gramr.common.AdjList;
import org.gramr.common.RankedAdjList;

public class ConvertWebgraphToRanked extends Configured implements Tool {
	public static final Log LOG = LogFactory
			.getLog(ConvertWebgraphToRanked.class);
	private static final String WEBGRAPH_BASENAME = "WEBGRAPH_BASENAME";

	private static final String extGraph = ".graph";
	private static final String extProperties = ".properties";
	private static final String extOffsets = ".offsets";

	public static class ConvertWebgraphMapper extends
			Mapper<Text, Text, NullWritable, RankedAdjList> {

		private BVGraphUtil graph = null;
		private int numSplits;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			numSplits = conf.getInt(RandomInputFormat.NUM_SPLITS, 32);

			if (graph == null) {
				FileSystem localfs = FileSystem.getLocal(conf);
				String basename = conf.get(WEBGRAPH_BASENAME);
				Path basenameDir = null;

				Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
				for (Path p : localFiles) {
					if (basenameDir == null) {
						basenameDir = new Path(p.getParent().getParent(),
								basename);
						localfs.mkdirs(basenameDir);
					}

					Path newpath = new Path(basenameDir, p.getName());
					if (!localfs.exists(newpath))
						localfs.rename(p, newpath);
				}

				graph = new BVGraphUtil(basenameDir.toString() + "/" + basename);
				LOG.info("Number of nodes in graph = " + graph.getNumNodes());
				graph.splitVerticesEqually(numSplits);
			}
		}

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			int split = Integer.parseInt(key.toString());
			graph.readRange(split);

			RankedAdjList ral;
			while ((ral = graph.getNextRankedAdjList()) != null) {
				context.write(NullWritable.get(), ral);
			}
		}
	}

	private static void printUsage() {
		System.out
				.println("hadoop jar <jar> org.gramr.utils.webgraph.ConvertWebgraphToRanked <inputDir> <basename> <output>");
	}

	public int run(String[] args) throws Exception {
		if (args.length < 3 || args.length > 4) {
			printUsage();
			System.exit(0);
		}

		String inputDir = args[0];
		String basename = args[1];
		String output = args[2];
		int numSplits = -1;

		if (args.length == 4)
			numSplits = Integer.parseInt(args[3]);

		Configuration conf = new Configuration(getConf());

		if (numSplits == -1)
			numSplits = new JobClient(new JobConf(conf)).getClusterStatus()
					.getMaxMapTasks();
		conf.setInt(RandomInputFormat.NUM_SPLITS, numSplits);

		FileSystem fs = FileSystem.get(conf);
		Path graphFile = new Path(inputDir, basename + extGraph);
		Path propertiesFile = new Path(inputDir, basename + extProperties);
		Path offsetsFile = new Path(inputDir, basename + extOffsets);
		if (!(fs.exists(graphFile) & fs.exists(propertiesFile))) {
			LOG.error("Invalid input file");
			return -1;
		}

		conf.set(WEBGRAPH_BASENAME, basename);
		DistributedCache.addCacheFile(graphFile.toUri(), conf);
		DistributedCache.addCacheFile(propertiesFile.toUri(), conf);
		DistributedCache.addCacheFile(offsetsFile.toUri(), conf);

		Job job = null;
		try {
			job = new Job(conf, "Convert AdjList to Ranked");
			job.setJarByClass(ConvertWebgraphToRanked.class);
			job.setInputFormatClass(RandomInputFormat.class);

			job.setMapperClass(ConvertWebgraphMapper.class);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(RankedAdjList.class);

			job.setNumReduceTasks(0);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(RankedAdjList.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(output));

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
			ToolRunner.run(new Configuration(), new ConvertWebgraphToRanked(),
					args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
}
