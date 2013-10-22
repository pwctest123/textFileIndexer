package com.eqt.tfi.cleanup;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.eqt.tfi.util.Statics;

/**
 * simple job that fires through a small set of reducers
 * to bring the number of files down to manageable sizes.
 * We don't care that this job takes a long time as its a background process.
 * @author gman
 */
public class SequenceFileCombiner {

	//Identity
	public static class Mappy extends Mapper<Text, Text, Text,Text> {}
	
	public static class Reducy extends Reducer<Text, Text, Text, Text> {}
	
	/**
	 * @param args expects inputPath and outputPath from the command line. [default behavior]
	 * Can also pass in jvm args for in and out as well.
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		GenericOptionsParser p = new GenericOptionsParser(args);
		Configuration conf = p.getConfiguration();
		
		Job job = Job.getInstance(conf, "Sequence File Combiner");
		job.setJarByClass(SequenceFileCombiner.class);
		
		job.setNumReduceTasks(conf.getInt(Statics.COMBINER_NUM_FILES, 1));
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Mappy.class);
		job.setReducerClass(Reducy.class);
		
		Path in = null;
		Path out = null;
		
		String[] remainingArgs = p.getRemainingArgs();
		if(remainingArgs.length >= 2) {
			in = new Path(remainingArgs[0]);
			out = new Path(remainingArgs[1]);
		} else {
			in = new Path(System.getProperty(Statics.INPUT_COMBINER_PATH));
			out = new Path(System.getProperty(Statics.INPUT_COMBINER_OUTPUT_PATH));
		}
		
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.waitForCompletion(true);

	}

}
