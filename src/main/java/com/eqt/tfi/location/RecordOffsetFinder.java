package com.eqt.tfi.location;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.eqt.tfi.input.KeyPathOffsetWritable;
import com.eqt.tfi.input.SequenceFileKeyFileOffsetInputFormat;
import com.eqt.tfi.util.Statics;

/**
 * This job will read over the data found in sequence files and report back
 * each new records starting offset within the file. Meaning it skips the records that are continuations of files
 * So the resulting output will be a key=name, value=file:offset.
 * TODO: Should we skip non start records? Not sure, maybe having every offset would be a good thing?
 * TODO: should we reduce and order these things? Don't think it matters atm.
 * TODO: folders running against in job name would be cool
 */
public class RecordOffsetFinder {

	public static class Mappy extends Mapper<KeyPathOffsetWritable<Text>, Text, Text, Text> {

		Text k = new Text();
		Text v = new Text();
		
		//if the key ends in :0 then we have a starting variable. 
		protected void map(KeyPathOffsetWritable<Text> key, Text value, Context context) throws IOException, InterruptedException {
			String fileKey = key.getKey().toString();
			if(fileKey.endsWith(":0")) {
				context.getCounter("stats", "filesSeen").increment(1);
				k.set(fileKey.substring(0, fileKey.lastIndexOf(":")));
				v.set(key.getPath().toString()+":"+key.getOffset());
				context.write(k, v);
			} else {
				context.getCounter("stats", "chunksSeen").increment(1);
			}
			context.progress();
		}

	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		GenericOptionsParser p = new GenericOptionsParser(args);
		Configuration conf = p.getConfiguration();
		
		Job job = Job.getInstance(conf, "Record Offset Finder");
		job.setJarByClass(RecordOffsetFinder.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(SequenceFileKeyFileOffsetInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Mappy.class);
		
		Path in = null;
		Path out = null;
		
		String[] remainingArgs = p.getRemainingArgs();
		if(remainingArgs.length >= 2) {
			in = new Path(remainingArgs[0]);
			out = new Path(remainingArgs[1]);
		} else {
			in = new Path(System.getProperty(Statics.RECORD_OFFSET_INPUT_PATH));
			out = new Path(System.getProperty(Statics.RECORD_OFFSET_OUTPUT_PATH));
		}
		
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.waitForCompletion(true);
	}

}
