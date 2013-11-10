package com.eqt.tfi.index;

import java.io.IOException;

import org.apache.blur.mapreduce.lib.BaseBlurMapper;
import org.apache.blur.mapreduce.lib.BlurOutputFormat;
import org.apache.blur.mapreduce.lib.BlurRecord;
import org.apache.blur.mapreduce.lib.BlurMutate.MUTATE_TYPE;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Example MR indexer, will create a table if it doesn't exist, then index into the thing.
 * Any files picked up (text files only) will be indexed line for line into blur.
 * @author gman
 */
public class TextFileIndexer {
	
	/*
	 * This mapper will get a single file, so recordID will be line position.
	 * rowid will be random gibberish since this is an example after all!
	 */
	public static class TextMapper extends BaseBlurMapper<LongWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//grab a handle on the reusable blur record writable
			BlurRecord record = _mutate.getRecord();
			//clean it out
		    record.clearColumns();
			
		    //set the master/primary key of the blur record. Think of this as a RDBMS PK
		    record.setRowId(System.currentTimeMillis()+"");
		    //set the child/secondary key of the record id. Think of this as an RDBMS child tables PK.
		    record.setRecordId(key.toString());
		    //set family name, equivalent of RDBMS child table name
		    record.setFamily("content");
		    //add column name, equivalent of RDBMS child table column name.
		    record.addColumn("data", value.toString());
		    
		    //setup MR output key
		    _key.set(record.getRowId());
		    //we will replace what existed before.
		    _mutate.setMutateType(MUTATE_TYPE.REPLACE);
		    //send record to reducer for indexing
		    context.write(_key, _mutate);
		    //counters are fun.
		    _recordCounter.increment(1);
		    _columnCounter.increment(1);
		    context.progress();
		}
		
	}
	/**
	 * @param args input path to index. expects plain text files.
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws TException 
	 * @throws BlurException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, BlurException, TException {
		GenericOptionsParser p = new GenericOptionsParser(args);
		Configuration conf = p.getConfiguration();
		String[] otherArgs = p.getRemainingArgs();
		
		//grab a handle to talk to blur, assuming single local instance.
		Iface client = BlurClient.getClient("localhost:40010");
		//the table we will mess with
		String tableName = "textExampleTable";
		//ask blur about the table
		
		TableDescriptor td = null;
		
		//if blur says 'what table?'
		if(!client.tableList().contains(tableName)) { 
			//lets make a table.
			td = new TableDescriptor();
			td.enabled = true;	//so we can use it
			td.shardCount = 1;	//were testing here, just need 1.
			td.name = tableName;//what we call this thing
			td.readOnly = false;//wont hurt letting us mod it via the client api
			
			//we aren't using this because I assume a single cluster, and blur will default to it.
			//td.cluster
			
			//we are not setting this because you set blur.cluster.default.table.uri right?
			//td.tableUri
			
			//'this table blur!'
			client.createTable(td);
			
			//just for example purposes, this is how to specify how to index a column using a built in blur indexer.
			client.addColumnDefinition(tableName,
				//this says, family:content, column:data, no sub index, no full text, "text" indexer, no extra props 
				new ColumnDefinition("content", "data", null, false, "text", null));
		}
		//well, exist or create, we grab the copy blur knows about.
		td = client.describe(tableName);
		
		Job job = Job.getInstance(conf, "Index Text Data");
		job.setJarByClass(TextFileIndexer.class);
		
		Path inputPath = new Path(otherArgs[0]);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(TextMapper.class);
		FileInputFormat.addInputPath(job, inputPath);
		
		//This handles the reducer setting, output types, output path normal in a MR job.
		BlurOutputFormat.setupJob(job, td);
		//blur indexes at the reducer before copying to the final blur location.
		BlurOutputFormat.setIndexLocally(job, true);
		//Lucene optimize operation triggered when blur copies from local index dir to final dir.
		BlurOutputFormat.setOptimizeInFlight(job, true);

		job.waitForCompletion(true);
	}

}
