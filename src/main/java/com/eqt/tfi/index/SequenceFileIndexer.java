package com.eqt.tfi.index;

import java.io.IOException;

import org.apache.blur.mapreduce.lib.BaseBlurMapper;
import org.apache.blur.mapreduce.lib.BlurOutputFormat;
import org.apache.blur.mapreduce.lib.BlurRecord;
import org.apache.blur.mapreduce.lib.BlurMutate.MUTATE_TYPE;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.eqt.tfi.util.Statics;

/**
 * TODO: params for, index name, additional mappers for meta,
 * overrides for table creation.
 * @author gman
 */
public class SequenceFileIndexer {

	public static class SequenceMapper extends BaseBlurMapper<Text, Text> {

		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			BlurRecord record = _mutate.getRecord();
		    record.clearColumns();
			
		    String[] keys = key.toString().split(Statics.KEY_DELIM);
		    
		    record.setRowId(keys[0]);
		    record.setRecordId(keys[1]);
		    record.setFamily(Statics.DEFAULT_CONTENT_FAMILY);
		    record.addColumn(Statics.DEFAULT_CONTENT_FIELD, value.toString());
		    _key.set(record.getRowId());
		    _mutate.setMutateType(MUTATE_TYPE.REPLACE);
		    context.write(_key, _mutate);
		    _recordCounter.increment(1);
		    _columnCounter.increment(1);
		    context.progress();
		}
		
	}
	
	/**
	 * TODO: handle Blur exceptions nicely.
	 * @param args
	 * @throws IOException 
	 * @throws TException 
	 * @throws BlurException
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, BlurException, TException, ClassNotFoundException, InterruptedException {
		GenericOptionsParser p = new GenericOptionsParser(args);
		Configuration conf = p.getConfiguration();
		
		Iface client = BlurClient.getClient(conf.get(Statics.BLUR_CONTROLLER));
		TableDescriptor tableDescriptor = client.describe(conf.get(Statics.BLUR_TFI_TABLE_NAME,Statics.BLUR_TFI_DEFAULT_TABLE_NAME));
		
		if(tableDescriptor == null && conf.getBoolean(Statics.BLUR_TFI_TABLE_CREATE_DEFAULT,false)) {
			tableDescriptor = Statics.getDefaultTableDescriptor(client);
			//TODO: get this
//			tableDescriptor.setTableUri(tableUri)
		}
		
		Job job = Job.getInstance(conf, "Index Text Data");
		job.setJarByClass(SequenceFileIndexer.class);
		
		Path inputPath = new Path(conf.get(Statics.INPUT_CONTENT_FOLDER));
		
		MultipleInputs.addInputPath(job, inputPath, SequenceFileInputFormat.class, SequenceMapper.class);
		
		BlurOutputFormat.setupJob(job, tableDescriptor);
		BlurOutputFormat.setIndexLocally(job, true);
		BlurOutputFormat.setOptimizeInFlight(job, true);

		job.waitForCompletion(true);
		//TODO: checks for success
	}

}
