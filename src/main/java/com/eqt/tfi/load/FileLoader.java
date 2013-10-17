package com.eqt.tfi.load;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

import com.eqt.tfi.file.uri.DatedHashURI;
import com.eqt.tfi.file.uri.UriGenerator;
import com.eqt.tfi.file.validate.FileUploadPolicy;
import com.eqt.tfi.file.validate.FlatFileDedupPolicy;
import com.eqt.tfi.util.Statics;


/**
 * Loads an individual file into HDFS.
 * Uses a configurable URIGenerator to work out where to put the files.
 * Uses a configurable FileValidator to tell if it should even 
 * load the file in the first place.
 * 
 * The URIGenerator is always run to return the potentially new uri/filename, which is then
 * passed to the FileValidator to see if file should be uploaded.
 * 
 * TODO: make file upload format pluggable
 * TODO: make the class wrappable for handling say zips of text files.
 * @author gman
 *
 */
public class FileLoader {

	UriGenerator gen = null;
	FileUploadPolicy policy = null;
	FileSystem fs = null;
	
	/**
	 * Construct a single loader for loading a file
	 * @param pathGen
	 */
	public FileLoader(UriGenerator pathGen, FileUploadPolicy policy, FileSystem fs) {
		this.gen = pathGen;
		this.policy = policy;
		this.fs = fs;
	}
	
	public long load(Path in, boolean localSource, Path prefix, boolean localDest) throws IOException {
		
//		if(localSource)
//			if(!new File(in.toString()).exists())
//				throw new IOException("file does not exist: " + in);
//		else 
			if(!fs.exists(in))
			throw new IOException("file does not exist: " + in);

		Path path = gen.generateDestinationPath(prefix, in);
		
		System.out.println((localSource?"local":"remote") + " file: " + in + " sending to: " + path.toString());
		
		FileContext fx = FileContext.getFileContext(fs.getConf());
		//make sure parent does exist.
		fs.mkdirs(path.getParent());

		SequenceFile.Writer writer = SequenceFile.createWriter(fx, fs.getConf(), path, Text.class,Text.class,
					SequenceFile.CompressionType.BLOCK, new DefaultCodec(), new Metadata(), 
					EnumSet.of(CreateFlag.CREATE,CreateFlag.APPEND), CreateOpts.blockSize(128*1024*1024));

		DataInputStream fis = null;
		long pos = 0;
		byte[] dataBytes = new byte[1024];
		int nread = 0;
		String fileName = path.getName();

		try {
//			if(localSource)
//				fis = new DataInputStream(new FileInputStream(new File(in.toString())));
//			else
				fis = fs.open(in);

			Text key = new Text(fileName+":0");
			Text val = new Text();
			
	        while ((nread = fis.read(dataBytes)) != -1) {
	        	if(nread == -1)
	        		break;
	
	        	key.set(fileName + Statics.KEY_DELIM + pos);
	        	val.set(dataBytes,0,nread);
	
	        	pos+=nread;
	        	writer.append(key, val);
	        };
		}catch(IOException e){
			throw e;
		} finally {
			if(fis != null)
				fis.close();
			if(writer != null)
				writer.close();
		}
        System.out.println("bytes read: " + pos);
        return pos;
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		FileLoader f = new FileLoader(new DatedHashURI(fs), FlatFileDedupPolicy.getInstance(),fs);
		f.load(new Path("/tmp/fark"), true, new Path("file:///tmp"), true);
	}

}
