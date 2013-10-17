package com.eqt.tfi.daemon;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.eqt.tfi.file.uri.DatedHashURI;
import com.eqt.tfi.file.validate.FlatFileDedupPolicy;
import com.eqt.tfi.load.FileLoader;

/**
 * Watches a given directory and uploads files to the given destination in a
 * threaded manor.
 * 
 * Note: that this watcher expects the file to be complete when dropped off into
 * its watch dir. As in a file move command was issued and not a copy.
 * 
 * TODO: make policy and validator pluggable options.
 * TODO: consider a hidden file policy to allow people to copy into the dir??
 * 
 * @author gman
 * 
 */
public class TFIDaemon {

	Path inputPath;
	final boolean localWatch;
	Path destPath;
	final boolean localDest;
	
	Configuration conf;
	final FileSystem fs;

	public TFIDaemon(String watch, String dest) throws IOException, InterruptedException {
		conf = new Configuration();
		fs = FileSystem.get(conf);		
		
		FileSystem localFS = FileSystem.getLocal(conf);
		
		if(!watch.endsWith("/"))
			watch += "/";
		//TODO: make more robust, if not hdfs: resolve fully local path to handle ../bla or ~/bla
		if(watch.startsWith("hdfs:")) {
			localWatch = false;
			inputPath = new Path(watch);
		} else { //if not explicit assumed local
			localWatch = true;
			inputPath = localFS.makeQualified(new Path(watch));
			if(!localFS.exists(inputPath))
				throw new IOException("cannot find local path: " + watch);
		}
				
		
		if(!dest.endsWith("/"))
			dest+="/";
		
		if(dest.startsWith("hdfs:")) {
			localDest = false;
			destPath = new Path(dest);
		} else {
			localDest = true;
			destPath = localFS.makeQualified(new Path(watch));
			if(!localFS.exists(destPath))
				throw new IOException("cannot find destination path: " + dest);
		}
		
		System.out.println("Watch Path: " + inputPath.toString());
		System.out.println("Destination Path: " + destPath.toString());

	}
	
	public void run() throws IOException, InterruptedException {


		// TODO: parameterize threading
		final int maxNumThreads = 10;
		ExecutorService executor = Executors.newFixedThreadPool(maxNumThreads);
		// keep track of the number of current threads running
		int currLoads = 0;
		// used for landing into the map of futures
		int currThreadNum = 0;
		Map<String, Future<FileForWork>> tasks = new HashMap<String, Future<FileForWork>>();

		List<String> files = null;
		RemoteIterator<LocatedFileStatus> it = null;

		Path currFile = null;

		// TODO: better shutdown than this.
		while (true) {
			if (it == null) {
				it = inputPath.getFileSystem(new Configuration()).listFiles(inputPath, false);
			}

			if (currFile == null) {
				if (it.hasNext())
					currFile = it.next().getPath();
				else {
					System.out.println("No files to Load, sleeping");
					it = null;
					// TODO: maybe a back off strategy??
					Thread.sleep(1000);
					continue;
				}
			}

			// lets kick off a load if we can.
			if (currFile != null && currLoads < maxNumThreads && tasks.get(currFile) == null) {
				currLoads++;
				final Path p = currFile;
				System.out.println("currFile: " + currFile + " at location: " + p.toString() + " assigned task: " + currThreadNum);
				Callable<FileForWork> worker = new Callable<FileForWork>() {

					@Override
					public FileForWork call() throws Exception {
						FileLoader fl = new FileLoader(new DatedHashURI(fs),FlatFileDedupPolicy.getInstance(),fs);
						FileForWork f = new FileForWork(p);
						f.bytes = fl.load(p, localWatch, destPath,localDest);
						return f;
					}
				};
				Future<FileForWork> submit = executor.submit(worker);
				tasks.put(currFile.toString(), submit);
				currFile = null;
				currThreadNum++;
			}

			// cleanup futures
			for(String task : tasks.keySet()) {
				try {
					FileForWork work = tasks.get(task).get(1000, TimeUnit.MILLISECONDS);
					//delete the file from the dir.
					fs.delete(new Path(task),false);
//					File f = new File(work.orig.toString());
//					System.out.println("File Complete: " + f.toString());
//					f.delete();
					currLoads--;
					tasks.remove(task);
				} catch (ExecutionException e) {
					//TODO: figure out how to handle this
					e.printStackTrace();
					System.exit(1);
				} catch (TimeoutException e) {
					//Thread isnt done loading file yet. this is aight.
					System.out.println("Task " + task + " still working.");
				}
			}
			
			System.out.println("Files Loaded: " + currThreadNum + " Concurrent Tasks: " + currLoads + "\n");

		}
	}

	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length != 2) {
			System.out.println("USAGE: TFIDaemon watchPath destPath ");
			System.out.println("IE: TFIDaemon /mnt/logs hdfs://applogs");
			System.exit(1);
		}
		
		TFIDaemon d = new TFIDaemon(args[0], args[1]);
		d.run();

	}

	public class FileForWork {
		public Path orig;
		public Path newFile;
		public long bytes;
		public String date; // joda?

		public FileForWork(Path orig) {
			this.orig = orig;
		}
	}

}
