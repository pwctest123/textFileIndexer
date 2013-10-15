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
import org.apache.hadoop.fs.Path;

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

	String watchDir;
	String destPrefix;

	public TFIDaemon(String watch, String dest) throws IOException, InterruptedException {
		if(!watch.endsWith("/"))
			watch += "/";
		if(!dest.endsWith("/"))
			dest+="/";
		this.watchDir = watch;
		this.destPrefix = dest;
	}
	
	public void run() throws IOException, InterruptedException {

		Configuration conf = new Configuration();
		final FileSystem fs = FileSystem.get(conf);

		// TODO: parameterize threading
		final int maxNumThreads = 10;
		ExecutorService executor = Executors.newFixedThreadPool(maxNumThreads);
		// keep track of the number of current threads running
		int currLoads = 0;
		// used for landing into the map of futures
		int currThreadNum = 0;
		Map<Integer, Future<FileForWork>> tasks = new HashMap<Integer, Future<FileForWork>>();

		List<String> files = null;
		Iterator<String> it = null;

		String currFile = null;

		// TODO: better shutdown than this.
		while (true) {
			if (it == null) {
				File dir = new File(watchDir);
				System.out.println("dir: " + dir.toString());
				String[] list = dir.list();
				if(list == null || list.length == 0) {
					System.out.println("No files to Load, sleeping");
					// TODO: maybe a back off strategy??
					Thread.sleep(1000);
					continue;
				}
				files = Arrays.asList(dir.list());
				it = files.iterator();
			}

			if (currFile == null) {
				if (it.hasNext())
					currFile = it.next();
				else
					it = null;
			}

			// lets kick off a load if we can.
			if (currFile != null && currLoads < maxNumThreads) {
				System.out.println("currFile: " + currFile + " assigned task: " + currThreadNum);
				final Path p = new Path(watchDir + currFile);
				Callable<FileForWork> worker = new Callable<FileForWork>() {

					@Override
					public FileForWork call() throws Exception {
						FileLoader fl = new FileLoader(new DatedHashURI(fs),FlatFileDedupPolicy.getInstance(),fs);
						FileForWork f = new FileForWork(p);
						f.bytes = fl.load(p, new Path(destPrefix));
						return f;
					}
				};
				Future<FileForWork> submit = executor.submit(worker);
				tasks.put(currThreadNum++, submit);
				currLoads++;
				currFile = null;
			}

			// cleanup futures
			for(int task : tasks.keySet()) {
				try {
					FileForWork work = tasks.get(task).get(1000, TimeUnit.MILLISECONDS);
					//delete the file from the dir.
					File f = new File(work.orig.toString());
					f.delete();
					currLoads--;
				} catch (ExecutionException e) {
					//TODO: figure out how to handle this
					e.printStackTrace();
					System.exit(1);
				} catch (TimeoutException e) {
					//Thread isnt done loading file yet. this is aight.
					System.out.println("Task " + task + " still working.");
				}
			}

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
