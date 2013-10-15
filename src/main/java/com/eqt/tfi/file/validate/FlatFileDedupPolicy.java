package com.eqt.tfi.file.validate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * About the most basic policy, this thing maintains a flat file
 * in tmp space that will tell if the file has been
 * seen before or not.
 * 
 * Internally maintains a keyValue file with the UriGenerator's key and the orig
 * file name as the value. This lets you conceivably lookup the original files
 * names. 
 * 
 * NOTE: this impl is just for testing/single loader instances.
 * 
 * TODO: maybe random file each time??
 * @author gman
 *
 */
public class FlatFileDedupPolicy implements FileUploadPolicy {

	static final File f = new File(System.getProperty("java.io.tmpdir") + "/test/dedup_file");
	private static Writer w = null;
	static HashMap<String, Set<String>> seenFiles = new HashMap<String, Set<String>>();

	static FlatFileDedupPolicy p = null;
	
	public static synchronized FlatFileDedupPolicy getInstance() throws IOException {
		if(p == null)
			p = new FlatFileDedupPolicy();
		return p;
	}
	
	//testing only
	void reset() throws IOException {
		p = new FlatFileDedupPolicy(true);
	}
	
	private FlatFileDedupPolicy() throws IOException {
		this(false);
	}
	
	/**
	 * I think just for testing purposes??
	 * @param reset
	 * @throws IOException
	 */
	private FlatFileDedupPolicy(boolean reset) throws IOException {
		if(reset && f.exists())
			f.delete();
		
		if(reset)
			seenFiles = new HashMap<String, Set<String>>();
		
		if(!f.exists()) {
			if(!new File(f.getParent()).exists())
				new File(f.getParent()).mkdirs();
			f.createNewFile();
		}

		BufferedReader r = new BufferedReader(new FileReader(f));
		String file = null;
		while((file = r.readLine()) != null) {
			String[] kv = file.split(" ");
			Set<String> set = seenFiles.get(kv[0]);
			if(set == null) {
				set = new HashSet<String>();
				seenFiles.put(kv[0], set);
			}
			set.add(kv[1]);
		}
		r.close();
	}
	
	public synchronized boolean uploadFile(Path orig, Path f, FileSystem fs) throws IllegalStateException {
		boolean newFile = false;
		
		Set<String> set = seenFiles.get(f.getName());
		if(set == null) {
			set = new HashSet<String>();
			seenFiles.put(f.getName(), set);
			newFile = true;
		}
			
		if(!set.contains(orig.getName())) {
			set.add(orig.getName());
			
			try {
				w = new FileWriter(FlatFileDedupPolicy.f,true);
				w.write(f.getName() + " " + orig.getName() + "\n");
				w.close();
			} catch (IOException e) {
				throw new RuntimeException("failure to store file");
			} finally {
				if(w != null)
					try {
						w.close();
					} catch (IOException e) {
						//gulp
					}
			}
		}
		return newFile;
	}

}
