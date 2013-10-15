package com.eqt.tfi.file.validate;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * The goal is to provide a way to decide if the file being given needs or should
 * be uploaded into HDFS.
 * @author gman
 *
 */
public interface FileUploadPolicy {

	/**
	 * returns true if the policy decides the file can be uploaded
	 * @param f
	 * @param fs
	 * @return
	 * @throws IllegalStateException when backing store gets gummed up
	 */
	public boolean uploadFile(Path orig, Path f, FileSystem fs) throws IllegalStateException;
	
}
