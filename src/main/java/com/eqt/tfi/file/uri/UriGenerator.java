package com.eqt.tfi.file.uri;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

/**
 * Pluggable generator for a given path (which must be a file)
 * which will return a path (does not have to be HDFS) for landing
 * the file to.
 * TODO: force file check???
 * @author gman
 *
 */
public interface UriGenerator {

	public Path generateDestinationPath(Path outPrefix ,Path in) throws IOException;
	
}
