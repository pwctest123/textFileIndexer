package com.eqt.tfi.file.uri;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;


/**
 * Uses the file name as given.
 * Takes current time and creates foldering with it.
 * ie: outPrefix + /2013/12/28/18/origFileName
 * @author gman
 *
 */
public class DatedURI implements UriGenerator {

	@Override
	public Path generateDestinationPath(Path outPrefix, Path in) throws IOException {
   	DateTime dt = new DateTime(); 
    	
    	StringBuffer out = new StringBuffer();
    	out.append(Path.SEPARATOR);
    	out.append(dt.getYear());
    	out.append(Path.SEPARATOR);
    	out.append(dt.getMonthOfYear());
    	out.append(Path.SEPARATOR);
    	out.append(dt.getDayOfMonth());
    	out.append(Path.SEPARATOR);
    	out.append(dt.getHourOfDay());
    	out.append(Path.SEPARATOR);
    	out.append(in.getName());

    	Path p = new Path(out.toString());
    	p = Path.mergePaths(outPrefix, p);
    	
		return p;
	}

}
