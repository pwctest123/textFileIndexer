package com.eqt.tfi.file.uri;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;


/**
 * Uses the file name as given.
 * Takes current time and creates foldering with it.
 * ie: outPrefix + /2013/12/28/18/15/origFileName
 * where the last integer value is the minutes floored to 15 minute intervals (0,15,30,45).
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
    	out.append(pad(dt.getMonthOfYear()+""));
    	out.append(Path.SEPARATOR);
    	out.append(pad(dt.getDayOfMonth()+""));
    	out.append(Path.SEPARATOR);
    	out.append(pad(dt.getHourOfDay()+""));
    	out.append(Path.SEPARATOR);
    	out.append( pad((dt.getMinuteOfHour() /15)*15 + "") );
    	out.append(Path.SEPARATOR);
    	out.append(in.getName());

    	Path p = new Path(outPrefix.toString() + out.toString());
    	
		return p;
	}

	private String pad(String str) {
		if(str.length() < 2)
			return "0"+str;
		return str;
	}
	
}
