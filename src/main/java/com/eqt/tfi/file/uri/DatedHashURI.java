package com.eqt.tfi.file.uri;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;

/**
 * Calculates the files sha512 hash for a filename.
 * Takes current time and creates foldering with it.
 * ie: outPrefix + /2013/12/28/18/15/cfcbeabc6b65731c7c1dbac....
 * where the last integer value is the minutes floored to 15 minute intervals (0,15,30,45).
 */
public class DatedHashURI implements UriGenerator {

	public Path generateDestinationPath(Path outPrefix, Path in) throws IOException {
		
		if(!in.getFileSystem(new Configuration()).isFile(in))
			throw new IOException(in.toString() + " is not a file");
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("SHA-512");
		} catch (NoSuchAlgorithmException e) {
			throw new UnsupportedOperationException("cannot create SHA-512 digest",e);
		}

		
		FSDataInputStream fis = in.getFileSystem(new Configuration()).open(in);
		byte[] dataBytes = new byte[1024];
		int nread = 0; 
		
        while ((nread = fis.read(dataBytes)) != -1) {
          md.update(dataBytes, 0, nread);
        };
        byte[] mdbytes = md.digest();
        StringBuffer hexString = new StringBuffer();
    	for (int i=0;i<mdbytes.length;i++) {
    	  hexString.append(Integer.toHexString(0xFF & mdbytes[i]));
    	}
    	fis.close();
    	
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
    	out.append( (dt.getMinuteOfHour() /15)*15 );
    	out.append(Path.SEPARATOR);
    	out.append(hexString);
   
    	Path p = new Path(outPrefix.toString() + out.toString());
		return p;
	}

	private String pad(String str) {
		if(str.length() < 2)
			return "0"+str;
		return str;
	}

}
