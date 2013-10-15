package com.eqt.tfi.file.uri;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Reads the file and calculates the sha512 hash of it.
 * Hash is then turned into a path with the first 5 characters
 * x/xx/xx to disperse the files somewhat evenly.
 * @author gman
 */
public class HashURI implements UriGenerator {

	FileSystem fs;
	
	/**
	 * instantiates default hash of SHA512
	 */
	public HashURI(FileSystem fs) {
		this.fs = fs;
	}
	
	public Path generateDestinationPath(Path outPrefix, Path in) throws IOException {
	
		if(!fs.isFile(in))
			throw new IOException(in.toString() + " is not a file");
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("SHA-512");
		} catch (NoSuchAlgorithmException e) {
			throw new UnsupportedOperationException("cannot create SHA-512 digest",e);
		}

		
		FSDataInputStream fis = fs.open(in);
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
    	
    	StringBuffer out = new StringBuffer();
    	out.append(Path.SEPARATOR);
    	out.append(hexString.substring(0, 1)); //dir 1
    	out.append(Path.SEPARATOR);
    	out.append(hexString.substring(1, 3)); //dir 2
    	out.append(Path.SEPARATOR);
    	out.append(hexString.substring(3, 5)); //dir 3
    	out.append(Path.SEPARATOR);
    	out.append(hexString);
   
    	Path p = new Path(out.toString());
    	p = Path.mergePaths(outPrefix, p);
    	
		return p;
	}

}
