package com.eqt.tfi.file.validate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FlatFileDedupTest {
	
	List<Path> orig = new ArrayList<Path>();
	List<Path> files = new ArrayList<Path>();
	
	@Before
	public void setup() {
		for(int i=0;i<10;i++) {
			orig.add(new Path("file://tmp/orig-"+i));
			files.add(new Path("file://tmp/file-"+i));
		}
	}


	@Test
	public void TestWriteSameFile() throws IOException {
		FlatFileDedupPolicy p = FlatFileDedupPolicy.getInstance();
		p.reset();
		Assert.assertTrue(p.uploadFile(orig.get(0), files.get(0), FileSystem.get(new Configuration())));
		Assert.assertFalse(p.uploadFile(orig.get(0), files.get(0), FileSystem.get(new Configuration())));
		Assert.assertFalse(p.uploadFile(orig.get(1), files.get(0), FileSystem.get(new Configuration())));
		
	}
	
	@Test
	public void TestWriteFile() throws IOException {
		FlatFileDedupPolicy p = FlatFileDedupPolicy.getInstance();
		p.reset();

		boolean allNew = true;
		for(int i=0;i<orig.size();i++)
			allNew = (allNew && p.uploadFile(orig.get(i), files.get(i), FileSystem.get(new Configuration())));
		
		boolean allSeen = true;
		for(int i=0;i<orig.size();i++)
			allSeen = (allSeen && !p.uploadFile(orig.get(i), files.get(i), FileSystem.get(new Configuration())));
		
		Assert.assertTrue(allNew);
		Assert.assertTrue(allSeen);
	}
	
	@Test
	public void TestSingletonness() throws IOException {
		FlatFileDedupPolicy p = FlatFileDedupPolicy.getInstance();
		p.reset();
		Assert.assertTrue(p.uploadFile(orig.get(0), files.get(0), FileSystem.get(new Configuration())));
		p = FlatFileDedupPolicy.getInstance();
		Assert.assertFalse(p.uploadFile(orig.get(0), files.get(0), FileSystem.get(new Configuration())));
		
	}
	
}
