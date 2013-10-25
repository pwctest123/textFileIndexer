package com.eqt.tfi.util;

import java.io.IOException;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.analysis.type.TextFieldTypeDefinition;
import org.apache.blur.server.TableContext;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.hadoop.fs.Path;

public class Statics {

	public static final String KEY_DELIM = ":";
	public static final String BLUR_CONTROLLER = "tfi.blur.controller";
	public static final String BLUR_TFI_TABLE_CREATE_DEFAULT = "tfi.blur.create.table.default";
	public static final String BLUR_TFI_TABLE_NAME = "tfi.blur.index.name";
	public static final String BLUR_TFI_DEFAULT_TABLE_NAME = "tfi_index";
	
	public static final String TFI_BASE_DIR = "tfi.base.dir";
	public static final String TFI_BASE_DIR_DEFAULT_VALUE = "/tfi";
	public static final String TFI_TMP_DIR = "tfi.tmp.dir";
	public static final String TFI_TMP_DIR_DEFAULT_VALUE = "/tmp" + TFI_BASE_DIR_DEFAULT_VALUE;
	
	public static final String INPUT_CONTENT_FOLDER = "tfi.content.input.folder";
	public static final String INPUT_CONTENT_FOLDER_DEFAULT = TFI_BASE_DIR_DEFAULT_VALUE + "/content";
	public static final String INPUT_COMBINER_PATH = "tfi.combiner.content.input";
	public static final String INPUT_COMBINER_OUTPUT_PATH = "tfi.combiner.content.output";
	public static final String COMBINER_NUM_FILES = "tfi.combinder.num.outputfiles";
	
	public static final String RECORD_OFFSET_INPUT_PATH = "tfi.record.offset.input";
	public static final String RECORD_OFFSET_OUTPUT_PATH = "tfi.record.offset.output";
	
	public static final String DEFAULT_CONTENT_FAMILY = "content";
	public static final String DEFAULT_CONTENT_FIELD = "data";
	public static final String DEFAULT_METADATA_FAMILY = "meta";
	
	
	/**
	 * Uses the first cluster found.
	 * Defaults to 4 shards per shard server.
	 * Uses default table name.
	 * @param client
	 * @return TableDescriptor which is missing the table URI Component.
	 * @throws TException 
	 * @throws BlurException 
	 * @throws IOException 
	 */
	public static final TableDescriptor getDefaultTableDescriptor(Iface client) throws BlurException, TException, IOException {
		TableDescriptor td = new TableDescriptor();
		td.setCluster(client.shardClusterList().get(0));
		td.setEnabled(true);
		td.setName(BLUR_TFI_DEFAULT_TABLE_NAME);
		td.setReadOnly(false);
		int shardServers = client.shardServerList(client.shardClusterList().get(0)).size();
		td.setShardCount(shardServers*4);

		TableContext context = TableContext.create(td);
		FieldManager fieldManager = context.getFieldManager();
		fieldManager.addColumnDefinition(DEFAULT_CONTENT_FAMILY, DEFAULT_CONTENT_FIELD,
				null, false,
				TextFieldTypeDefinition.NAME, null);
//		td.setTableUri("");
		
		return td;
	}


	/**
	 * Copied from 2.0.5 hadoop-common Path since CDH isnt at 2.0.5 yet.
	 * took out the windows checks, who uses windows!??
	 * @param path1
	 * @param path2
	 * @return
	 */
	public static Path mergePaths(Path path1, Path path2) {
		String path2Str = path2.toUri().getPath();
		// Add path components explicitly, because simply concatenating two path
		// string is not safe, for example:
		// "/" + "/foo" yields "//foo", which will be parsed as authority in
		// Path
		return new Path(path1.toUri().getScheme(), path1.toUri().getAuthority(), path1.toUri().getPath() + path2Str);
	}
	
}
