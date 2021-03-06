package com.eqt.tfi.input;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

/**
 * Extension of SequenceFileOffsetInputFormat (author url linked in mentioned class)
 * where the key's data is returned as well as its path and offset information.
 * content in this case is skipped completely for performance reasons.
 * TODO: name could be longer :P
 * @author gman
 *
 * @param <V>
 */
//@SuppressWarnings("rawtypes")
public class SequenceFileKeyFileOffsetInputFormat<K extends WritableComparable<K>, V extends Writable> extends FileInputFormat<KeyPathOffsetWritable<K>, V> {

    @SuppressWarnings("hiding")
	public class SequenceFileOffsetRecordReader<K extends WritableComparable<K>,V extends Writable> extends RecordReader<KeyPathOffsetWritable<K>, V> {

        private SequenceFile.Reader in;
        private long start;
        private long end;
        private boolean more = true;
        private KeyPathOffsetWritable<K> key = null;
        private K k = null;
        private V value = null;
        private Configuration conf;

        @SuppressWarnings({ "unchecked", "deprecation" })
		@Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) split;
            conf = context.getConfiguration();
            Path path = fileSplit.getPath();
            FileSystem fs = path.getFileSystem(conf);
            this.in = new SequenceFile.Reader(fs, path, conf);
            try {
                this.k = (K) in.getKeyClass().newInstance();
                this.value = (V) in.getValueClass().newInstance();
            } catch (InstantiationException e) {
                throw new IOException(e);
            } catch (IllegalAccessException e) {
                throw new IOException(e);
            }
            this.end = fileSplit.getStart() + fileSplit.getLength();

            if (fileSplit.getStart() > in.getPosition()) {
                in.sync(fileSplit.getStart());
            }

            this.start = in.getPosition();
            more = start < end;

            key = new KeyPathOffsetWritable<K>(path, start, this.k);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!more) {
                return false;
            }
            long pos = in.getPosition();

            more = in.next(k);
            if (!more || (pos >= end && in.syncSeen())) {
                key = null;
                value = null;
                more = false;
            } else {
                key.setOffset(pos);
            }
            return more;
        }

        @Override
        public KeyPathOffsetWritable<K> getCurrentKey() {
            return key;
        }

        @Override
        public V getCurrentValue() {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            if (end == start) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (in.getPosition() - start) / (float)(end - start));
            }
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

    }

    @Override
    public RecordReader<KeyPathOffsetWritable<K>, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new SequenceFileOffsetRecordReader<K,V>();
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        return new SequenceFileInputFormat<KeyPathOffsetWritable<K>, V>().getSplits(context);
    }

    @Override
    public long getFormatMinSplitSize() {
        return SequenceFile.SYNC_INTERVAL;
    }


}