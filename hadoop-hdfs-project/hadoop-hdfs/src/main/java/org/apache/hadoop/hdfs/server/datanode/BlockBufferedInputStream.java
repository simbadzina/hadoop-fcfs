package org.apache.hadoop.hdfs.server.datanode;


import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.mortbay.log.Log;



public class BlockBufferedInputStream extends InputStream {

  /**
   * The internal buffer array where the data is stored. When necessary,
   * it may be replaced by another array of
   * a different size.
   */
  BlockBufferedOutputStream bout;
  FileChannel channel;
  FCFSManager manager;
  long blockID;

  /**
   * Atomic updater to provide compareAndSet for buf. This is
   * necessary because closes can be asynchronous. We use nullness
   * of buf[] as primary indicator that this stream is closed. (The
   * "in" field is also nulled out on close.)
   */
 
  protected int count;  
  protected int markpos = -1;
  protected int marklimit;

  public BlockBufferedInputStream(FCFSManager _man, long _bId,BlockBufferedOutputStream _bout) {
    bout= _bout;
    manager = _man;
    blockID = _bId;
    count = bout.getCount();
    channel = bout.channel;
    bout.buf.position(0);
  }


  @Override
  public synchronized int read() throws IOException {
      return bout.buf.get();
  }

  public synchronized int read(byte b[], int off, int len)
      throws IOException
  {
    bout.buf.get(b, off, len);
    return len;
  }


  public synchronized long seekToOffset(long n) throws IOException {
    bout.buf.position((int)n);
    return n;
  }

  public synchronized void mark(int readlimit) {
    marklimit = readlimit;
    markpos = bout.buf.position();
  }
  public synchronized void reset() throws IOException {

    if (markpos < 0)
      throw new IOException("Resetting to invalid mark");
    bout.buf.position(markpos);
  }

  public boolean markSupported() {
    return true;
  }
  
  public void close() throws IOException {
     channel.truncate(count);
     channel.close();
     manager.unlockAndRemove(blockID); 
  }



}
