package org.apache.hadoop.hdfs.server.datanode;


import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;



public class BlockBufferedInputStream extends InputStream {

  /**
   * The internal buffer array where the data is stored. When necessary,
   * it may be replaced by another array of
   * a different size.
   */
  ByteBuffer buf;
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

  public BlockBufferedInputStream(ByteBuffer _buf, FCFSManager _man, long _bId) {
    buf = _buf;
    manager = _man;
    blockID = _bId;
  }


  @Override
  public synchronized int read() throws IOException {
    if (buf.remaining() > 0) {
      return buf.get();
    }

    return -1;
  }

  public synchronized int read(byte b[], int off, int len)
      throws IOException
  {
    buf.get(b, off, len);
    return len;
  }


  public synchronized long skip(long n) throws IOException {
    buf.position(buf.position()+(int)n);
    return n;
  }

  public synchronized void mark(int readlimit) {
    marklimit = readlimit;
    markpos = buf.position();
  }
  public synchronized void reset() throws IOException {

    if (markpos < 0)
      throw new IOException("Resetting to invalid mark");
    buf.position(markpos);
  }

  public boolean markSupported() {
    return true;
  }
  
  public void close() throws IOException {
     manager.unlockAndRemove(blockID); 
  }



}
