package cg.hadoop.write;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockWriter  implements Writer{
  
  protected static class WriteTaskWrapper extends WriteTask
  {
    private WriteTask writeTask;
    private BlockWriter writer;
    private volatile long totalWroteLength = 0;
    public WriteTaskWrapper( WriteTask writeTask, BlockWriter writer )
    {
      this.writeTask = writeTask;
      this.writer = writer;
      startWriteThread();
    }
    
    @Override
    protected void write(byte[] data, int start, int length) throws Exception {
      writeTask.write(data, start, length);
    }
    
    @Override
    public void flush()
    {
      writeTask.flush();
    }
    
    @Override
    protected void writeDone(byte[] data, boolean success, int wroteSize) {
      totalWroteLength += wroteSize;
      writer.availableBlocks.release();
      if(logger.isDebugEnabled())
        logger.debug("just released a permit. available permits: {}", writer.availableBlocks.availablePermits());

      if( writer.flushDoneLatch != null && data == writer.currentBuf )
      {
        writer.flushDoneLatch.countDown();
      }
    
    }
  }
  
  
  private static final Logger logger = LoggerFactory.getLogger( BlockWriter.class );
      
  public static final int BLOCK_SIZE = 1024*1024;  //1M
  
  private final Semaphore availableBlocks = new Semaphore(2, true);
  
  private int blockSize = BLOCK_SIZE;
  private byte[] buf1;
  private byte[] buf2;
  private byte[] currentBuf;
  private int currentBufOffset;
  final private Lock bufferLock = new ReentrantLock();
  
  private WriteTaskWrapper writeTask;
  
  private volatile CountDownLatch flushDoneLatch;
  
  public BlockWriter()
  {
    this(BLOCK_SIZE);
  }
  
  public BlockWriter(int blockSize)
  {
    this.blockSize = blockSize;
    buf1 = new byte[blockSize];
    buf2 = new byte[blockSize];
    
    currentBuf = null;
    currentBufOffset = 0;
  }
  
  public BlockWriter(WriteTask writeTask)
  {
    this();
    setWriteTask(writeTask);
  }
  
  public BlockWriter(int blockSize, WriteTask writeTask)
  {
    this(blockSize);
    setWriteTask(writeTask);
  }
  
  
  public BlockWriter(int blockSize, WriteToFileTask writeTask)
  {
    this(blockSize);
    setWriteTask(writeTask);
  }

  /**
   * When the buffer going to overflow or flush is called, switch to another buffer for writing and send writeTask to write
   * @return
   */
  public void write(byte[] data, int start, int length) {
    byte[] oldBuffer = null;
    int oldBufferLength = 0;
  
    bufferLock.lock();
    if (currentBuf==null || currentBufOffset + length > blockSize) {
      try {
        if(logger.isDebugEnabled())
          logger.debug("Going to acquire a permit. available permits: {}", availableBlocks.availablePermits());
        availableBlocks.acquire();
      } catch (InterruptedException e) {
        logger.warn(e.getMessage());
      }
      oldBufferLength = currentBufOffset;
      oldBuffer = flipCurrentBuffer();
      
    }
    copyDataToBuffer(data, start, length);
    bufferLock.unlock();
    
    if( oldBuffer != null )
      saveData(oldBuffer, oldBufferLength);
    
  }
  
  
  @Override
  public void write(byte[] data)
  {
    write(data, 0, data.length);
  }
      
  protected void copyDataToBuffer(byte[] data, int start, int length)
  {
    if(length<=0)
      return;
    for(int i=start; i<start+length; ++i)
    {
      currentBuf[currentBufOffset++] = data[i];
    }
  }
  
  //flip current buffer, and return buffer
  protected final byte[] flipCurrentBuffer()
  {
    byte[] returnBuf = currentBuf;
    currentBuf = ( currentBuf == buf1 ) ? buf2 : buf1;
    currentBufOffset = 0;
    return returnBuf;
  }
  
  /**
   * - when flush() in block mode, all write should be blocked until flush done.
   * - when multiple threads call flush() in block mode at some time. all thread will be blocked until flush done.
   * - when multiple threads call flush() both in block and non-lock mode, the the non-block mode will return immediately 
   *   while block mode should be blocked
   */
  public void flush( boolean block ) {
    bufferLock.lock();
    if (currentBuf == null || currentBufOffset <= 0)
    {
      bufferLock.unlock();
      return;
    }
    
    //non-block
    if(!block)
    {
      byte[] oldBuffer = null;
      int oldBufferSize = 0;
      
      try {
        if(logger.isDebugEnabled())
          logger.debug("Going to acquire a permit. available permits: {}", availableBlocks.availablePermits());
        availableBlocks.acquire();
      } catch (InterruptedException e) {
        logger.warn(e.getMessage());
      }
      oldBufferSize = currentBufOffset;
      oldBuffer = flipCurrentBuffer();
      
      bufferLock.unlock();
      
      if (oldBuffer != null && oldBufferSize > 0)
        saveData(oldBuffer, oldBufferSize);
      return;
    }

    //block
    //get all permits avoid get other block for writing
//    final int availablePermits = availableBlocks.availablePermits();
//    if(availablePermits > 0)
//    {
//      if(logger.isDebugEnabled())
//        logger.debug("Availabe permits: {}. acquire all of them.", availablePermits);
//      try {
//        availableBlocks.acquire(availablePermits);
//      } catch (InterruptedException ie) {
//        logger.warn(ie.getMessage());
//      }
//    }
    
    flushDoneLatch = new CountDownLatch(1);
    saveData(currentBuf, currentBufOffset);
    //reuse currentBuf as current buffer
    currentBufOffset = 0;

    try {
      //waiting for write done.
      flushDoneLatch.await();
      
      //all cached data should wrote. flush now
      writeTask.flush();
      
    } catch (InterruptedException e) {
      logger.warn("flushDoneLatch await() exception.", e.getMessage());
    }
    
    bufferLock.unlock();
  }

  protected void saveData( byte[] data, int dataLength )
  {
    writeTask.setData(data, 0, dataLength);
  }

  public void setWriteTask(WriteTask writeTask) {
    this.writeTask = new WriteTaskWrapper( writeTask, this );
  }
  
  /**'
   * method to cleanup the resource
   */
  public void cleanup()
  {
    writeTask.cleanup();
  }
  
  public long getTotalWroteLength()
  {
    return writeTask.totalWroteLength;
  }
}