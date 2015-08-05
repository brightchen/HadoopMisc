package cg.hadoop.write;

import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockWriter  implements Writer{
  
  public static class WriteTaskWrapper extends WriteTask
  {
    private WriteTask writeTask;
    private BlockWriter writer;
    public WriteTaskWrapper( WriteTask writeTask, BlockWriter writer )
    {
      this.writeTask = writeTask;
      this.writer = writer;
    }
    @Override
    protected void write(byte[] data, int start, int length) throws Exception {
      writeTask.write(data, start, length);
    }
    
    @Override
    protected void writeDone(boolean success) {
      logger.debug("releasing a permit. available permits:{}", writer.availableBlocks.availablePermits());
      writer.availableBlocks.release();
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

  private WriteTaskWrapper writeTask;
  private Thread saverThread;
  
  public BlockWriter()
  {
    this(BLOCK_SIZE);
  }
  
  public BlockWriter(int blockSize)
  {
    this.blockSize = blockSize;
    buf1 = new byte[blockSize];
    buf2 = new byte[blockSize];
    
    currentBuf = buf1;
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

  public void write(byte[] data, int start, int length) {
    if (currentBufOffset + length > blockSize) {
      try {
        logger.debug("getting permit. available permits:{}", availableBlocks.availablePermits());
        availableBlocks.acquire();
      } catch (InterruptedException e) {
        logger.warn(e.getMessage());
      }
      saveData(currentBuf);
      flipCurrentBuffer();
      currentBufOffset = 0;
    }
    copyDataToBuffer(data, start, length);
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
  
  //flip current os, and return old current os
  protected final byte[] flipCurrentBuffer()
  {
    byte[] returnBuf = currentBuf;
    currentBuf = ( currentBuf == buf1 ) ? buf2 : buf1;
    return returnBuf;
  }
  
  public void flush( boolean block ) {
    saveData(currentBuf);
    if( !block )
      return;
  }

  protected void saveData( byte[] data )
  {
    if(saverThread == null){
      saverThread = new Thread(writeTask);
      saverThread.start();
    }
    writeTask.setData(data, 0, currentBufOffset);
  }

  public void setWriteTask(WriteTask writeTask) {
    this.writeTask = new WriteTaskWrapper( writeTask, this );
  }
  
  /**'
   * method to cleanup the resource
   */
  public void cleanup()
  {
    
  }
}