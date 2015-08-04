package cg.hadoop.write;

public class BlockWriter  implements Writer{
  public static final int BLOCK_SIZE = 1024*1024;  //1M
  
  private int blockSize = BLOCK_SIZE;
  private byte[] buf1;
  private byte[] buf2;
  private byte[] currentBuf;
  private int currentBufOffset;

  private WriteTask writeTask;
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
  
  
  public BlockWriter(int blockSize, WriteToFileTask fsSaver)
  {
    this(blockSize);
    setWriteTask(fsSaver);
  }
  
  public void write(byte[] data, int start, int length) {
    if( currentBufOffset + length > blockSize )
    {
      //should change os
      saveData( currentBuf );
      flipCurrentBuffer();
      currentBufOffset=0;
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
  
  public void flush() {
    saveData(currentBuf);
    
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
    this.writeTask = writeTask;
  }
  
  
}