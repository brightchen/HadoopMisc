package cg.hadoop.write;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteApp implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(WriteApp.class);
  
  public static final String filePath = "/tmp/WriteApp.out";
  public final static int count = 1000;
  public static int theadNum = 10;
  public static Thread[] threads = new Thread[theadNum];
  
  byte[] data = "012345678901234567890000000000\n".getBytes();
  
  private AtomicInteger sequence = new AtomicInteger(0);
  private BlockWriter writer;
  
  public static void main(String[] argvs)
  {
    WriteApp app = new WriteApp();
    app.runApp();
  }
  
  public void runApp()
  {
    WriteTask writeTask = null;
    try
    {
      writeTask = new WriteToLocalFileTask(filePath);   //new WriteToFileTask(filePath);
    }
    catch(Exception e)
    {
      logger.warn(e.getMessage());
      return;
    }
    
    writer = new BlockWriter( 1024, writeTask );
    
    for( int i=0; i<theadNum; ++i )
    {
      threads[i] = new Thread(this);
      threads[i].start();
    }
    
    while( sequence.get() < count )
    {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
      
    writer.flush(true);
    writer.cleanup();
    
    int expectedLen = data.length*count;
    logger.info("Total Wrote lenght: {}; expectedLen: {}", writer.getTotalWroteLength(), expectedLen);
  }
  
  @Override
  public void run()
  {
    while(true)
    {
      int curSeq = sequence.incrementAndGet();
      if( curSeq > count )
        break;
  
      byte[] dataClone = Arrays.copyOf(data, data.length);
      byte[] seqArray = String.format("%10d", curSeq).getBytes();
      for( int i=0; i<seqArray.length; ++i )
      {
        dataClone[i] = seqArray[i];
      }
      
      writer.write( dataClone );
    }
  }
}
