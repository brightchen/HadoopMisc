package cg.hadoop.write;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteApp {
  private static final Logger logger = LoggerFactory.getLogger(WriteApp.class);
  
  public static final String filePath = "/tmp/WriteApp.out";
  public final static int count = 100;
  
  public static void main(String[] argvs)
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
    
    BlockWriter writer = new BlockWriter( 1024, writeTask );
    
    byte[] data = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890000000000\n".getBytes();
    int dataLen = data.length;
    for( int i=0; i<count; ++i )
    { 
      writer.write( String.format("%10d", i).getBytes() );
      writer.write( data );
    }
    logger.info("going to flush.");
    writer.flush(true);
    
    long expectedLen = (dataLen+10) * count;
    
    writer.cleanup();
    logger.info("Total Wrote lenght: {}; expectedLen: {}", writer.getTotalWroteLength(), expectedLen);
  }
}
