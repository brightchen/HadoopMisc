package cg.hadoop.write;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WriteToLocalFileTask extends WriteTask
{
  private static final Logger logger = LoggerFactory.getLogger(WriteToLocalFileTask.class);

  private FileOutputStream fileOutputStream;
  public WriteToLocalFileTask( String fileName ) throws FileNotFoundException
  {
    File file = new File(fileName);
    fileOutputStream = new FileOutputStream( file );
  }
  
  @Override
  protected void write( byte[] data, int start, int length ) throws IOException
  {
    fileOutputStream.write(data, start, length);
  }
  
  @Override
  public void flush()
  {
    try {
      fileOutputStream.flush();
    } catch (IOException e) {
      logger.debug(e.getMessage());
    }
  }
  
  @Override
  protected void writeDone(byte[] data, boolean success, int wroteSize){}
  
}

