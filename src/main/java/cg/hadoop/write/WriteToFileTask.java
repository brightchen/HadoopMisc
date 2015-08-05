package cg.hadoop.write;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteToFileTask extends WriteTask
{
  private static final Logger logger = LoggerFactory.getLogger(WriteToFileTask.class);
  
  final private Configuration conf = new Configuration();
  private FileSystem fileSystem;
  private FSDataOutputStream fsOutStream;

  public WriteToFileTask( String fileName )
  {
    Path filePath = new Path(fileName);
    try {
      fileSystem = FileSystem.get(conf);
      fsOutStream = fileSystem.create(filePath);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  protected void write( byte[] data, int start, int length ) throws IOException
  {
    fsOutStream.write(data, start, length);
    flush();
  }
  
  @Override
  public void flush()
  {
    try {
      fsOutStream.flush();
    } catch (IOException e) {
      logger.debug(e.getMessage());
    }
  }
  
  @Override
  public void cleanup() {
    flush();
    try {
      fileSystem.close();
    } catch (IOException e) {
      logger.warn(e.getMessage());
    }
  }
}
