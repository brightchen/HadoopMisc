package cg.hadoop.write;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class SimpleFileWriter implements Writer{

  private String filePath;
  private FileOutputStream fos;
  
  public SimpleFileWriter()
  {
  }

  public SimpleFileWriter( String filePath )
  {
    setFilePath( filePath );
  }

  
  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  @Override
  public void write(byte[] data)
  {
    write(data, 0, data.length);
  }
  
  @Override
  public void write(byte[] data, int start, int length) {
    if(fos==null)
    {
      createFileOutputStream();
    }
    try {
      fos.write(data, start, length);
      //flush each time
      flush(true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void flush( boolean block ) {
    if(fos==null)
      return;
    
    try {
      fos.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
    
  }

  protected synchronized void createFileOutputStream()
  {
    if(fos!=null)
      return;
    try {
      fos = new FileOutputStream(filePath);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }
}
