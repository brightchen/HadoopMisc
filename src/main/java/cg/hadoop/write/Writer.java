package cg.hadoop.write;

public interface Writer {
  public void write(byte[] data, int start, int length);
  public void write(byte[] data);
  
  /**
   * 
   * @param block the method will be blocked until flush really done
   */
  public void flush(boolean block);
}
