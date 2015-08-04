package cg.hadoop.write;

public interface Writer {
  public void write(byte[] data, int start, int length);
  public void write(byte[] data);
  public void flush();
}
