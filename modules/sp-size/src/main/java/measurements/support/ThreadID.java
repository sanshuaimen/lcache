package measurements.support;

public class ThreadID {
  public static final ThreadLocal<Integer> threadID = new ThreadLocal<Integer>();
  public static final int MAX_THREADS = 64;
}