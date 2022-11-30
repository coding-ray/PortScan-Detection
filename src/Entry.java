import java.io.IOException;

public class Entry {
  public static int main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    if (FilterAndSessionExtraction.run() == 1)
      return 1;

    if (Grouping.run() == 2)
      return 2;

    if (MaliciousFlowFilter.run() == 3)
      return 3;

    return 0;
  }
}
