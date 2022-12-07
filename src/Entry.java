import java.io.IOException;
import org.apache.hadoop.security.WhitelistBasedResolver;

public class Entry {
  public static int main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    if (FilterAndSessionExtraction.run() == 1)
      return 1;

    // 2 is skipped.

    if (SessionSplitAndPacketFilter.run() == 3)
      return 3;

    // if (PortScanVerticalCombiner.run() == 41)
    //   return 41;

    // if (MaliciousFlowFilter.run() == 5)
    //   return 5;

    return 0;
  }
}
