import java.io.IOException;

public class Entry {
  public static int main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    if (WhitelistFilterAndSessionExtraction.run() == 1)
      return 1;

    // 2 is skipped.

    if (SessionSplit_SmallPacketFilter_SessionAccumulator.run() == 3)
      return 3;

    if (PortScanVerticalFilter.run() == 41)
      return 41;

    if (PortScanAllCombiner.run() == 5)
      return 5;

    return 0;
  }
}
