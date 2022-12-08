package psd;

import java.io.IOException;
import psd.stage1.WhitelistFilterAndSessionExtraction;
import psd.stage3.SessionSplit_SmallPacketFilter_SessionAccumulator;
import psd.stage4.vertical.PortScanVerticalFilter;
import psd.stage5.PortScanAllCombiner;

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
