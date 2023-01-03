package psd;

import java.io.IOException;
import psd.stage1.WhitelistFilterAndSessionExtraction;
import psd.stage3.SessionSplit_SmallPacketFilter_SessionAccumulator;
import psd.stage4.block.PortScanBlockFilter;
import psd.stage4.vertical.PortScanVerticalFilter;
import psd.stage5.PortScanAllCombiner;
import psd.stage6.PortScanStatisticsSorter;

public class Entry {
  public static int main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    int code = 0;
    if ((code = WhitelistFilterAndSessionExtraction.run()) != 0)
      return code;

    // 2 is skipped.

    if ((code = SessionSplit_SmallPacketFilter_SessionAccumulator.run()) != 0)
      return code;

    if ((code = PortScanBlockFilter.run()) != 0)
      return code;

    if ((code = PortScanVerticalFilter.run()) != 0)
      return code;

    if ((code = PortScanAllCombiner.run()) != 0)
      return code;

    if ((code = PortScanStatisticsSorter.run()) != 0)
      return code;

    return 0;
  }
}
