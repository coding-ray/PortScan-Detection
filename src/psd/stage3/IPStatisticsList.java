package psd.stage3;

import java.util.regex.Pattern;
import java.util.Arrays;

import psd.com.IPPortPair;

public class IPStatisticsList {

  public static final String NONE = "none";
  private static final Pattern TAB_PATTERN = Pattern.compile("\t");

  /**
   * @param oneLine One line of session record from
   *          1. Filter and Session Extraction Stage.
   * 
   */
  public static IPStatistics[] from(String oneLine) {
    /* Sameple input 1 (with index in elements): 
     * 0                  1   2                 3                       4     5 6   7     8 9       10                      11    1213  14    1516
     * 192.168.10.9:1030	<->	192.168.10.3:445	2021-04-11 09:40:57.998	10882	6	25	8530	1	.APRS.	2021-04-11 09:40:57.998	10882	6	25	8530	1	.APRS.
     */

    /* Sample input 2:
     * 0                  1   2               3     4                       5 6   7 8   9 10
     * 192.168.10.9:56588	<->	192.168.10.3:53	none	2021-04-11 09:40:57.873	1	17	2	120	1	......
     */
    IPStatistics[] list = new IPStatistics[2];
    String[] elements = TAB_PATTERN.split(oneLine);

    if (elements.length == 0)
      return list;

    // Temporary variables
    IPPortPair src = new IPPortPair(elements[0]);
    IPPortPair dst = new IPPortPair(elements[2]);
    String dateString = elements[3];

    NFFeature srcFeature;
    NFFeature dstFeature;
    if (dateString.equals(NONE)) {
      srcFeature = new NFFeature();
      dstFeature = new NFFeature(Arrays.copyOfRange(elements, 4, 11));
    } else {
      srcFeature = new NFFeature(Arrays.copyOfRange(elements, 3, 10));
      dateString = elements[10];
      if (dateString.equals(NONE))
        dstFeature = new NFFeature();
      else
        dstFeature = new NFFeature(Arrays.copyOfRange(elements, 10, 17));
    }

    list[0] = new IPStatistics(src, srcFeature);
    list[1] = new IPStatistics(dst, dstFeature);

    return list;
  }
}

