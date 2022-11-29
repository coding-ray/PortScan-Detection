package test_local;

public class Flag {
  public boolean URG;
  public boolean ACK;
  public boolean PSH;
  public boolean RST;
  public boolean SYN;
  public boolean FIN;

  public Flag(String flag_string) {
    if (flag_string.charAt(0) == '0') {
      // flag_string is of format CEUAPRSF in hexidecimal.
      // E.g., 0x5b.
      // Note that there are additional 2 ECN bits (CWR, ECE)
      // ignored in this project.
      int f = Integer.parseInt(flag_string.substring(2), 16);
      URG = (f & 32) != 0;
      ACK = (f & 16) != 0;
      PSH = (f & 8) != 0;
      RST = (f & 4) != 0;
      SYN = (f & 2) != 0;
      FIN = (f & 1) != 0;
    } else {
      // flag_string is of format UAPRSF.
      char[] flag_array = flag_string.toCharArray();
      URG = flag_array[0] != '.';
      ACK = flag_array[1] != '.';
      PSH = flag_array[2] != '.';
      RST = flag_array[3] != '.';
      SYN = flag_array[4] != '.';
      FIN = flag_array[5] != '.';
    }
  }
}