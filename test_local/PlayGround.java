package test_local;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

public class PlayGround {
  private static String filename = "data/NetFlow.nf";
  // private static String filename = "data/cicids2017_friday.nf";

  public static void main(String[] args) throws IOException {
    long begin = System.nanoTime();

    // smallFileReader(args);
    largeFileReader(args);

    long end = System.nanoTime();
    System.out.println(
        "Elapsed time (s): " + (float) (end - begin) / (1000 * 1000 * 1000));
  }

  private static void smallFileReader(String[] args) throws IOException {
    File netFlowData = new File(filename);
    Scanner s = new Scanner(netFlowData);
    while (s.hasNextLine()) {
      NF nf = new NF(s.nextLine());
      nf.printData();
    }
    s.close();
  }

  private static void largeFileReader(String[] args) throws IOException {
    FileReader netFlowData = new FileReader(filename);
    BufferedReader br = new BufferedReader(netFlowData);
    String line;
    while ((line = br.readLine()) != null) {
      NF nf = new NF(line);
    }
    br.close();
  }
}
