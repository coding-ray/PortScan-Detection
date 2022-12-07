package archive;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

public class RunPSDSplitDirect {
  static int startingIndex = 7;
  static int splitCount = 10;
  static final String LOG_PATH = "out/psd-split-dirct-log.txt";

  public static void execCommand(String command)
      throws IOException, InterruptedException {
    String osName = System.getProperty("os.name");
    String rawVersion = null;

    // StringBuilder outputReport = new StringBuilder();
    if (osName == null ||
        osName.indexOf("Windows") != -1 ||
        osName.indexOf("SunOS") != -1)
      return;

    Runtime rt = Runtime.getRuntime();
    // execute the RPM process
    Process proc = rt.exec(new String[] {"sh", "-c", command});

    try (BufferedReader is = new BufferedReader(
        new InputStreamReader(proc.getInputStream()));
        FileWriter fw = new FileWriter(LOG_PATH, true);) {
      fw.write("autorun $ " + command);
      int temp;
      while ((temp = is.read()) != -1) {
        // System.out.print((char) temp);
        fw.write((char) temp);
        fw.flush();
      }
      fw.write('\n');

      // read output of the rpm process
      // String tempStr = "";
      // while ((tempStr = is.readLine()) != null) {
      //   outputReport.append(tempStr.replace(">", "/>\n") + "\n");
      //   tempStr = "";
      // }
      // int inBuffer;
      // while ((inBuffer = is.read()) != -1) {
      //   outputReport.append((char) inBuffer);
      // }
      // rawVersion = is.readLine();
      // response.append(rawVersion);
      proc.destroy();
    } catch (IOException ioe) {
      System.out.println("Exception executing command " + command + "\n" + ioe);
    }
    // System.out.println(outputReport.toString());
  }

  public static void main(String[] args)
      throws IOException, InterruptedException {
    System.out.println("Results: " + LOG_PATH);

    System.out.println("Results: " + "out/job-message.txt");
    for (int i = startingIndex; i < splitCount; i++) {
      System.out.println("Current PSD split: " + i);
      execCommand("make remove_data --no-print-directory");
      execCommand("hdfs dfs -put -f data/split/" + i + ".nf psd/0/");
      execCommand("hdfs dfs -put -f whitelist/* psd/whitelist/");

      execCommand("make all --no-print-directory");
      execCommand("mv out/4/part-r-00000 data/output_archive/" + i + ".nf");
    }
  } // end of main
} // end of class 

