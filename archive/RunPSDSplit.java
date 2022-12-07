package archive;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

public class RunPSDSplit {
  static int splitCount = 0;

  public static void execCommand(String command) throws IOException {
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
        FileWriter fw = new FileWriter("out/psd-all-log.txt", true);) {
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

  public static void main(String[] args) throws IOException {
    new BufferedWriter(new FileWriter(
        "out/psd-all-log.txt")).close(); // remove old log

    System.out.println("Results: " + "out/psd-all-log.txt");

    execCommand("make split_all --no-print-directory");

    try (
        // Initialize input
        DataInputStream dis = new DataInputStream(
            new FileInputStream("data/splitCount.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(dis));
    // Finish initialization
    ) {
      splitCount = Integer.parseInt(br.readLine());
    }
    // System.out.println("split count = " + splitCount);

    System.out.println("Results: " + "out/job.message.txt");
    for (int i = 0; i < 1; i++) {
      execCommand("make remove_data --no-print-directory");
      execCommand("hdfs dfs -put -f data/split/" + i + ".nf psd/0/");
      execCommand("hdfs dfs -put -f whitelist/* psd/whitelist/");

      execCommand("make all --no-print-directory");
      execCommand("mv out/4/part-r-00000 data/output_archive/" + i + ".nf");
    }
  } // end of main
} // end of class 

