package archive;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

public class RunPSDSplitDirect {
  static final String EXE_LOG_PATH = "out/psd-split-dirct-log.txt";
  static final String ALL_JOB_LOG_PATH = "out/psd-split-job-log.txt";
  static final String CURRENT_JOB_LOG_PATH = "out/job-message.txt";

  public static void execCommand(String command)
      throws IOException {
    String osName = System.getProperty("os.name");

    if (osName == null ||
        osName.indexOf("Windows") != -1 ||
        osName.indexOf("SunOS") != -1)
      return;

    Runtime rt = Runtime.getRuntime();
    // execute the RPM process
    Process proc = rt.exec(new String[] {"sh", "-c", command});

    try (BufferedReader is = new BufferedReader(
        new InputStreamReader(proc.getInputStream()));
        FileWriter fw = new FileWriter(EXE_LOG_PATH, true);) {
      fw.write("autorun $ " + command);
      int temp;
      while ((temp = is.read()) != -1) {
        fw.write((char) temp);
        fw.flush();
      }
      fw.write('\n');

      proc.destroy();
    } catch (IOException ioe) {
      System.out.println("Exception executing command " + command + "\n" + ioe);
    }
  }

  public static void main(String[] args)
      throws IOException, InterruptedException {
    if (args.length != 2) {
      System.out.println("There must be 2 arguments provided.");
      return;
    }

    int startingIndex = Integer.parseInt(args[0]);
    int endingIndex = Integer.parseInt(args[1]);

    System.out.println("Execution Results: " + EXE_LOG_PATH);
    System.out.println("All Job Results: " + ALL_JOB_LOG_PATH);
    System.out.println("Current Job Results: " + CURRENT_JOB_LOG_PATH);

    // Clean up execution and job logs
    execCommand("rm -r -f " + EXE_LOG_PATH + " " + ALL_JOB_LOG_PATH);

    // Run commands for each split
    for (int i = startingIndex; i < endingIndex; i++) {
      System.out.println("Current PSD split: " + i);
      // Put data
      execCommand("make remove_data --no-print-directory");
      execCommand("hdfs dfs -put -f data/split/" + i + ".nf psd/0/");
      execCommand("hdfs dfs -put -f whitelist/* psd/whitelist/");

      // Run PSD
      execCommand("make all --no-print-directory");

      // Collect data
      execCommand("mv out/6/part-r-00000 data/output_archive/" + i + ".nf");
      execCommand("cat " + CURRENT_JOB_LOG_PATH + " >> " + ALL_JOB_LOG_PATH);

      // Remove used split
      execCommand("rm -f data/split/" + i + ".nf");
    }
  } // end of main
} // end of class 

