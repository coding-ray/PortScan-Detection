package archive;

import java.io.*;
import java.lang.IllegalArgumentException;
import java.util.logging.*;

public class GetOneSplitOfFile {
  private static final Logger logger = Logger
      .getLogger(GetOneSplitOfFile.class.getName());


  /**
   * 
   * @param args Provided arguments in the following order:
   * 
   *          <pre>
   * Index: Format. Description
   * 0: long. The number of lines in a split
   * 1: int. The index of the split, which starts from 0.
   *          </pre>
   * 
   * @throws IOException
   */
  public static void main(String args[]) throws IOException {
    if (args.length != 2)
      throw new IllegalArgumentException(
          "Exact 2 arguments must be provided.");

    // No. of lines to be split and saved in each output file.
    // 10 million lines ~ 1.2GB
    long numberOfLines = Long.parseLong(args[0]);

    int indexOfSplit = Integer.parseInt(args[1]);
    // Reading file and getting no. of files to be generated
    String inputFilename = "/mnt/WinData/Download/PJ_Data/netflow.nf";
    String outputFilePrefix = "data/split/";
    String outputFileSuffix = ".nf";


    Progress progress = new Progress();

    // Get one split of the input file.
    try (
        // Initialize input
        DataInputStream dis = new DataInputStream(
            new FileInputStream(inputFilename));
        BufferedReader br = new BufferedReader(new InputStreamReader(dis));

        // Initialize output
        FileWriter fw = new FileWriter(
            outputFilePrefix + indexOfSplit + outputFileSuffix);
        BufferedWriter bw = new BufferedWriter(fw);
    // Finish initialization
    ) {
      System.out.println("Files are initialized.");
      long lowerBound = numberOfLines * indexOfSplit;
      long upperBound = numberOfLines * (indexOfSplit + 1);
      long lineIndex = 0;
      String line;

      // Read until the lower bound is reached.
      System.out.print("Going to the lower bound: ");
      while (lineIndex < lowerBound) {
        if ((line = br.readLine()) == null)
          break;
        lineIndex++;
        progress.printOrUpdate(lineIndex, lowerBound);
      }
      System.out.println("");

      // Read from input and write to output until the upper bound is readched.
      System.out.print("Going to the upper bound: ");
      progress = new Progress();
      while (lineIndex < upperBound) {
        if ((line = br.readLine()) != null) {
          bw.write(line);
          bw.newLine();
          lineIndex++;
          progress.printOrUpdate(lineIndex - lowerBound, numberOfLines);
        }
      }
      System.out.println("");

      System.out.println("Writing the file...");
    } // End of try
  } // End of main
}
