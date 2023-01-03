
package archive;

import java.io.*;

public class GetAllSplitOfFile {

  /**
   * 
   * @param args Provided arguments in the following order:
   * 
   *          <pre>
   * Index: Format. Description
   * 0: long. The number of lines in a split
   *          </pre>
   * 
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    if (args.length != 2)
      throw new IllegalArgumentException(
          "Exact 2 arguments must be provided.");

    // No. of lines to be split and saved in each output file.
    // 10 million lines ~ 1.2GB
    long numberOfLines = Long.parseLong(args[0]);

    // Reading file and getting no. of files to be generated
    String inputFilename = args[1];
    String outputFilePrefix = "data/split/";
    String outputFileSuffix = ".nf";
    System.out.println("Split all: Input file: " + inputFilename);

    Progress progress = new Progress();

    // Get one split of the input file.
    try {
      // Initialize input
      DataInputStream dis = new DataInputStream(
          new FileInputStream(inputFilename));
      BufferedReader br = new BufferedReader(new InputStreamReader(dis));
      // Finish initialization
      int indexOfSplit = 0;
      String line = "not Empty";
      while (line != null) {
        // Initialize output
        try {
          FileWriter fw = new FileWriter(
              outputFilePrefix + indexOfSplit + outputFileSuffix);
          BufferedWriter bw = new BufferedWriter(fw);
          System.out.println(
              "Output: " + outputFilePrefix + indexOfSplit + outputFileSuffix);
          long lineIndex = 0;

          // Read from input and write to output until the upper bound is readched.
          System.out.print("Going to the upper bound: ");
          progress = new Progress();
          while (lineIndex < numberOfLines) {
            if ((line = br.readLine()) == null)
              break;
            bw.write(line);
            bw.newLine();
            lineIndex++;
            progress.printOrUpdate(lineIndex, numberOfLines);
          }
          System.out.println("");

          System.out.println("Writing the file...");
          bw.close();
        } finally {
        }
        indexOfSplit++;
      } // end of while

      if (indexOfSplit == 0)
        indexOfSplit = 1;

      try (FileWriter fw = new FileWriter(
          "data/splitCount.txt");
          BufferedWriter bw = new BufferedWriter(fw);) {
        bw.write(String.valueOf(indexOfSplit));
        br.close();
      }
    } finally {
      System.out.println();
    }
  } // End of main
}
