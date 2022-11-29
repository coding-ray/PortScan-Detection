/*
 * Please notice the "package" and the input_file_name.
 */

package archive;

import java.io.*;
import java.util.Scanner;

public class Split {
  public static void main(String args[]) {
    try {
      // Reading file and getting no. of files to be generated
      String input_file_name = "cicids2017_friday.nf";
      String output_file_prefix = "data/nf";
      String output_file_suffix = ".nf";

      // No. of lines to be split and saved in each output file.
      double number_of_lines = 10000;

      File file = new File(input_file_name);
      Scanner scanner = new Scanner(file);
      int count = 0;
      while (scanner.hasNextLine()) {
        scanner.nextLine();
        count++;
      }
      scanner.close();
      System.out.println("Lines in the file: " + count);
      int nof = (int) Math.ceil(count / number_of_lines);
      System.out.println("No. of files to be generated :" + nof);

      // -------------------------------------------
      // Actual splitting of file into smaller files
      // -------------------------------------------
      DataInputStream dis = new DataInputStream(new FileInputStream(input_file_name));

      BufferedReader br = new BufferedReader(new InputStreamReader(dis));
      String one_line;

      for (int file_num = 1; file_num <= nof; file_num++) {
        FileWriter fw = new FileWriter(
            output_file_prefix + file_num + output_file_suffix);
        BufferedWriter bw = new BufferedWriter(fw);
        for (int i = 1; i <= number_of_lines; i++) {
          if ((one_line = br.readLine()) == null)
            continue;
          bw.write(one_line);
          if (i != number_of_lines)
            bw.newLine();
        }

        bw.close();
      }

      dis.close();

    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
    }

  }

}