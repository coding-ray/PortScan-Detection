package archive;

import java.util.Arrays;

public class EliminateEmptyStringInArray {
  public static void main(String[] args) {
    String[] firstArray = { "test1", "", "test2", "test4", "", null };

    firstArray = Arrays.stream(firstArray)
        .filter(s -> (s != null && s.length() > 0))
        .toArray(String[]::new);

    for (String s : firstArray) {
      System.out.println(s);
    }
  }
}
