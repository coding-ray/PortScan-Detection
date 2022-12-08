import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class NFWhitelistInitializer {
  public static List<Long> getWhitelist()
      throws IllegalArgumentException, IOException {
    // Credit: https://stackoverflow.com/a/23183963

    String filePath = IOPath.WHITELIST;
    FileSystem fs = new Path(filePath).getFileSystem(new Configuration());
    List<String> fileList = getWhitelistFileList(new Path(filePath), fs);
    return getWhitelistFromFiles(fileList, fs);
  }

  private static List<String> getWhitelistFileList(Path filePath,
      FileSystem fs) throws IOException {
    // Credit: https://www.folkstalk.com/tech/hdfs-java-read-all-files-in-directory-with-code-examples/

    List<String> fileList = new ArrayList<>();
    FileStatus[] fileStatusList = fs.listStatus(filePath);
    for (FileStatus fileStatus : fileStatusList) {
      if (fileStatus.isDirectory()) {
        fileList.addAll(getWhitelistFileList(fileStatus.getPath(), fs));
      } else {
        fileList.add(fileStatus.getPath().toString());
      }
    }
    return fileList;
  }

  private static List<Long> getWhitelistFromFiles(List<String> fileList,
      FileSystem fs)
      throws IOException {
    // Credit: https://stackoverflow.com/a/22020771

    List<Long> result = new ArrayList<>();
    for (String file : fileList) {
      Path path = new Path(file);
      try (BufferedReader br = new BufferedReader(
          new InputStreamReader(fs.open(path)))) {
        String line;
        while ((line = br.readLine()) != null) {
          result.add(NFWritable.convertIPtoNumber(line, NFWritable.TCP));
        }
      }
    } // end of for loop
    return result;
  }
}
