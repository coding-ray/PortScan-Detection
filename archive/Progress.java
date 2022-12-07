package archive;

public class Progress {
  int progress; // Between 0 and 100. Must started with 0.

  public Progress() {
    progress = 0;
  }

  @Override
  public String toString() {

    return getPrefix(" ") + String.valueOf(progress) + "%";
  }

  public void printOrUpdate(int newProgress) {
    if (newProgress > progress) {
      progress = newProgress;
      if (progress % 10 == 0)
        System.out.print(String.valueOf(progress) + "%");
      else
        System.out.print(".");
    }
  }

  public void printOrUpdate(long currentIndex, long totalCount) {
    int currentProgress = Math.round((float) currentIndex / totalCount * 100);
    this.printOrUpdate(currentProgress);
  }

  private String getPrefix(String prefixString) {
    String prefix = "";
    if (progress < 10)
      prefix = prefixString + prefixString;
    else if (progress < 100)
      prefix = prefixString;
    return prefix;
  }
}
