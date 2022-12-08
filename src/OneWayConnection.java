/**
 * NetFlow key used in port-scan analysis stages
 */
public class OneWayConnection extends TwoWayConnection {

  public OneWayConnection() {
    super();
  }

  public OneWayConnection(long srcIP, int srcPort, long dstIP, int dstPort) {
    super(srcIP, srcPort, dstIP, dstPort);
  }

  public OneWayConnection(IPPortPair src, IPPortPair dst) {
    super(src, dst);
  }

  // Get the source and destination IPs and ports
  @Override
  public String toString() {
    return convertIPToString(srcIP.get()) + ":" + srcPort.toString() +
        "\t->\t" +
        convertIPToString(dstIP.get()) + ":" + dstPort.toString();
  }
}

