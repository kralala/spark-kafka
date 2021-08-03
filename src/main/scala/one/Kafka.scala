package one

//custom receiver with a template from spark.apache.org

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.pcap4j.packet.Packet
import org.pcap4j.core.PcapHandle
import org.pcap4j.core.PcapNetworkInterface
import org.pcap4j.core.Pcaps
import org.pcap4j.core.NotOpenException
import org.pcap4j.core.PcapNativeException
import java.util.Objects
import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket


class Kafka extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart(): Unit = {
    new Thread("Receiver") {
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  def onStop() {}

  @throws[PcapNativeException]
  private def openPacketHandlerOn(networkInterface: PcapNetworkInterface) = {
    val len = 65535
    val time = 100
    networkInterface.openLive(len, PcapNetworkInterface.PromiscuousMode.PROMISCUOUS, time)
  }

  @throws[PcapNativeException]
  @throws[NotOpenException]
  private def receive(): Unit = {
    val interfaceAny = Pcaps.getDevByName("any")
    val packetHandler = openPacketHandlerOn(interfaceAny)
    while ({
      Objects.requireNonNull(packetHandler).getNextPacketEx != null
    }) {println(packetHandler.getNextPacketEx)}
  }
}