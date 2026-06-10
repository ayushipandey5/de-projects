package skystreamprocessor.Constants

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

class FlightTimestampExtractor extends TimestampExtractor{
  override def extract(record: ConsumerRecord[AnyRef, AnyRef], partitionTime: Long): Long = {
    try{
      val jsonString = record.value().toString
      val jsonObj = ujson.read(jsonString)

      val flightTime = jsonObj("last_contact").num.toLong * 1000L

      flightTime
    } catch {
      case _: Exception =>
        // If parsing fails, fall back to the last known good time, or the metadata time
        if (partitionTime > 0) partitionTime else System.currentTimeMillis()
    }
  }
}
