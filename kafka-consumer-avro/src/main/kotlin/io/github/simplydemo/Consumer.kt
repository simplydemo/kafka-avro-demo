package io.github.simplydemo

import io.github.simplydemo.avro.User
import io.github.simplydemo.avro.UserDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.*


fun main() {

    val props = Properties().apply {
        put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.simplydemo.local:9094")
        put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, "user-group")
        put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializer::class.java)
        put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // 기존 메시지도 소비하도록 설정
    }

    /*
consumer = KafkaConsumer('your_topic',
                         group_id='your_group',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=False)
     */

    // Kafka 컨슈머 생성
    val consumer = KafkaConsumer<String, User>(props)
    consumer.subscribe(listOf("user-topic"))

    // 메시지 소비
    try {
        while (true) {
            val records: ConsumerRecords<String, User> = consumer.poll(Duration.ofMillis(100))
            for (record: ConsumerRecord<String, User> in records) {
                val user = record.value()
                println("Received message: ${user.name} (${user.age})")
            }
            consumer.commitSync() // 메시지 커밋
            println("Consumer-committed")
        }
    } finally {
        consumer.close()
    }
}