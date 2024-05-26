package io.github.simplydemo.kafka.support

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

abstract class ProducerTemplate<K, V> {


    private lateinit var producer: KafkaProducer<K, V>

    fun sendMessage(topic: String, key: K, value: V) {
        initProducer()
        try {
            execute(topic, key, value)
        } finally {
            closeProducer()
        }
    }

    abstract fun initProducer()

    abstract fun closeProducer()

    protected fun createProducer(props: Properties): KafkaProducer<K, V> {
        return KafkaProducer(props)
    }

    protected fun getProducer(): KafkaProducer<K, V> {
        return producer
    }

    protected fun setProducer(producer: KafkaProducer<K, V>) {
        this.producer = producer
    }

    private fun execute(topic: String, key: K, value: V) {
        val producer = getProducer()
        val record = ProducerRecord(topic, key, value)
        producer.send(record) { metadata, exception ->
            if (exception != null) {
                println("Error sending message: ${exception.message}")
            } else {
                println("Message sent to topic ${metadata.topic()} partition ${metadata.partition()} with offset ${metadata.offset()}")
            }
        }
    }

//    protected fun send(producer: KafkaProducer<String, String>, topic: String, key: String, value: String) {
//        val record = ProducerRecord(topic, key, value)
//        producer.send(record) { metadata, exception ->
//            if (exception != null) {
//                println("Error sending message: ${exception.message}")
//            } else {
//                println("Message sent to topic ${metadata.topic()} partition ${metadata.partition()} with offset ${metadata.offset()}")
//            }
//        }
//    }


}