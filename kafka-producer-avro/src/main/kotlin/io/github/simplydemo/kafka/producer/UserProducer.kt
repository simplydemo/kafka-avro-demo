package io.github.simplydemo.kafka.producer

import io.github.simplydemo.avro.User
import io.github.simplydemo.avro.UserSerializer
import io.github.simplydemo.kafka.support.ProducerTemplate
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

class UserProducer : ProducerTemplate<String, User>() {

    private val props = Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.simplydemo.local:9094")
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer::class.java)
        put(ProducerConfig.ACKS_CONFIG, "all")
    }

    override fun initProducer() {
        setProducer(createProducer(props))
    }

    override fun closeProducer() {
        getProducer().close()
    }
}