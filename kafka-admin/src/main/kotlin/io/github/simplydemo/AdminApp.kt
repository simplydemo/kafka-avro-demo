package io.github.simplydemo


import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import java.util.Properties

fun createTopic(topicName: String, numPartitions: Int, replicationFactor: Short, retentionMs: Long) {
    val props = Properties()
    props["bootstrap.servers"] = "kafka.simplydemo.local:9094"
    AdminClient.create(props).use { adminClient ->
        val topicConfig = mapOf("retention.ms" to retentionMs.toString())
        val newTopic = NewTopic(topicName, numPartitions, replicationFactor).configs(topicConfig)
        adminClient.createTopics(listOf(newTopic)).all().get()
        println("Topic $topicName created with $numPartitions partitions and retention time of $retentionMs ms")
    }
}

fun main() {
    val topicName = "user-topic"
    val numPartitions = 3
    val replicationFactor: Short = 1
    val retentionMs: Long = 5 * 60 * 1000 // 5 Min

    createTopic(topicName, numPartitions, replicationFactor, retentionMs)
}
