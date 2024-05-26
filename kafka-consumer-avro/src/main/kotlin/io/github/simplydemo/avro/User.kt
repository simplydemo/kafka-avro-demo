package io.github.simplydemo.avro

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import java.nio.charset.StandardCharsets

@kotlinx.serialization.Serializable
data class User(val name: String, val age: Int)

class UserDeserializer : Deserializer<User> {
    override fun deserialize(topic: String, data: ByteArray): User {
        return try {
            Json.decodeFromString(String(data, StandardCharsets.UTF_8))
        } catch (e: Exception) {
            throw SerializationException("Error deserializing message", e)
        }
    }
}