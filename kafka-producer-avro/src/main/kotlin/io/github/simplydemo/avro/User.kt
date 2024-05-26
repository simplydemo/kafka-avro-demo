package io.github.simplydemo.avro

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.kafka.common.serialization.Serializer
import java.nio.charset.StandardCharsets

@kotlinx.serialization.Serializable
data class User(val name: String, val age: Int)

class UserSerializer : Serializer<User> {
    override fun serialize(topic: String, data: User): ByteArray {
        return Json.encodeToString(data).toByteArray(StandardCharsets.UTF_8)
    }
}


