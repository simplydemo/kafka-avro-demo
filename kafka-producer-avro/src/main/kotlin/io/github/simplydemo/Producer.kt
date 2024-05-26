package io.github.simplydemo

import io.github.simplydemo.avro.User
import io.github.simplydemo.kafka.producer.UserProducer


fun generateRandomUsername(): String {
    val adjectives = listOf("Friendly", "Brave", "Curious", "Clever", "Adventurous", "Playful")
    val nouns = listOf("Lion", "Panda", "Giraffe", "Elephant", "Dolphin", "Koala")
    val numbers = (100..999).toList()
    val randomAdjective = adjectives.random()
    val randomNoun = nouns.random()
    val randomNumber = numbers.random()
    return "$randomAdjective$randomNoun$randomNumber"
}


fun main() {

    val userTopic = "user-topic"
    val userKey = "user01"

    val producer = UserProducer()
    val user = User(generateRandomUsername(), 48)
    producer.sendMessage(userTopic, userKey, user)

}