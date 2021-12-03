package com.dreweaster.ddd.kestrel.util.json

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.*

private val _objectMapper = ObjectMapper()

private fun <T : Any> JsonNode?._nullOr(getNotNull: JsonNode.() -> T) : T?
        = if (this == null || isNull) null else getNotNull()

val JsonNode.string: String get() = asText()
val JsonNode?.nullString: String? get() = _nullOr { string }

val JsonNode.bool: Boolean get() = asBoolean()
val JsonNode?.nullBool: Boolean? get() = _nullOr { bool }

val JsonNode.int: Int get() = asInt()
val JsonNode?.nullInt: Int? get() = _nullOr { int }

val JsonNode.long: Long get() = asLong()
val JsonNode?.nullLong: Long? get() = _nullOr { long }

val JsonNode.double: Double get() = asDouble()
val JsonNode?.nullDouble: Double? get() = _nullOr { double }

val JsonNode.array: ArrayNode get() = this as ArrayNode
val JsonNode?.nullArray: ArrayNode? get() = _nullOr { array }

val JsonNode.obj: ObjectNode get() = this as ObjectNode
val JsonNode?.nullObj: ObjectNode? get() = _nullOr { obj }

val jsonNull: NullNode = NullNode.instance

operator fun JsonNode.get(key: String): JsonNode = obj.getNotNull(key)
operator fun JsonNode.get(index: Int): JsonNode = array.get(index)

fun ObjectNode.getNotNull(key: String): JsonNode = get(key) ?: throw NoSuchElementException("'$key' is not found")

operator fun ObjectNode.plus(property: Pair<String, *>): ObjectNode = set<JsonNode>(property.first, property.second.toJsonNode()).obj
operator fun ObjectNode.minus(propertyName: String): ObjectNode = without<JsonNode>(propertyName).obj

internal fun Any?.toJsonNode(): JsonNode {
    if (this == null)
        return jsonNull

    return when (this) {
        is JsonNode -> this
        is String -> TextNode(this)
        is Int -> IntNode(this)
        is Long -> LongNode(this)
        is Double -> DoubleNode(this)
        is Boolean -> BooleanNode.valueOf(this)
        else -> throw IllegalArgumentException("${this} cannot be converted to JSON")
    }
}

private fun _jsonObject(values: Iterator<Pair<String, *>>): ObjectNode {
    val obj = _objectMapper.createObjectNode()
    for ((key, value) in values) {
        obj.set<JsonNode>(key, value.toJsonNode())
    }
    return obj
}

fun jsonObject(vararg values: Pair<String, *>) = _jsonObject(values.iterator())
fun jsonObject(values: Iterable<Pair<String, *>>) = _jsonObject(values.iterator())
fun jsonObject(values: Sequence<Pair<String, *>>) = _jsonObject(values.iterator())

fun Iterable<Pair<String, *>>.toJsonObject() = jsonObject(this)
fun Sequence<Pair<String, *>>.toJsonObject() = jsonObject(this)

private fun _jsonArray(values: Iterator<Any?>): ArrayNode {
    val array = _objectMapper.createArrayNode()
    for (value in values)
        array.add(value.toJsonNode())
    return array
}

fun jsonArray(vararg values: Any?) = _jsonArray(values.iterator())
fun jsonArray(values: Iterable<*>) = _jsonArray(values.iterator())
fun jsonArray(values: Sequence<*>) = _jsonArray(values.iterator())

fun Iterable<*>.toJsonArray() = jsonArray(this)
fun Sequence<*>.toJsonArray() = jsonArray(this)

fun ObjectNode.shallowCopy(): ObjectNode = _objectMapper.createObjectNode().apply { this@shallowCopy.fields().forEach { set<JsonNode>(it.key, it.value) } }
fun ArrayNode.shallowCopy(): ArrayNode = _objectMapper.createArrayNode().apply { addAll(this@shallowCopy) }