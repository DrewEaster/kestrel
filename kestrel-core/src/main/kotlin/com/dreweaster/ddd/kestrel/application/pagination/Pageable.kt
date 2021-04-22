package com.dreweaster.ddd.kestrel.application.pagination

import io.vavr.kotlin.Try

data class Pageable(val pageNumber: Int, val pageSize: Int) {
    val offset = (pageNumber - 1) * pageSize
}

interface Page<T> : Collection<T> {

    companion object {

        fun <T> single(values: List<T>, pageable: Pageable): Page<T> = SinglePage(values, pageable)

        fun <T> slice(values: List<T>, totalElements: Int, pageable: Pageable): Page<T> = SlicedPage(values, totalElements, pageable)
    }

    val isFirst: Boolean

    val isLast: Boolean

    val values: List<T>

    val totalElements: Int

    val totalPages: Int

    val pageSize: Int

    val pageNumber: Int

    val hasPrevious: Boolean

    val hasNext: Boolean
}

data class SinglePage<T>(override val values: List<T>, private val pageable: Pageable) : Page<T> {
    override val isFirst = true
    override val isLast = true
    override val size = values.count()
    override val totalElements = values.count()
    override val totalPages = 1
    override val pageSize = pageable.pageSize
    override val pageNumber = pageable.pageNumber
    override val hasPrevious = false
    override val hasNext = false

    override fun contains(element: T): Boolean = values.contains(element)

    override fun containsAll(elements: Collection<T>): Boolean = values.containsAll(elements)

    override fun isEmpty(): Boolean = values.isEmpty()

    override fun iterator(): Iterator<T> = values.iterator()
}

data class SlicedPage<T>(override val values: List<T>, override val totalElements: Int, private val pageable: Pageable) : Page<T> {
    override val size = values.count()
    override val totalPages: Int = Try { (totalElements + pageable.pageSize - 1) / pageable.pageSize }.toOption().getOrElse(0)
    override val pageSize = pageable.pageSize
    override val pageNumber = pageable.pageNumber
    override val isFirst = pageable.pageNumber == 1
    override val isLast = pageable.pageNumber == totalPages || totalPages == 0
    override val hasPrevious = pageable.pageNumber > 1
    override val hasNext = !isLast

    override fun contains(element: T): Boolean = values.contains(element)

    override fun containsAll(elements: Collection<T>): Boolean = values.containsAll(elements)

    override fun isEmpty(): Boolean = values.isEmpty()

    override fun iterator(): Iterator<T> = values.iterator()
}
