package com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc

import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import kotlinx.coroutines.experimental.run
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.statements.InsertStatement
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.Instant
import javax.sql.DataSource
import org.joda.time.DateTime as JodaDateTime
import java.time.Instant as JavaInstant

class Database(name: String, dataSource: DataSource, poolSize: Int) {

    private val db = Database.connect(dataSource)

    private val context = newFixedThreadPoolContext(nThreads = poolSize, name = name)

    suspend fun <T> transaction(block: (DatabaseTransaction) -> T): T =
        run(context) {
            transaction(db) { block(object : DatabaseTransaction {
                override fun rollback() {
                    throw DatabaseTransaction.TransactionRollbackException
                }

                override fun rollback(throwable: Throwable) {
                   throw throwable
                }
            })}
        }
}

class UnexpectedNumberOfRowsAffectedInUpdate(val actual: Int, val expected: Int) : RuntimeException()

interface DatabaseTransaction {

    object TransactionRollbackException : RuntimeException()

    fun rollback()

    fun rollback(throwable: Throwable)
}

fun Table.instant(name: String): Column<Instant> = registerColumn(name, InstantColumnType(true))

private fun JodaDateTime.toInstantJava() = JavaInstant.ofEpochMilli(this.millis)
private fun JavaInstant.toJodaDateTime() = JodaDateTime(this.toEpochMilli())

class InstantColumnType(time: Boolean) : ColumnType() {
    private val delegate = DateColumnType(time)

    override fun sqlType(): String = delegate.sqlType()

    override fun nonNullValueToString(value: Any): String = when (value) {
        is JavaInstant -> delegate.nonNullValueToString(value.toJodaDateTime())
        else -> delegate.nonNullValueToString(value)
    }

    override fun valueFromDB(value: Any): Any {
        val fromDb = when (value) {
            is JavaInstant -> delegate.valueFromDB(value.toJodaDateTime())
            else -> delegate.valueFromDB(value)
        }
        return when (fromDb) {
            is JodaDateTime -> fromDb.toInstantJava()
            else -> error("failed to convert value to Instant")
        }
    }

    override fun notNullValueToDB(value: Any): Any = when (value) {
        is JavaInstant -> delegate.notNullValueToDB(value.toJodaDateTime())
        else -> delegate.notNullValueToDB(value)
    }
}

sealed class ConflictTarget(val name: String, val columns: List<Column<*>>) { abstract fun toSql(): String }
class PrimaryKeyConstraintTarget(table: Table, columns: List<Column<*>>): ConflictTarget("${table.nameInDatabaseCase()}_pkey", columns) {
    override fun toSql() = "ON CONFLICT ON CONSTRAINT $name"
}
class ColumnTarget(column: Column<*>): ConflictTarget(column.name, listOf(column)) {
    override fun toSql() = "ON CONFLICT($name)"
}
class IndexTarget(index: Index): ConflictTarget(index.indexName, index.columns) {
    override fun toSql() = "ON CONFLICT($name)"
}

class UpsertStatement<Key : Any>(table: Table, val conflictTarget: ConflictTarget, val where: Op<Boolean>? = null) : InsertStatement<Key>(table, false) {

    override fun prepareSQL(transaction: Transaction) = buildString {
        append(super.prepareSQL(transaction))
        append(" ")
        append(conflictTarget.toSql())
        append(" DO UPDATE SET ")
        values.keys.filter { it !in conflictTarget.columns }.joinTo(this) { "${transaction.identity(it)}=EXCLUDED.${transaction.identity(it)}" }

        val builder = QueryBuilder(true)
        where?.let { append(" WHERE " + it.toSQL(builder)) }
    }

    override fun arguments(): List<List<Pair<IColumnType, Any?>>> {
        val superArgs = super.arguments().first()

        QueryBuilder(true).run {
            where?.toSQL(this)
            return listOf(superArgs + args)
        }
    }
}

class InsertOnConflictDoNothingStatement<Key : Any>(table: Table, val conflictTarget: ConflictTarget) : InsertStatement<Key>(table, false) {

    override fun prepareSQL(transaction: Transaction) = buildString {
        append(super.prepareSQL(transaction))
        append(" ")
        append(conflictTarget.toSql())
        append(" DO NOTHING")
    }
}

fun <T: Table> T.insertOnConflictDoNothing(conflictTarget: ConflictTarget, body: T.(InsertOnConflictDoNothingStatement<Number>) -> Unit): Int {
    val query = InsertOnConflictDoNothingStatement<Number>(this, conflictTarget)
    body(query)
    return query.execute(TransactionManager.current())!!
}

fun <T : Table> T.upsert(conflictTarget: ConflictTarget, where: (SqlExpressionBuilder.()->Op<Boolean>)? = null, body: T.(UpsertStatement<Number>) -> Unit): Int {
    val query = UpsertStatement<Number>(this, conflictTarget, where?.let { SqlExpressionBuilder.it() })
    body(query)
    return query.execute(TransactionManager.current())!!
}

fun Table.indexR(customIndexName: String? = null, isUnique: Boolean = false, vararg columns: Column<*>): Index {
    val index = Index(columns.toList(), isUnique, customIndexName)
    indices.add(index)
    return index
}

fun Table.uniqueIndexR(customIndexName: String? = null, vararg columns: Column<*>): Index = indexR(customIndexName, true, *columns)

fun Table.primaryKeyConstraintConflictTarget(vararg columns: Column<*>): ConflictTarget = PrimaryKeyConstraintTarget(this, columns.toList())

fun Table.uniqueIndexConflictTarget(customIndexName: String? = null, vararg columns: Column<*>): IndexTarget = IndexTarget(uniqueIndexR(customIndexName, *columns))