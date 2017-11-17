package com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc

import com.github.andrewoma.kwery.core.DefaultSession
import com.github.andrewoma.kwery.core.Session
import com.github.andrewoma.kwery.core.dialect.Dialect
import com.github.andrewoma.kwery.core.interceptor.LoggingInterceptor
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import kotlinx.coroutines.experimental.run
import java.sql.Connection
import javax.sql.DataSource
import kotlin.coroutines.experimental.AbstractCoroutineContextElement
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.coroutines.experimental.intrinsics.suspendCoroutineOrReturn

object TransactionRollbackException : RuntimeException()

class Database(val name: String, val dataSource: DataSource, val poolSize: Int, val dialect: Dialect) {

    val context = newFixedThreadPoolContext(nThreads = poolSize, name = name) + DataSourceContext(dataSource)

    suspend fun <T> transaction(block: suspend (Transaction) -> T): T {
        val transaction = currentTransaction()
        return if (transaction == null) {
            newTransaction(block)
        } else {
            block(transaction)
        }
    }

    suspend fun currentTransaction(): Transaction? = coroutineContext()[TransactionContext]

    private suspend fun <T> newTransaction(block: suspend (Transaction) -> T): T = run(context) {
        val connection = context.dataSource.connection
        try {
            connection.autoCommit = false
            val transactionContext = TransactionContext(connection)
            val newContext = context + transactionContext
            run(newContext) {
                val response = block(transactionContext)
                if (transactionContext.rollbackOnly) {
                    throw TransactionRollbackException
                } else {
                    connection.commit()
                }
                response
            }
        } catch (throwable: Throwable) {
            connection.rollback()
            throw throwable
        } finally {
            connection.close()
        }
    }

    suspend fun <T> withConnection(block: suspend (Connection) -> T): T {
        val connection = coroutineContext().connection
        return if (connection == null) {
            run(context) {
                val newConnection = context.dataSource.connection
                try {
                    block(newConnection)
                } finally {
                    newConnection.close()
                }
            }
        } else {
            block(connection)
        }
    }

    suspend fun <T> withSession(block: suspend (Session) -> T): T = withConnection { connection ->
        block(DefaultSession(connection, dialect, interceptor = LoggingInterceptor()))
    }
}

interface Transaction {
    var rollbackOnly: Boolean
}

private suspend fun coroutineContext(): CoroutineContext = suspendCoroutineOrReturn { it.context }

private class TransactionContext(val connection: Connection, override var rollbackOnly: Boolean = false) :
        AbstractCoroutineContextElement(TransactionContext), Transaction {
    companion object Key : CoroutineContext.Key<TransactionContext>
}

private class DataSourceContext(val dataSource: DataSource) : AbstractCoroutineContextElement(DataSourceContext) {
    companion object Key : CoroutineContext.Key<DataSourceContext>
}

private val CoroutineContext.dataSource
    get() = this[DataSourceContext]?.dataSource ?: throw IllegalStateException("DataSourceContext not in coroutine scope")

private val CoroutineContext.connection
    get() = this[TransactionContext]?.connection