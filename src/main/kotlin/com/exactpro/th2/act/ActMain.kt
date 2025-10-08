/*
 * Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@file:JvmName("ActMain")
package com.exactpro.th2.act

import com.exactpro.th2.act.impl.SubscriptionManagerImpl
import com.exactpro.th2.check1.grpc.Check1Service
import com.exactpro.th2.common.metrics.LIVENESS_MONITOR
import com.exactpro.th2.common.metrics.READINESS_MONITOR
import com.exactpro.th2.common.schema.factory.CommonFactory
import io.github.oshai.kotlinlogging.KotlinLogging
import java.lang.AutoCloseable
import java.util.Deque
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Consumer
import kotlin.system.exitProcess

private val LOGGER = KotlinLogging.logger { }
private const val OE_ATTRIBUTE_NAME = "oe"

fun main(args: Array<String>) {
    val resources: Deque<AutoCloseable?> = ConcurrentLinkedDeque<AutoCloseable?>()
    val lock = ReentrantLock()
    val condition = lock.newCondition()
    configureShutdownHook(resources, lock, condition)

    try {
        // You need to initialize the CommonFactory
        // You can use custom paths to each config that is required for the CommonFactory
        // If args are empty the default path will be chosen.
        val factory = CommonFactory.createFromArguments(*args)
        // do not forget to add resource to the resources queue
        resources.add(factory::close)

        setupApp(factory, resources::add)

        awaitShutdown(lock, condition)
    } catch (e: InterruptedException) {
        LOGGER.info(e) { "The main thread interrupted" }
    } catch (e: Exception) {
        LOGGER.error(e) { "Fatal error: ${e.message}" }
        exitProcess(1)
    }
}

private fun awaitShutdown(lock: ReentrantLock, condition: Condition) {
    try {
        lock.lock()
        LOGGER.info { "Wait shutdown" }
        condition.await()
        LOGGER.info { "App shutdown" }
    } finally {
        lock.unlock()
    }
}

private fun configureShutdownHook(resources: Deque<AutoCloseable?>, lock: ReentrantLock, condition: Condition) {
    Runtime.getRuntime().addShutdownHook(object : Thread("Shutdown hook") {
        override fun run() {
            LOGGER.info { "Shutdown start" }
            READINESS_MONITOR.disable()
            try {
                lock.lock()
                condition.signalAll()
            } finally {
                lock.unlock()
            }

            resources.descendingIterator().forEachRemaining(Consumer { resource: AutoCloseable? ->
                try {
                    resource!!.close()
                } catch (e: Exception) {
                    LOGGER.error(e) { e.message }
                }
            })
            LIVENESS_MONITOR.disable()
            LOGGER.info { "Shutdown end" }
        }
    })
}

fun setupApp(
    factory: CommonFactory,
    closeResource: (resource: AutoCloseable) -> Unit,
) {
    LIVENESS_MONITOR.enable()

    val grpcRouter = factory.grpcRouter.also(closeResource)

    val messageRouter = factory.transportGroupBatchRouter.also(closeResource)

    val configuration = factory.getCustomConfiguration(Configuration::class.java)

    val subscriptionManager = SubscriptionManagerImpl()
    messageRouter.subscribeAll(subscriptionManager, OE_ATTRIBUTE_NAME).also { closeResource(it::unsubscribe) }

    val actHandler = ActHandler(
        messageRouter,
        subscriptionManager,
        factory.eventBatchRouter,
        if (configuration.isCheck1Enabled) grpcRouter.getService(Check1Service::class.java) else null,
        configuration.responseTimeout,
    )
    ActServer(grpcRouter.startServer(actHandler)).also { closeResource(it::stop) }
    READINESS_MONITOR.enable()
    LOGGER.info { "Act started" }
}
