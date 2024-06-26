package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.google.common.util.concurrent.ThreadFactoryBuilder
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val properties4: ExternalServiceProperties,
    private val properties3: ExternalServiceProperties,
    private val properties2: ExternalServiceProperties,

    ) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        fun createNamedThreadFactory(name: String) : ThreadFactory {
            return ThreadFactoryBuilder().setNameFormat(name + "-%d").setDaemon(true).build();
        }
    }

    private val rateLimiter4 = RateLimiter(properties4.rateLimitPerSec)
    private val rateLimiter3 = RateLimiter(properties3.rateLimitPerSec)
    private val rateLimiter2 = RateLimiter(properties2.rateLimitPerSec)
    private val processTime4 = arrayListOf<Double>()
    private val processTime3 = arrayListOf<Double>()
    private val processTime2 = arrayListOf<Double>()

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val accountExecutor4 = ThreadPoolExecutor(properties4.parallelRequests,
        properties4.parallelRequests,
        paymentOperationTimeout.seconds,
        TimeUnit.SECONDS,
        ArrayBlockingQueue(properties4.parallelRequests),
        createNamedThreadFactory("account4"),
        ThreadPoolExecutor.CallerRunsPolicy())

    private val accountExecutor3 = ThreadPoolExecutor(properties3.parallelRequests,
        properties3.parallelRequests,
        paymentOperationTimeout.seconds,
        TimeUnit.SECONDS,
        ArrayBlockingQueue(properties3.parallelRequests),
        createNamedThreadFactory("account3"),
        ThreadPoolExecutor.CallerRunsPolicy())

    private val accountExecutor2 = ThreadPoolExecutor(properties2.parallelRequests,
        properties2.parallelRequests,
        paymentOperationTimeout.seconds,
        TimeUnit.SECONDS,
        ArrayBlockingQueue(properties2.parallelRequests),
        createNamedThreadFactory("account2"),
        ThreadPoolExecutor.CallerRunsPolicy())

    private val client4 = OkHttpClient.Builder()
        .dispatcher(Dispatcher(accountExecutor4))
        .protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
        .build()

    private val client3 = OkHttpClient.Builder()
        .dispatcher(Dispatcher(accountExecutor3))
        .protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
        .build()

    private val client2 = OkHttpClient.Builder()
        .dispatcher(Dispatcher(accountExecutor2))
        .protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
        .build()

    private fun chooseAccount(): Pair<ExternalServiceProperties, Boolean> {
        if (!rateLimiter4.tick()
            || accountExecutor4.queue.remainingCapacity() <= 0) {
            if (rateLimiter3.tick() && accountExecutor3.queue.remainingCapacity() > 0)
                return Pair(properties3, true)
            else {
                if (rateLimiter2.tick() && accountExecutor2.queue.remainingCapacity() > 0)
                    return Pair(properties2, true)
                else {
                    if (accountExecutor4.queue.remainingCapacity() <= 0 || (processTime4.average() > processTime2.average() && processTime4.average() > processTime3.average())){
                        if (accountExecutor3.queue.remainingCapacity() <= 0 || processTime3.average() > processTime2.average() )
                            return Pair(properties2, false)
                        return Pair(properties3, false)
                    }
                    else
                        return Pair(properties4, false)
                }
            }
        }
        else
            return Pair(properties4, true)
    }


    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        val (properties, isTick) = chooseAccount()

        logger.warn("[${properties.accountName}] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info("[${properties.accountName}] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val time = when (properties) {
            properties4 -> processTime4
            properties3 -> processTime3
            else -> processTime2
        }

        val accountExecutor = when (properties) {
            properties4 -> accountExecutor4
            properties3 -> accountExecutor3
            else -> accountExecutor2
        }

        try {
            if (!isTick) {
                if (time.average() == 0.0)
                    Thread.sleep(500)
                else
                    Thread.sleep((time.average() * 1000).toLong())
            }
            if (time.average() + ((now() - paymentStartedAt) / 1000) >= paymentOperationTimeout.seconds) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
                return
            }
            accountExecutor.submit {
                processPaymentRequest(paymentId, transactionId, paymentStartedAt, properties)
            }
        }
        catch (ex: RejectedExecutionException){
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
            }
        }
    }

    private fun processPaymentRequest(paymentId: UUID, transactionId: UUID, paymentStartedAt: Long, properties: ExternalServiceProperties) {
        if (Duration.ofSeconds((now() - paymentStartedAt) / 1000) >= paymentOperationTimeout) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
            }
            return
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${properties.serviceName}&accountName=${properties.accountName}&transactionId=${transactionId}")
            post(emptyBody)
        }.build()

        val client = when (properties) {
            properties4 -> client4
            properties3 -> client3
            else -> client2
        }

        try {
            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[${properties.accountName}] [ERROR] Payment processed for txId: ${transactionId}, payment: ${paymentId}, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }

                logger.warn("[${properties.accountName}] Payment processed for txId: ${transactionId}, payment: ${paymentId}, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[${properties.accountName}] Payment failed for txId: ${transactionId}, payment: ${paymentId}", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            val time = when (properties) {
                properties4 -> processTime4
                properties3 -> processTime3
                else -> processTime2
            }
            time.add((now() - paymentStartedAt).toDouble() / 1000)
        }
    }
}

public fun now() = System.currentTimeMillis()