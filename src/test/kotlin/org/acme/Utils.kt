package org.acme

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.HeadBucketRequest
import java.io.File
import java.lang.RuntimeException
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.CompletionHandler
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.CompletableFuture
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

suspend fun <T> CompletableFuture<T>.await(): T =
    suspendCoroutine<T> { cont: Continuation<T> ->
        whenComplete { result, exception ->
            if (exception == null)
                cont.resume(result)
            else
                cont.resumeWithException(exception)
        }
    }

fun readFromFile(file: Path): Flow<ByteBuffer> = flow {
    val channel = AsynchronousFileChannel.open(file)
    channel.use {
        var filePosition = 0L
        while(true) {
            val buf = ByteBuffer.allocate(4096)
            val bytesRead = it.asyncRead(buf, filePosition)
            if(bytesRead <= 0)
                break
            filePosition += bytesRead
            buf.flip()
            // FIXME: uncomment the following line to mitigate the race-condition
            //        (this extends the test run time to ~ 6-7min)
            //delay(10)
            emit(buf)
        }
    }
}

suspend fun AsynchronousFileChannel.asyncRead(buf: ByteBuffer, position: Long): Int =
    suspendCoroutine { cont ->
        read(buf, position, Unit, object : CompletionHandler<Int, Unit> {
            override fun completed(bytesRead: Int, attachment: Unit) {
                cont.resume(bytesRead)
            }

            override fun failed(exception: Throwable, attachment: Unit) {
                cont.resumeWithException(exception)
            }
        })
    }

fun startMinIO(minIOExecutable: File): Process {
    val command = arrayOf(
        minIOExecutable.toString(),
        "server",
        "build/tmp/minio-data"
    )

    val process = ProcessBuilder(*command).start()
    println("MinIO started pid=${process.pid()}")
    println("Waiting 5 secs to let MinIO initialize...")
    Thread.sleep(5000)
    return process
}

fun createS3Client(): S3AsyncClient {

    val credentialsProvider = AwsCredentialsProvider {
        AwsBasicCredentials.create("minioadmin","minioadmin")
    }

    val httpClient: software.amazon.awssdk.http.async.SdkAsyncHttpClient = software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.builder()
            .writeTimeout(Duration.ZERO)
            .maxConcurrency(64)
            .build()
    val serviceConfiguration: S3Configuration = S3Configuration.builder()
            .checksumValidationEnabled(true)
            .chunkedEncodingEnabled(true)
            .build()

    var b: S3AsyncClientBuilder = S3AsyncClient.builder().httpClient(httpClient)
            .region(software.amazon.awssdk.regions.Region.of("test-region"))
            .credentialsProvider(credentialsProvider)
            .serviceConfiguration(serviceConfiguration)

    b = b.endpointOverride(URI("http://localhost:9000/"))

    return b.build()
}

fun createBucket(s3: S3AsyncClient, bucket: String) {
    try {
        s3.headBucket(HeadBucketRequest.builder().bucket(bucket).build()).join()

    } catch(e: Exception) {
        val bucketCreationResponse = s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build()).join()
        if(!bucketCreationResponse.sdkHttpResponse().isSuccessful) {
            throw RuntimeException()
        }
    }
}
