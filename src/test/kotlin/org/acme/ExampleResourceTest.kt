package org.acme

import io.quarkus.test.junit.QuarkusTest
import io.reactivex.Flowable
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.reactive.asPublisher
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.io.File
import java.nio.ByteBuffer
import javax.ws.rs.core.MediaType

@QuarkusTest
class ExampleResourceTest {

    @Test
    fun `Failing test using Kotlin Coroutine's Flow`() = runBlocking {
        val minIOExecutable = File("minio").absoluteFile

        val requestBody = AsyncRequestBody.fromPublisher(readFromFile(minIOExecutable.toPath()).asPublisher())

        runTestWorkflow(minIOExecutable, "test1-bucket", requestBody)
    }

    @Test
    fun `Passing test using Kotlin Coroutine's Flow but caching in-memory`() = runBlocking {
        val minIOExecutable = File("minio").absoluteFile

        val chunks = ArrayList<ByteBuffer>()
        readFromFile(minIOExecutable.toPath()).collect {
            chunks.add(it)
        }

        val requestBody = AsyncRequestBody.fromPublisher(Flowable.fromIterable(chunks))

        runTestWorkflow(minIOExecutable, "test2-bucket", requestBody)
    }

    @Test
    fun `Passing test using S3 built-in Publisher`() = runBlocking {
        val minIOExecutable = File("minio").absoluteFile

        val chunks = ArrayList<ByteBuffer>()
        readFromFile(minIOExecutable.toPath()).collect {
            chunks.add(it)
        }

        val requestBody = AsyncRequestBody.fromFile(minIOExecutable)

        runTestWorkflow(minIOExecutable, "test3-bucket", requestBody)
    }

    suspend fun runTestWorkflow(minIOExecutable: File, bucket: String, requestBody: AsyncRequestBody) {
        val minio = startMinIO(minIOExecutable)
        val s3 = createS3Client()
        createBucket(s3, bucket)

        try {

            val future = s3.putObject(PutObjectRequest.builder()
                    .bucket(bucket)
                    .key("test-file")
                    .contentLength(minIOExecutable.length())
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .build(),
                    requestBody)

            future.await()

        } finally {
            minio.destroy()
        }
    }
}