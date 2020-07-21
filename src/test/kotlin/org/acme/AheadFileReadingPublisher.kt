package org.acme

import org.reactivestreams.*
import java.nio.*
import java.nio.channels.*
import java.nio.file.*
import java.util.concurrent.atomic.*

private sealed class State
private object Completed : State()
private data class Next(val buf: ByteBuffer) : State()
private data class Requested(val count: Long) : State()

@Suppress("ReactiveStreamsPublisherImplementation")
class AheadFileReadingPublisher(val file: Path) : Publisher<ByteBuffer> {
    override fun subscribe(subscriber: Subscriber<in ByteBuffer>) {
        val channel = AsynchronousFileChannel.open(file)
        var filePosition = 0L
        fun notify(event: State) {
            when (event) {
                is Completed -> subscriber.onComplete()
                is Next -> subscriber.onNext(event.buf)
                else -> error("Cannot happen")
            }
        }
        val state = AtomicReference<State?>(null)
        fun sendRequest() {
            val buf = ByteBuffer.allocate(4096)
            channel.read(buf, filePosition, Unit, object : CompletionHandler<Int, Unit> {
                override fun completed(bytesRead: Int, attachment: Unit) {
                    val event = if (bytesRead <= 0) {
                        Completed
                    } else {
                        filePosition += bytesRead
                        buf.flip()
                        Next(buf)
                    }
                    val upd = state.updateAndGet { state ->
                        when (state) {
                            is Requested -> if (state.count > 0) Requested(state.count - 1) else event
                            else -> error("Cannot happen")
                        }
                    }
                    if (upd is Requested) {
                        notify(event)
                        sendRequest()
                    }
                }
                override fun failed(ex: Throwable, attachment: Unit) { subscriber.onError(ex) }
            })
        }
        subscriber.onSubscribe(object : Subscription {
            override fun cancel() {}
            override fun request(delta: Long) {
                if (delta <= 0) return
                val prev = state.getAndUpdate { state ->
                    when(state) {
                        is Requested -> Requested(state.count + delta)
                        else -> Requested(delta - 1)
                    }
                }
                if (prev is Completed || prev is Next) notify(prev)
                if (prev !is Requested) sendRequest()
            }
        })
    }
}