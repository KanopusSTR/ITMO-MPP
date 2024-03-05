import java.util.concurrent.atomic.*

/**
 * @author Rynk Artur
 *
 * TODO: Copy the code from `FAABasedQueueSimplified`
 * TODO: and implement the infinite array on a linked list
 * TODO: of fixed-size `Segment`s.
 */
class FAABasedQueue<E> : Queue<E> {

    private val first = Segment(0)
    private val tail = AtomicReference(first)
    private val head = AtomicReference(first)
    private val enqIdx = AtomicLong(0)
    private val deqIdx = AtomicLong(0)

    private fun findSegment(start: Segment, id: Long): Segment {
        var segment = start
        for (i in segment.id..<(id * SEGMENT_SIZE)) {
            val tmp = Segment(i + 1)
            segment.next.compareAndSet(null, tmp)
            segment = segment.next.get()!!
        }
        if (start.id > id) {
            segment = findSegment(first, id)
        }
        return segment
    }

    override fun enqueue(element: E) {
        while (true) {
            val curTail = tail.get()
            val i = enqIdx.getAndIncrement()
            val s = findSegment(start = curTail, id = i / SEGMENT_SIZE)
            moveTailForward(s, curTail)
            if (s.cells.compareAndSet((i % SEGMENT_SIZE).toInt(), null, element)) return
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun dequeue(): E? {
        while (true) {
            if (!shouldTryToDequeue()) return null
            val curHead = head.get()
            val i = deqIdx.getAndIncrement()
            val s = findSegment(start = curHead, id = i / SEGMENT_SIZE)
            moveHeadForward(s, curHead)
            if (!s.cells.compareAndSet((i % SEGMENT_SIZE).toInt(), null, POISONED)) {
                return s.cells.get((i % SEGMENT_SIZE).toInt()) as E?
            }
        }
    }

    private fun moveTailForward(segment: Segment, compSegment: Segment) {
        if (segment.id > compSegment.id) {
            segment.next.compareAndSet(null, Segment(segment.id + 1))
            tail.set(segment)
        }
    }

    private fun moveHeadForward(segment: Segment, compSegment: Segment) {
        if (segment.id > compSegment.id) {
            segment.next.compareAndSet(null, Segment(segment.id + 1))
            head.set(segment)
        }
    }

    private fun shouldTryToDequeue(): Boolean {
        while (true) {
            val curEnqIdx = enqIdx.get()
            val curDeqIdx = deqIdx.get()
            if (curEnqIdx == enqIdx.get()) return curDeqIdx < curEnqIdx
        }
    }
}

private class Segment(val id: Long) {
    val next = AtomicReference<Segment?>(null)
    val cells = AtomicReferenceArray<Any?>(SEGMENT_SIZE)
}

// DO NOT CHANGE THIS CONSTANT
private const val SEGMENT_SIZE = 2
private val POISONED = Any()
