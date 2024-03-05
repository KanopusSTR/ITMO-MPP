package dijkstra

import java.util.*
import java.util.concurrent.Phaser
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReferenceArray
import kotlin.Comparator
import kotlin.collections.ArrayList
import kotlin.concurrent.thread

private val NODE_DISTANCE_COMPARATOR = Comparator<Node> { o1, o2 -> Integer.compare(o1!!.distance, o2!!.distance) }

class PriorityMultiQueue(workersCount: Int) {
    private val queueCount = 4
    private val size = workersCount * queueCount
    private val queues = ArrayList<PriorityQueue<Node>>()
    private val locks = AtomicReferenceArray<Boolean>(size)
    init {
        for (i in 0..<size) {
            queues.add(PriorityQueue(size, NODE_DISTANCE_COMPARATOR))
            locks.set(i, false)
        }
    }

    fun insert(task: Node) {
        while (true) {
            val random = ThreadLocalRandom.current()
            val i1 = random.nextInt(size)
            val q = queues[i1]
            if (!locks.compareAndSet(i1, false, true)) continue
            q.add(task)
            locks.compareAndSet(i1, true, false)
            return
        }
    }

    fun delete(): Node? {
        while (true) {
            val random = ThreadLocalRandom.current()
            val i1 = random.nextInt(size)
            val i2 = random.nextInt(size)
            val q1 = queues[i1]
            val q2 = queues[i2]
            val q: PriorityQueue<Node>
            val i : Int
            if (q1.peek() == null) {
                i = i2
                q = q2
            } else if (q2.peek() == null || NODE_DISTANCE_COMPARATOR.compare(q1.peek(), q2.peek()) < 0) {
                i = i1
                q = q1
            } else {
                i = i2
                q = q2
            }
            if (!locks.compareAndSet(i, false, true)) continue
            val task = q.poll()
            locks.compareAndSet(i, true, false)
            return task
        }
    }
}

// Returns `Integer.MAX_VALUE` if a path has not been found.
fun shortestPathParallel(start: Node) {
    val workers = Runtime.getRuntime().availableProcessors()
    // The distance to the start node is `0`
    start.distance = 0
    // Create a priority (by distance) queue and add the start node into it
//    val q = PriorityQueue(workers, NODE_DISTANCE_COMPARATOR) // TODO replace me with a multi-queue based PQ!
    val q = PriorityMultiQueue(workers)
    q.insert(start)
    // Run worker threads and wait until the total work is done
    val onFinish = Phaser(workers + 1) // `arrive()` should be invoked at the end by each worker
    val jobsCount = AtomicInteger()
    jobsCount.getAndIncrement()
    repeat(workers) {
        thread {
            while (jobsCount.get() > 0) {
                // TODO Write the required algorithm here,
                // TODO break from this loop when there is no more node to process.
                // TODO Be careful, "empty queue" != "all nodes are processed".
                val cur: Node? = synchronized(q) { q.delete() }
                if (cur == null) continue
                for (e in cur.outgoingEdges) {
                    while (true) {
                        val dist1 = e.to.distance
                        val dist2 = cur.distance + e.weight
                        if (dist1 > dist2) {
                            if (e.to.casDistance(dist1, dist2)) {
                                q.insert(e.to)
                                jobsCount.getAndIncrement()
                            } else continue
                        }
                        break
                    }
                }
                jobsCount.getAndDecrement()
            }
            onFinish.arrive()
        }
    }
    onFinish.arriveAndAwaitAdvance()
}