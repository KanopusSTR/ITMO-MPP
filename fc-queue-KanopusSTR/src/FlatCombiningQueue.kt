import java.util.concurrent.*
import java.util.concurrent.atomic.*

/**
 * @author :Rynk Artur
 */

class FlatCombiningQueue<E> : Queue<E> {
    private val queue = ArrayDeque<E>() // sequential queue
    private val combinerLock = AtomicBoolean(false) // unlocked initially
    private val tasksForCombiner = AtomicReferenceArray<Any?>(TASKS_FOR_COMBINER_SIZE)

    @Suppress("UNCHECKED_CAST")
    override fun enqueue(element: E) {
        // TODO: Make this code thread-safe using the flat-combining technique.
        // TODO: 1.  Try to become a combiner by
        // TODO:     changing `combinerLock` from `false` (unlocked) to `true` (locked).
        // TODO: 2a. On success, apply this operation and help others by traversing
        // TODO:     `tasksForCombiner`, performing the announced operations, and
        // TODO:      updating the corresponding cells to `Result`.
        // TODO: 2b. If the lock is already acquired, announce this operation in
        // TODO:     `tasksForCombiner` by replacing a random cell state from
        // TODO:      `null` with the element. Wait until either the cell state
        // TODO:      updates to `Result` (do not forget to clean it in this case),
        // TODO:      or `combinerLock` becomes available to acquire.
        var wait = false
        val rci = randomCellIndex()
        while (true) {
            if (combinerLock.compareAndSet(false, true)) {
                if (wait) {
                    val task = tasksForCombiner.getAndSet(rci, null)
                    if (task is Result<*>) {
                        combinerLock.set(false)
                        return
                    }
                }
                queue.addLast(element)
                for (i in 0..<TASKS_FOR_COMBINER_SIZE) {
                    val task = tasksForCombiner.get(i)
                    if (task is Dequeue) {
                        tasksForCombiner.set(i, Result(queue.removeFirstOrNull()))
                    } else if (task != null && (task as E?) !is Result<*>) {
                        queue.addLast(task as E)
                        tasksForCombiner.set(i, Result<E?>(task))
                    }
                }
                return Unit.also { combinerLock.set(false) }
            } else if (tasksForCombiner.compareAndSet(rci, null, element)) {
                wait = true
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun dequeue(): E? {
        // TODO: Make this code thread-safe using the flat-combining technique.
        // TODO: 1.  Try to become a combiner by
        // TODO:     changing `combinerLock` from `false` (unlocked) to `true` (locked).
        // TODO: 2a. On success, apply this operation and help others by traversing
        // TODO:     `tasksForCombiner`, performing the announced operations, and
        // TODO:      updating the corresponding cells to `Result`.
        // TODO: 2b. If the lock is already acquired, announce this operation in
        // TODO:     `tasksForCombiner` by replacing a random cell state from
        // TODO:      `null` with `Dequeue`. Wait until either the cell state
        // TODO:      updates to `Result` (do not forget to clean it in this case),
        // TODO:      or `combinerLock` becomes available to acquire.
        var wait = false
        val rci = randomCellIndex()
        while (true) {
            if (combinerLock.compareAndSet(false, true)) {
                if (wait) {
                    val task = tasksForCombiner.getAndSet(rci, null)
                    if (task is Result<*>) {
                        combinerLock.set(false)
                        return task.value as E?
                    }
                }
                return queue.removeFirstOrNull().also {
                    combinerLock.set(false)
                }
            } else if (tasksForCombiner.compareAndSet(rci, null, Dequeue)) {
                wait = true
            }
        }
    }

    private fun randomCellIndex(): Int =
        ThreadLocalRandom.current().nextInt(tasksForCombiner.length())
}

private const val TASKS_FOR_COMBINER_SIZE = 3 // Do not change this constant!

// TODO: Put this token in `tasksForCombiner` for dequeue().
// TODO: enqueue()-s should put the inserting element.
private object Dequeue

// TODO: Put the result wrapped with `Result` when the operation in `tasksForCombiner` is processed.
private class Result<V>(
    val value: V
)