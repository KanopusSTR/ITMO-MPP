import java.util.concurrent.atomic.*;

public class Solution implements Lock<Solution.Node> {
	private final Environment env;

	// todo: необходимые поля (final, используем AtomicReference)
	private final AtomicReference<Node> tail = new AtomicReference<>(null); // shared, atomic


	public Solution(Environment env) {
		this.env = env;
	}

	@Override
	public Node lock() {
		Node my = new Node(); // сделали узел
		// todo: алгоритм
		my.locked.set(true);
		Node pred = tail.getAndSet(my);
		if (pred != null) {
			pred.next.set(my);
			while (my.locked.get()) {
				env.park();
			}
		}
		return my; // вернули узел
	}

	@Override
	public void unlock(Node node) {
		// todo: алгоритм
		if (node.next.get() == null) {
			if (tail.compareAndSet(node, null)) {
				return;
			} else {
				while (node.next.get() == null) {
					env.park();
				}
			}
		}
		Node next = node.next.get();
		next.locked.set(false);
		env.unpark(next.thread);
	}

	static class Node {
		final Thread thread = Thread.currentThread(); // запоминаем поток, которые создал узел
		// todo: необходимые поля (final, используем AtomicReference)
		final AtomicReference<Boolean> locked = new AtomicReference<>(false); // shared, atomic
		final AtomicReference<Node> next = new AtomicReference<>(null);

	}
}
