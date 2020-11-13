public class Pair<A, B> {
	private final A first;
	private final B second;

	public Pair(A first, B second) {
		this.first = first;
		this.second = second;
	}

	public A getFirst() {
		return first;
	}

	public B getSecond() {
		return second;
	}

	public int hashCode() {
		int hashFirst = first != null ? first.hashCode() : 0;
		int hashSecond = second != null ? second.hashCode() : 0;
		return (hashFirst + hashSecond) * hashSecond + hashFirst;
	}

	public boolean equals(Object other) {
		if (!(other instanceof Pair<?, ?>)) {
			return false;
		}

		Pair<?, ?> p = (Pair<?, ?>) other;
		boolean firstEquals = getFirst() == null ? p.getFirst() == null : getFirst().equals(p.getFirst());
		boolean secondEquals = getSecond() == null ? p.getSecond() == null : getSecond().equals(p.getSecond());
		return firstEquals && secondEquals;
	}

	public String toString() {
		return "(" + first + ", " + second + ")";
	}
}
