
/*
 * A simple generic Pair class. Helps with stories.
 */
public class Pair<K,V> {
	private K key;
	private V value;
	
	public Pair(K key, V value) {
		this.key = key;
		this.value = value;
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}
	
	@Override
	public boolean equals(Object o) {
		
		if (o == this) {
            return true;
        }
		
		 if (!(o instanceof Pair)) {
			 return false;
	     }
		 
		 Pair<K,V> p = (Pair<K,V>)o;
		 return this.key.equals(p.key) && this.value.equals(p.value);
	}
	
}
