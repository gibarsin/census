package ar.edu.itba.pod.census.predicate;

import com.hazelcast.mapreduce.KeyPredicate;
import java.util.Map;
import java.util.function.BiPredicate;

public class MapFilter<K, V> implements KeyPredicate<K> {

  private final Map<K, V> map;
  private final BiPredicate<K, V> predicate;

  public MapFilter(final Map<K, V> map, final BiPredicate<K, V> predicate) {
    this.map = map;
    this.predicate = predicate;
  }

  @Override
  public boolean evaluate(final K key) {
    return predicate.test(key, map.get(key));
  }
}
