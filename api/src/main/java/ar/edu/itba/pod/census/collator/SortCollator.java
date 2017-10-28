package ar.edu.itba.pod.census.collator;

import com.hazelcast.mapreduce.Collator;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

public class SortCollator<K, V> implements Collator<Entry<K, V>, List<Entry<K, V>>> {

  private final Comparator<? super Entry<K, V>> comparator;

  public SortCollator(final Comparator<Entry<K, V>> comparator) {
    this.comparator = Objects.requireNonNull(comparator);
  }

  @Override
  public List<Entry<K, V>> collate(final Iterable<Entry<K, V>> entries) {
    final List<Entry<K, V>> sortedEntries = new LinkedList<>();

    entries.forEach(sortedEntries::add);
    sortedEntries.sort(comparator);

    return sortedEntries;
  }
}
