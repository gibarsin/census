package ar.edu.itba.pod.census.collator;

import com.hazelcast.mapreduce.Collator;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

public class MinIntegerValueSortCollator<K> implements
    Collator<Entry<K, Integer>, List<Entry<K, Integer>>> {

  private final int minValue;
  private final Comparator<? super Entry<K, Integer>> comparator;

  public MinIntegerValueSortCollator(final int minValue,
      final Comparator<Entry<K, Integer>> comparator) {
    this.minValue = minValue;
    this.comparator = Objects.requireNonNull(comparator);
  }

  @Override
  public List<Entry<K, Integer>> collate(final Iterable<Entry<K, Integer>> entries) {
    final List<Entry<K, Integer>> sortedEntries = new LinkedList<>();

    entries.forEach(e -> {
      if (e.getValue() >= minValue) {
        sortedEntries.add(e);
      }
    });
    sortedEntries.sort(comparator);

    return sortedEntries;
  }
}
