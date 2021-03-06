package ar.edu.itba.pod.census.collator;

import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

public class LimitedSortCollator<K, V> extends SortCollator<K, V> {

  private final int limit;

  public LimitedSortCollator(final int limit, final Comparator<Entry<K, V>> comparator) {
    super(comparator);
    this.limit = limit;
  }

  @Override
  public List<Entry<K, V>> collate(final Iterable<Entry<K, V>> entries) {
    final List<Entry<K, V>> sortedCollatedList = super.collate(entries);
    final int realLimit = sortedCollatedList.size();
    final int maxPossibleLimit = realLimit < limit ? realLimit : limit;
    return super.collate(entries).subList(0, maxPossibleLimit);
  }
}
