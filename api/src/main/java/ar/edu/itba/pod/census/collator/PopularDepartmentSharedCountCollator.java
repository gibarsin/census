package ar.edu.itba.pod.census.collator;

import ar.edu.itba.pod.census.model.ProvincePair;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class PopularDepartmentSharedCountCollator
        implements Collator<Map.Entry<String, Set<ProvincePair>>, List<Map.Entry<String, Integer>>> {
  private final LimitedSortCollator<String, Integer> limitedSortCollator;

  public PopularDepartmentSharedCountCollator(final int requiredN, final Comparator<Map.Entry<String, Integer>> comparator) {
    limitedSortCollator = new LimitedSortCollator<String, Integer>(requiredN, comparator);
  }

  @Override
  public List<Map.Entry<String, Integer>> collate(final Iterable<Map.Entry<String, Set<ProvincePair>>> entries) {
    final Map<String, Integer> resultMap = new HashMap<>();

    for (final Map.Entry<String, Set<ProvincePair>> entry : entries) {
      final Set<ProvincePair> allProvincesOfADepartment = entry.getValue();
      for (final ProvincePair eachPairOfADepartment : allProvincesOfADepartment) {
        final int diff = eachPairOfADepartment.getFirst().compareToIgnoreCase(eachPairOfADepartment.getSecond());
        final String key;
        if (diff < 0) {
          key = eachPairOfADepartment.getFirst() + " + " + eachPairOfADepartment.getSecond();
        } else {
          key = eachPairOfADepartment.getSecond() + " + " + eachPairOfADepartment.getFirst();
        }
        resultMap.compute(key, (s, counter) -> counter == null ? 1 : counter + 1);
      }
    }

    return limitedSortCollator.collate(resultMap.entrySet());
  }
}
