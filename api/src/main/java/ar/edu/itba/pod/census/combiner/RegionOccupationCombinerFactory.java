package ar.edu.itba.pod.census.combiner;

import ar.edu.itba.pod.census.model.Region;
import ar.edu.itba.pod.census.model.RegionOccupationCombinerWrapper;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class RegionOccupationCombinerFactory implements CombinerFactory<Region, Integer, RegionOccupationCombinerWrapper> {

  @Override
  public Combiner<Integer, RegionOccupationCombinerWrapper> newCombiner(final Region key) {
    return new RegionOccupationCombiner();
  }

  private static class RegionOccupationCombiner extends Combiner<Integer, RegionOccupationCombinerWrapper> {
    private int unemployed;
    private int total;

    @Override
    public void reset() {
      unemployed = 0;
      total = 0;
    }

    @Override
    public void combine(final Integer unemployed) {
      this.unemployed += unemployed; // It will only add one if the person is unemployed (because of the mapping)
      total++; // Always count one for new records
    }

    @Override
    public RegionOccupationCombinerWrapper finalizeChunk() {
      return new RegionOccupationCombinerWrapper(unemployed, total);
    }
  }
}
