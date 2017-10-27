package ar.edu.itba.pod.census.reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class DepartmentPopulationReducerFactory implements
    ReducerFactory<String, Integer, Integer> {

  @Override
  public Reducer<Integer, Integer> newReducer(final String key) {
    return new DepartmentPopulationReducer();
  }

  private class DepartmentPopulationReducer extends Reducer<Integer, Integer> {

    private volatile int sum = 0;

    @Override
    public void reduce(final Integer value) {
      sum += value;
    }

    @Override
    public Integer finalizeReduce() {
      return sum;
    }
  }
}
