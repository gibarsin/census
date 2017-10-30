package ar.edu.itba.pod.census.reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SharedDepartmentCountReducerFactory implements
    ReducerFactory<String, String, Integer> {

  @Override
  public Reducer<String, Integer> newReducer(final String key) {
    return new SharedDepartmentCountReducer();
  }

  private class SharedDepartmentCountReducer extends Reducer<String, Integer> {

    private final Map<String, Boolean> map;

    private SharedDepartmentCountReducer() {
      this.map = Collections.synchronizedMap(new HashMap<>());
    }

    @Override
    public void reduce(final String department) {
      map.compute(department, (k, v) -> v == null ? Boolean.FALSE : Boolean.TRUE);
    }

    @Override
    public Integer finalizeReduce() {
      return Math
          .toIntExact(map.entrySet().stream().filter(e -> e.getValue() == Boolean.TRUE).count());
    }
  }
}
