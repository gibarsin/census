package ar.edu.itba.pod.census.mapper;

import ar.edu.itba.pod.census.model.Home;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class HomesInRegionMapper implements Mapper<String, Home, String, Integer> {

  @Override
  public void map(final String key, final Home home, final Context<String, Integer> context) {
    context.emit(home.getRegion(), 1);
  }
}
