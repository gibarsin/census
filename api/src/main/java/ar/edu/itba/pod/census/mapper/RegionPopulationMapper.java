package ar.edu.itba.pod.census.mapper;

import ar.edu.itba.pod.census.model.Province;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class RegionPopulationMapper implements Mapper<String, String, String, Integer> {

  @Override
  public void map(final String key, final String province, final Context<String, Integer> context) {
    context.emit(Province.fromString(province).getRegion().toString(), 1);
  }
}
