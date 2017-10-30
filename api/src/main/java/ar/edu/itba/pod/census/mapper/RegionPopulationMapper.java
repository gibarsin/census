package ar.edu.itba.pod.census.mapper;

import ar.edu.itba.pod.census.model.Citizen;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class RegionPopulationMapper implements Mapper<String, Citizen, String, Integer> {

  @Override
  public void map(final String key, final Citizen citizen, final Context<String, Integer> context) {
    context.emit(citizen.getRegion(), 1);
  }
}
