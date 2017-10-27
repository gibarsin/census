package ar.edu.itba.pod.census.mapper;

import ar.edu.itba.pod.census.model.Citizen;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class RegionPopulationMapper implements Mapper<Long, Citizen, String, Long> {

  @Override
  public void map(final Long key, final Citizen citizen, final Context<String, Long> context) {
    context.emit(citizen.getRegion(), 1L);
  }
}
