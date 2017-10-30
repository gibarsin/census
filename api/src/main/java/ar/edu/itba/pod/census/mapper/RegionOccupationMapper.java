package ar.edu.itba.pod.census.mapper;

import static ar.edu.itba.pod.census.model.Citizen.EMPLOYMENT_STATUS.UNEMPLOYED;

import ar.edu.itba.pod.census.model.Citizen;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class RegionOccupationMapper implements Mapper<Long, Citizen, String, Integer> {

  @Override
  public void map(final Long id, final Citizen citizen, final Context<String, Integer> context) {
    context.emit(citizen.getRegion(), citizen.getEmploymentStatus() == UNEMPLOYED ? 1 : 0);
  }
}
