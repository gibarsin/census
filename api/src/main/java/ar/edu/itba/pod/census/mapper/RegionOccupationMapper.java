package ar.edu.itba.pod.census.mapper;

import ar.edu.itba.pod.census.model.Citizen;
import ar.edu.itba.pod.census.model.Citizen.EMPLOYMENT_STATUS;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class RegionOccupationMapper implements Mapper<Long, Citizen, String, Boolean> {

  @Override
  public void map(final Long id, Citizen citizen, Context<String, Boolean> context) {
    final Boolean isEmployed;

    if (citizen.getEmploymentStatus() == EMPLOYMENT_STATUS.EMPLOYED) {
      isEmployed = Boolean.TRUE;
    } else if (citizen.getEmploymentStatus() == EMPLOYMENT_STATUS.UNEMPLOYED) {
      isEmployed = Boolean.FALSE;
    } else {
      throw new IllegalArgumentException("Citizen should be employed or unemployed");
    }

    context.emit(citizen.getRegion(), isEmployed);
  }
}
