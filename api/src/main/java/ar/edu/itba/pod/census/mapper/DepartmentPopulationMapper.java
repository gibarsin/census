package ar.edu.itba.pod.census.mapper;

import ar.edu.itba.pod.census.model.Citizen;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class DepartmentPopulationMapper implements Mapper<Long, Citizen, String, Integer> {

  @Override
  public void map(final Long aLong, final Citizen citizen, final Context<String, Integer> context) {
    context.emit(citizen.getDepartmentName(), 1);
  }
}
