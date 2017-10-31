package ar.edu.itba.pod.census.mapper;

import ar.edu.itba.pod.census.model.Citizen;
import ar.edu.itba.pod.census.model.Province;
import ar.edu.itba.pod.census.model.Region;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class HomeRegionMapper implements Mapper<String, Citizen, Region, Integer> {

  @Override
  public void map(final String key, final Citizen citizen, final Context<Region, Integer> context) {
    context
        .emit(Region.fromProvince(Province.fromString(citizen.getProvince())), citizen.getHomeId());
  }
}
