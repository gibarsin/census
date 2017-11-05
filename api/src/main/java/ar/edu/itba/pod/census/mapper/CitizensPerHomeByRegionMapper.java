package ar.edu.itba.pod.census.mapper;

import ar.edu.itba.pod.census.model.Container;
import ar.edu.itba.pod.census.model.Province;
import ar.edu.itba.pod.census.model.Region;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class CitizensPerHomeByRegionMapper implements Mapper<Integer, Container, Region, Integer> {
  @Override
  public void map(final Integer key, final Container container, final Context<Region, Integer> context) {
    context.emit(Province.fromString(container.getProvince()).getRegion(), container.getHomeId());
  }
}
