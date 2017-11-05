package ar.edu.itba.pod.census.mapper;

import ar.edu.itba.pod.census.model.Container;
import ar.edu.itba.pod.census.model.Province;
import ar.edu.itba.pod.census.model.Region;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import static ar.edu.itba.pod.census.model.Container.EmploymentStatus.EMPLOYED;
import static ar.edu.itba.pod.census.model.Container.EmploymentStatus.UNEMPLOYED;

public class RegionOccupationMapper implements Mapper<Integer, Container, Region, Integer> {

  @Override
  public void map(final Integer key, final Container container, final Context<Region, Integer> context) {
    final Container.EmploymentStatus employmentStatus = Container.EmploymentStatus.valueOf(container.getEmploymentStatusId());
    final Region region = Province.fromString(container.getProvince()).getRegion();
    if (employmentStatus == EMPLOYED) {
      context.emit(region, 0);
    } else if (employmentStatus == UNEMPLOYED) {
      context.emit(region, 1);
    }
  }
}
