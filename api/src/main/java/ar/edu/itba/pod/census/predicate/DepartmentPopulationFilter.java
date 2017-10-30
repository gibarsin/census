package ar.edu.itba.pod.census.predicate;

import ar.edu.itba.pod.census.model.Citizen;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.mapreduce.KeyPredicate;
import java.util.Map;

public class DepartmentPopulationFilter implements KeyPredicate<Long>, HazelcastInstanceAware {

  private transient HazelcastInstance hazelcastInstance;
  private final String mapName;
  private final String province;

  public DepartmentPopulationFilter(final String mapName, final String province) {
    this.mapName = mapName;
    this.province = province;
  }

  @Override
  public boolean evaluate(final Long key) {
    final Map<Long, Citizen> map = hazelcastInstance.getMap(mapName);
    final Citizen citizen = map.get(key);

    return citizen.getProvince().equalsIgnoreCase(province);
  }

  @Override
  public void setHazelcastInstance(final HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }
}
