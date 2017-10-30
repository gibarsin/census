package ar.edu.itba.pod.census.predicate;

import ar.edu.itba.pod.census.model.Citizen;
import ar.edu.itba.pod.census.model.Citizen.EMPLOYMENT_STATUS;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.mapreduce.KeyPredicate;
import java.util.Map;

public class RegionOccupationFilter implements KeyPredicate<Long>,HazelcastInstanceAware {

  private transient HazelcastInstance hazelcastInstance;
  private final String mapName;

  public RegionOccupationFilter(final String mapName) {
    this.mapName = mapName;
  }

  @Override
  public boolean evaluate(final Long key) {
    final Map<Long, Citizen> map = hazelcastInstance.getMap(mapName);
    final Citizen citizen = map.get(key);

    return citizen.getEmploymentStatus() == EMPLOYMENT_STATUS.EMPLOYED
        || citizen.getEmploymentStatus() == EMPLOYMENT_STATUS.UNEMPLOYED;
  }

  @Override
  public void setHazelcastInstance(final HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }
}
