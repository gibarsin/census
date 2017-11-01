package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.exception.QueryFailedException;

public interface IQuery {
    /**
     * Run the query represented by the class that implements this interface
     */
    void run() throws QueryFailedException;
}
