package ar.edu.itba.pod.census.client.query;

public interface IQuery {
    /**
     * Run the query represented by the class that implements this interface
     */
    void run() throws QueryFailedException;
}
