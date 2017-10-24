package ar.edu.itba.pod.census.client;

import ar.edu.itba.pod.census.client.args.ClientArgs;
import com.beust.jcommander.JCommander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Client {

    private final static Logger logger = LoggerFactory.getLogger(Client.class);

    private static final ClientArgs clientArgs = ClientArgs.getInstance();

    public static void main(final String[] args) {
        parseClientArguments(args);
    }

    private static void parseClientArguments(final String[] args) {
        JCommander.newBuilder().addObject(clientArgs).build().parse(args);
    }
}
