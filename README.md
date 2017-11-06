# Census

TPE for POD (2017) @ ITBA

Java + Hazelcast Implementation of a computer cluster for distributed processing.

## Hazelcast configuration
`hazelcast.xml` is located at the project's root directory.
If you want to configure your network, you can update this file as needed, and when done, recompile all the project to include this new version with the compiled sources.

You can also update the `hazelcast.xml` file located at the server's unpacked directory, but be aware that any recompilation of the sources may result in this file being deleted.

**We strongly suggest updating the `hazelcast.xml` file from the project's root directory.**

## Usage

### Clean
To clean all related stuff (including maven resources), just run from the project's root folder

    $ ./clean-all

### Run
The run script is in charge of compiling the project sources (if they are not already compiled), unpacking them (if they are not already unpacked), moving them to a folder in `$HOME/census-server` or `$HOME/census-client` (depending on the param specified to the `run` command) and running the specified side (`server` or `client`, also based on the given parameter).

Please note that both `server` and `client` will run locally to the created folder in the `$HOME` directory, so all specified paths will be relative to that folder (take this into account when specifying `client`'s arguments).

**Please also note that both relative and absolute paths may be specified, but they should not contain shell or environmental variables.**

So, for example, instead of specifying as a `client` argument `-DinPath=$HOME/Desktop/census.csv`, you must specify `-DinPath=/home/<your_username>/Desktop/census.csv`.

**If you want to recompile and redistribute the code, just call `./clean-all` and then run the `server` or `client` depending on your needs.**

### Server
To run a server node, just execute from the project's root folder

    $ ./run server

For running a node server directly from the unpacked compiled sources directory (if unpacked with the run script, directory is `$HOME/census-server`), just run being inside there
  
    $ ./run-server.sh

### Client
To run a client, just execute from the project's root folder

    $ ./run client <program-args>
    
with `<program-args>` being the ones specified in the given assignment.

For running a client directly from the unpacked compiled sources directory (if unpacked with the `run` script, directory is `$HOME/census-client`), just run being inside there
  
    $ ./run-client.sh <program-args>

**Please note that for specifying multiple addresses or multi-word arguments, they have to be specified between double quotes**

So, for example, to run query 2 with addresses `10.1.34.73` and `10.1.34.120` you may call from the project's root directory

    $ ./run client -Daddresses="10.1.34.73;10.1.34.120" -DoutPath=output.txt -DinPath=/home/<your_username>/Desktop/census100.csv -DtimeOutPath=times.txt -Dquery=2 -Dn=2 -Dprov="Buenos Aires"

Note that client arguments may be specified in any order.
