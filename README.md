# Distributed System Data Storage

This distributed data storage system can accommodate as many servers and clients as needed. The servers coordinate with each other to replicate data in case one of them randomly crashes. A 'leader' server, known as the External Configuration Service (ECS), manages tasks such as adding new servers to the system or handling server crashes. The ECS monitors the health of all servers by continuously sending a 'heartbeat' signal to each one and ensuring they respond.However, if the ECS itself fails, we have implemented a leader election strategy that allows one of the remaining servers to assume the role of the ECS.

Additionally, we have implemented a 'consistent hashing' algorithm, which efficiently distributes the load among servers when clients modify or retrieve values from the database. This algorithm allows an easy way to re-distribute data that a server is responsible for when it crashes, and also replication of data between servers. More information about it can be found here: https://www.geeksforgeeks.org/consistent-hashing/.

Demo: https://www.youtube.com/watch?v=THoMuKXWlX0

If you want to use the system, follow the steps below. You will need Java installed on your machine.
### HOW TO USE THE SYSTEM?
1. Build the application using `ant`
2. Launch the ECS using `java -jar m4-ecs.jar`. By default, the ECS uses the address `localhost:5100`. Use `-h` for a full list of options when starting the ECS.
3. In different terminal windows, launch several KV servers using `java -jar m4-server.jar -p <port_number>`. `<port_number>` should be unique for each KV server. If you have changed address of the ECS, you will additionally need to use the `-b` tag to specify the address of the ECS. Use `-h` for a full list of options when starting the KV servers.
4. Launch a client instance using `java -jar m4-client.jar`.
5. [RECOMMENDED] It is strongly recommended that you disable all log messages in the client as they clutter the client interface. This can be done with the command `logLevel OFF`
6. Connect the client to the service using `connect localhost <port_number>` where `<port_number>` is the port number of any of the KV servers launched in step 3.
7. You can now start putting and getting keys and values in the KV service using `put <key> <value>` and `get <key>`
8. To inspect the storage of each server, check the root directory for files which look like `<address>.txt`. That text file contains the key-value pairs for which the server at `<address>` is the coordinator. In the files which look like `<replica_port>_localhost-<coordinator_port>.txt` are the values stored by replica server at `<replica_port>` for coordinator at `<coordinator_port>`
9. To test the ECS failure recovery, shut down the ECS using any method you like (the simplest would be `ctrl-c` in the ECS window)
10. At this point, one of the KV servers will take over the ECS responsibilities, typically this will be the server most recently added to the network. You can verify that all the KV pairs originally in it's storage file have been redistributed and replica's have been updated.
11. Finally, test adding a new server to the network by running `java -jar m4-server.jar -p <port_number> -b <ecs_address>` where `<ecs_address>` is the address of the server which took over as ECS. Typically it will look like `localhost:<port>`.
12. Again, you can verify that KV pairs have been redistributed appropriately.
