# tpch
This implements parts of the TPC-H benchmark for TellStore and Kudu. You can use the tpch_client to populate the storage with some data generated by [dbgen](http://www.tpc.org/TPC_Documents_Current_Versions/download_programs/tools-download-request.asp?BM=TPC-H), or you can use it to connect to a tpch_server and let it execute refresh functions 1 and 2 repeatedly. This code does not execute any TPC-H benchmark queries. The idea is to execute these queries on [Spark](https://github.com/tellproject/SparkBenchmarks) or [Presto](https://github.com/tellproject/presto). For Spark, either use [https://github.com/tellproject/TellSpark](https://github.com/tellproject/telljava) as an interface to TellStore or [SparkOnKudu](https://github.com/renato2099/SparkOnKudu) as an interface to Kudu.

## Building
First, build [Tell](https://github.com/tellproject/tell). Then, clone then newest version of the TPC-H benchmark and build Tell again. This will also build the TPC-H benchmark:

```bash
cd <tell-main-directory>
cd watch
git clone https://github.com/tellproject/tpch.git
cd <tell-build>
make
```

### Building for Kudu
If you want to run the TPC-H Benchmark not only on Tell, but also on [Kudu](http://getkudu.io), first make sure that you configure the kuduClient_DIR correctly in the CMakeLists.txt. Once it is set correctly, you have to call cmake again in the tell build directory and set the additional flag:

```bash
-DUSE_KUDU=ON
```

## Running
The simplest way to run the benchmark is to use the [Python Helper Scripts](https://github.com/tellproject/helper_scripts). They will not only help you to start TellStore, but also one or several TPC-H servers and clients.

### Server
Of course, you can also run the server from the commandline. You can execute the following line to print out to the console the commandline options you need to start the server (-k will start it with Kudu instead of Tell):

```bash
watch/tpch/tpch_kudu -h
```

### Client
The TPC-H client uses a TCP connection to send RF1, resp. RF2 requests to a TPC-H server. It writes a log file in CSV format where it logs every transaction that was executed with transaction type, start time, end time (both in millisecs and relative to the beginning of the experiment) as well as whether the transaction was successfully commited or not. This log file can then be grepped in order to compute some other statistics. The client can connect to server(s) regardless of the used storage backend. The client can also be used to populate the store with data generated yy [dbgen](http://www.tpc.org/TPC_Documents_Current_Versions/download_programs/tools-download-request.asp?BM=TPC-H). In that case, it does not connect to the TPC-H server, but to the storage backend directly which is why it needs some additional commandline options. You can find out about the commandline options for the client by typing:

```bash
watch/tpch/tpch_client -h
```
