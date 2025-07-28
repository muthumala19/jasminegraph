/*
 * Copyright 2024 JasminGraph Team
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef JASMINEGRAPH_HASH_PARTITIONER_HEADER
#define JASMINEGRAPH_HASH_PARTITIONER_HEADER

#include "Partitioner.h"
#include "../../util/logger/Logger.h"
#include "../../util/Utils.h"
#include "../../server/JasmineGraphServer.h"
#include <string>
#include <vector>
#include <map>
#include <queue>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <future>
#include <atomic>

struct EdgeInfo
{
    std::string edge;
    int foreignPartitionIndex;
    bool isDuplicate;
};

class HDFSMultiThreadedHashPartitioner
{
public:
    HDFSMultiThreadedHashPartitioner(int numberOfPartitions, int graphID, std::string masterIp, bool isDirected);
    ~HDFSMultiThreadedHashPartitioner();

    void addLocalEdge(const std::string& edge, int index);
    void addEdgeCut(const std::string& edge, int index, int foreignPartitionIndex, bool isDuplicateEdge);

    void updatePartitionTable();
    long getVertexCount();
    long getEdgeCount();
    void startPeriodicStatsUpdater(std::atomic<bool>& runningFlag, int intervalSeconds, SQLiteDBInterface* sqlite);

private:
    void consumeLocalEdges(int partitionIndex, JasmineGraphServer::worker worker);
    void consumeEdgeCuts(int partitionIndex, JasmineGraphServer::worker worker);
    void asyncSendFileToWorker(int partitionIndex, const std::string& path,
                               const JasmineGraphServer::worker& worker, const std::string& streamType);
    void stopConsumerThreads();

    // Core config
    int numberOfPartitions;
    int graphId;
    std::string masterIp;
    std::string outputFilePath;
    bool isDirected;

    std::atomic<bool> terminateConsumers;

    // Partition management
    std::vector<Partition> partitions;
    std::vector<std::mutex> partitionLocks;
    std::vector<std::mutex> partitionMutexArray;

    // Local edges
    std::vector<std::vector<std::string>> localEdgeArrays;
    std::vector<std::mutex> localEdgeMutexes;
    std::vector<std::condition_variable> edgeAvailableCV;
    std::vector<bool> edgeReady;

    // Edge cuts
    std::vector<std::vector<EdgeInfo>> edgeCutsArrays;
    std::vector<std::mutex> edgeCutsMutexes;
    std::vector<std::condition_variable> edgeCutsAvailableCV;
    std::vector<bool> edgeCutsReady;

    // Consumers
    std::vector<std::thread> localEdgeThreads;
    std::vector<std::thread> edgeCutThreads;

    // Asynchronous file transfers
    std::queue<std::future<void>> fileTransferQueue;

    // Graph stats
    long vertexCount = 0;
    long edgeCount = 0;
};

#endif  // !JASMINEGRAPH_HASH_PARTITIONER_HEADER
