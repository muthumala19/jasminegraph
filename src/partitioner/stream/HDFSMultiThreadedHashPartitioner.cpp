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

#include "HDFSMultiThreadedHashPartitioner.h"
#include "../../server/JasmineGraphServer.h"
#include <nlohmann/json.hpp>

Logger hash_partitioner_logger;

int PARTITION_FILE_EDGE_COUNT_THRESHOLD = 1000000;
int BATCH_SIZE = 1000;

HDFSMultiThreadedHashPartitioner::HDFSMultiThreadedHashPartitioner(int numberOfPartitions, int graphID,
    std::string masterIp, bool isDirected)
        : numberOfPartitions(numberOfPartitions), graphId(graphID),
          partitionLocks(numberOfPartitions), vertexCount(0), edgeCount(0),
          localEdgeArrays(numberOfPartitions), edgeCutsArrays(numberOfPartitions),
          localEdgeMutexes(numberOfPartitions), edgeAvailableCV(numberOfPartitions),
          edgeReady(numberOfPartitions, false), edgeCutsMutexes(numberOfPartitions),
          edgeCutsAvailableCV(numberOfPartitions), edgeCutsReady(numberOfPartitions, false),
          terminateConsumers(false), masterIp(masterIp), partitionMutexArray(numberOfPartitions),
          isDirected(isDirected) {
    this->outputFilePath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.hdfs.tempfolder")
            + "/" + std::to_string(this->graphId);
    Utils::createDirectory(this->outputFilePath);

    JasmineGraphServer *server = JasmineGraphServer::getInstance();
    std::vector<JasmineGraphServer::worker> workers = server->workers(numberOfPartitions);

    // Start consumer threads and store them
    for (int i = 0; i < numberOfPartitions; i++) {
        this->partitions.push_back(Partition(i, numberOfPartitions));
        localEdgeThreads.emplace_back(&HDFSMultiThreadedHashPartitioner::consumeLocalEdges, this, i, workers[i]);
        edgeCutThreads.emplace_back(&HDFSMultiThreadedHashPartitioner::consumeEdgeCuts, this, i, workers[i]);
        Utils::assignPartitionToWorker(graphId, i, workers.at(i).hostname, workers.at(i).port);
    }
}

HDFSMultiThreadedHashPartitioner::~HDFSMultiThreadedHashPartitioner() {
    stopConsumerThreads();
    while (!fileTransferQueue.empty()) {
        fileTransferQueue.front().wait();
        fileTransferQueue.pop();
    }
}

void HDFSMultiThreadedHashPartitioner::addLocalEdge(const std::string &edge, int index) {
    if (index < numberOfPartitions) {
        std::lock_guard<std::mutex> lock(localEdgeMutexes[index]);
        localEdgeArrays[index].push_back(edge);
        edgeReady[index] = true;
        edgeAvailableCV[index].notify_one();
    } else {
        hash_partitioner_logger.error("Invalid partition index : "
        + std::to_string(index) + " in addLocalEdge. Total number of partitions : "
        + std::to_string(numberOfPartitions));
    }
}

void HDFSMultiThreadedHashPartitioner::addEdgeCut(const std::string &edge, int index, int foreignPartitionIndex, bool isDuplicateEdge) {
    if (index < numberOfPartitions) {
        std::lock_guard<std::mutex> lock(edgeCutsMutexes[index]);
        edgeCutsArrays[index].push_back({edge, foreignPartitionIndex, isDuplicateEdge});
        edgeCutsReady[index] = true;
        edgeCutsAvailableCV[index].notify_one();
    } else {
        hash_partitioner_logger.error("Invalid partition index : "
        + std::to_string(index) + " in addEdgeCut. Total number of partitions : "
        + std::to_string(numberOfPartitions));
    }
}

void HDFSMultiThreadedHashPartitioner::stopConsumerThreads() {
    terminateConsumers = true;
    for (auto& cv : edgeAvailableCV) cv.notify_all();
    for (auto& cv : edgeCutsAvailableCV) cv.notify_all();

    for (auto& thread : localEdgeThreads)
        if (thread.joinable()) thread.join();

    for (auto& thread : edgeCutThreads)
        if (thread.joinable()) thread.join();
}

void HDFSMultiThreadedHashPartitioner::consumeLocalEdges(int partitionIndex, JasmineGraphServer::worker worker) {
    std::ofstream partitionFile;
    int edgeCount = 0, fileIndex = 0;
    std::string filePath;
    std::stringstream edgeBuffer;

    auto openNewFile = [&]() {
        filePath = outputFilePath + "/" + std::to_string(graphId) + "_" + std::to_string(partitionIndex) + "_localstore_" + std::to_string(fileIndex);
        partitionFile.open(filePath);
        if (!partitionFile.is_open())
            hash_partitioner_logger.error("Cannot open local file for partition " + std::to_string(partitionIndex));
    };

    openNewFile();

    while (true) {
        std::unique_lock<std::mutex> lock(localEdgeMutexes[partitionIndex]);
        edgeAvailableCV[partitionIndex].wait(lock, [this, partitionIndex] {
            return edgeReady[partitionIndex] || terminateConsumers;
        });

        if (terminateConsumers) break;

        while (!localEdgeArrays[partitionIndex].empty()) {
            std::string edge = localEdgeArrays[partitionIndex].back();
            localEdgeArrays[partitionIndex].pop_back();
            edgeBuffer << edge << "\n";
            edgeCount++;

            auto jsonEdge = json::parse(edge);
            std::string src = jsonEdge["source"]["id"].get<std::string>(), dst = jsonEdge["destination"]["id"].get<std::string>();
            {
                std::lock_guard<std::mutex> partitionLock(partitionLocks[partitionIndex]);
                partitions[partitionIndex].addEdge({src, dst}, isDirected);
            }

            if (edgeCount % BATCH_SIZE == 0) {
                partitionFile << edgeBuffer.str();
                edgeBuffer.str("");
                edgeBuffer.clear();
            }

            if (edgeCount >= PARTITION_FILE_EDGE_COUNT_THRESHOLD) {
                partitionFile << edgeBuffer.str();
                edgeBuffer.str("");
                edgeBuffer.clear();

                partitionFile.close();
                fileTransferQueue.push(std::async(std::launch::async, [this, filePath, worker, partitionIndex]() {
                    this->asyncSendFileToWorker(partitionIndex, filePath, worker, JasmineGraphInstanceProtocol::HDFS_LOCAL_STREAM_START);
                }));

                fileIndex++;
                edgeCount = 0;
                openNewFile();
            }
        }

        edgeReady[partitionIndex] = false;
    }

    if (partitionFile.is_open()) {
        partitionFile << edgeBuffer.str();
        edgeBuffer.str("");
        edgeBuffer.clear();
        partitionFile.close();
        fileTransferQueue.push(std::async(std::launch::async, [this, filePath, worker, partitionIndex]() {
            this->asyncSendFileToWorker(partitionIndex, filePath, worker, JasmineGraphInstanceProtocol::HDFS_LOCAL_STREAM_START);
        }));
    }
}


void HDFSMultiThreadedHashPartitioner::consumeEdgeCuts(int partitionIndex, JasmineGraphServer::worker worker) {
    std::ofstream edgeCutsFile;
    int edgeCount = 0, fileIndex = 0;
    std::string filePath;
    std::stringstream edgeBuffer;

    auto openNewFile = [&]() {
        filePath = outputFilePath + "/" + std::to_string(graphId) + "_" + std::to_string(partitionIndex) + "_centralstore_" + std::to_string(fileIndex);
        edgeCutsFile.open(filePath);
        if (!edgeCutsFile.is_open()) {
            hash_partitioner_logger.error("Cannot open central file for partition " + std::to_string(partitionIndex));
        }
    };

    openNewFile();

    while (true) {
        std::unique_lock<std::mutex> lock(edgeCutsMutexes[partitionIndex]);
        edgeCutsAvailableCV[partitionIndex].wait(lock, [this, partitionIndex] {
            return edgeCutsReady[partitionIndex] || terminateConsumers;
        });

        if (terminateConsumers) break;

        while (!edgeCutsArrays[partitionIndex].empty()) {
            EdgeInfo edgeInfo = edgeCutsArrays[partitionIndex].back();
            edgeCutsArrays[partitionIndex].pop_back();
            edgeBuffer << edgeInfo.edge << "\n";
            edgeCount++;

            auto jsonEdge = json::parse(edgeInfo.edge);
            std::string src = jsonEdge["source"]["id"].get<std::string>(), dst = jsonEdge["destination"]["id"].get<std::string>();

            std::lock_guard<std::mutex> partitionLock(partitionLocks[partitionIndex]);
            if (!edgeInfo.isDuplicate) {
                partitions[partitionIndex].addToEdgeCuts(src, dst, edgeInfo.foreignPartitionIndex);
            } else {
                partitions[partitionIndex].addToEdgeCuts(dst, src, edgeInfo.foreignPartitionIndex);
            }

            if (edgeCount % BATCH_SIZE == 0) {
                edgeCutsFile << edgeBuffer.str();
                edgeBuffer.str("");
                edgeBuffer.clear();
            }

            if (edgeCount >= PARTITION_FILE_EDGE_COUNT_THRESHOLD) {
                edgeCutsFile << edgeBuffer.str();
                edgeBuffer.str("");
                edgeBuffer.clear();

                edgeCutsFile.close();
                fileTransferQueue.push(std::async(std::launch::async, [this, filePath, worker, partitionIndex]() {
                    this->asyncSendFileToWorker(partitionIndex, filePath, worker, JasmineGraphInstanceProtocol::HDFS_CENTRAL_STREAM_START);
                }));
                fileIndex++;
                edgeCount = 0;
                openNewFile();
            }
        }

        edgeCutsReady[partitionIndex] = false;
    }

    if (edgeCutsFile.is_open()) {
        edgeCutsFile << edgeBuffer.str();
        edgeBuffer.str("");
        edgeBuffer.clear();
        edgeCutsFile.close();
        fileTransferQueue.push(std::async(std::launch::async, [this, filePath, worker, partitionIndex]() {
            this->asyncSendFileToWorker(partitionIndex, filePath, worker, JasmineGraphInstanceProtocol::HDFS_CENTRAL_STREAM_START);
        }));
    }
}

void HDFSMultiThreadedHashPartitioner::asyncSendFileToWorker(int partitionIndex,
                                                              const std::string& path,
                                                              const JasmineGraphServer::worker& worker,
                                                              const std::string& streamType) {
    std::lock_guard<std::mutex> lock(partitionMutexArray[partitionIndex]);
    Utils::sendFileChunkToWorker(worker.hostname, worker.port, worker.dataPort, path, masterIp, streamType);
}


void HDFSMultiThreadedHashPartitioner::updatePartitionTable() {
    SQLiteDBInterface sqlite;
    sqlite.init();
    for (int i = 0; i < numberOfPartitions; i++) {
        std::string sql = "INSERT INTO partition (idpartition,graph_idgraph,vertexcount,central_vertexcount,"
              "edgecount,central_edgecount) VALUES('" +
              std::to_string(i) + "','" +
              std::to_string(graphId) + "','" +
              std::to_string(partitions[i].getLocalVertexCount()) + "','" +
              std::to_string(partitions[i].getCentralVertexCount(i)) + "','" +
              std::to_string(partitions[i].getEdgesCount(isDirected)) + "','" +
              std::to_string(partitions[i].edgeCutsCount()) + "')";
        sqlite.runUpdate(sql);
    }
    sqlite.finalize();
}

long HDFSMultiThreadedHashPartitioner::getVertexCount() {
    int totalVertices = 0;
    for (auto & partition : this->partitions) {
        totalVertices += partition.getVertextCountQuick();
    }
    return totalVertices;
}

long HDFSMultiThreadedHashPartitioner::getEdgeCount() {
    int totalEdges = 0;
    int edgeCuts = 0;
    for (auto & partition : this->partitions) {
        totalEdges += partition.getEdgesCount(isDirected);
        edgeCuts += partition.edgeCutsCount();
    }
    return  totalEdges + edgeCuts / 2;
}

void HDFSMultiThreadedHashPartitioner::startPeriodicStatsUpdater(std::atomic<bool> &runningFlag, int intervalSeconds, SQLiteDBInterface *sqlite) {
    std::thread([this, &runningFlag, intervalSeconds, sqlite ]() {
        while (runningFlag) {
            std::time_t time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            std::this_thread::sleep_for(std::chrono::seconds(intervalSeconds));
            long currentVertices = getVertexCount();
            long currentEdges = getEdgeCount();

            std::time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            std::string timeStr = std::ctime(&now);
            timeStr.pop_back(); // remove newline

            std::string logEntry = "Time: " + timeStr + " | Vertices: " + std::to_string(currentVertices) +
                                   " | Edges: " + std::to_string(currentEdges);

            // Log or insert into DB
            if (sqlite) {
                std::string insertQuery = "UPDATE graph SET vertexcount = '" + std::to_string(currentVertices) +
                           "', centralpartitioncount = '" + std::to_string(this->numberOfPartitions) +
                           "', edgecount = '" + std::to_string(currentEdges) +
                           "', report_time = '" + ctime(&time) +
                           "' WHERE idgraph = '" + std::to_string(this->graphId) + "'";
                sqlite->runInsert(insertQuery);
            }

            hash_partitioner_logger.info("Periodic DB update: vertices = " + std::to_string(currentVertices) +
                                        ", edges = " + std::to_string(currentEdges));
        }
    }).detach();
}