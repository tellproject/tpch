/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#include <crossbow/program_options.hpp>
#include <crossbow/logger.hpp>
#include <crossbow/allocator.hpp>

#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <string>
#include <iostream>
#include <cassert>
#include <fstream>

#include <thread>

#include <common/Util.hpp>

#include "Client.hpp"

using namespace crossbow::program_options;
using namespace boost::asio;
using err_code = boost::system::error_code;

int main(int argc, const char** argv) {
    bool help = false;
    bool populate = false;
    crossbow::string host;
    std::string port("8713");
    std::string logLevel("DEBUG");
    std::string outFile("out.csv");
    size_t numClients = 1;
    std::string baseDir = "/mnt/SG/braunl-tpch-data/all/1";
    uint batchSize = 1500;
    unsigned time = 5*60;
    bool exit = false;
    auto opts = create_options("tpch_client",
            value<'h'>("help", &help, tag::description{"print help"})
            , value<'H'>("host", &host, tag::description{"Comma-separated list of hosts"})
            , value<'l'>("log-level", &logLevel, tag::description{"The log level"})
            , value<'c'>("num-clients", &numClients, tag::description{"Number of Clients to run per host"})
            , value<'P'>("populate", &populate, tag::description{"Populate the database"})
            , value<'t'>("time", &time, tag::description{"Duration of the benchmark in seconds"})
            , value<'o'>("out", &outFile, tag::description{"Path to the output file"})
            , value<'d'>("base-dir", &baseDir, tag::description{"Base directory to the generated tbl/upd/del files, assumes for population that this base-dir exists on server as well."})
            , value<'b'>("batch-size", &batchSize, tag::description{"Batch Size for RF1/RF2 to be logged."})
            , value<-1>("exit", &exit, tag::description{"Quit server"})
            );
    try {
        parse(opts, argc, argv);
    } catch (argument_not_found& e) {
        std::cerr << e.what() << std::endl << std::endl;
        print_help(std::cout, opts);
        return 1;
    }
    if (help) {
        print_help(std::cout, opts);
        return 0;
    }

    crossbow::allocator::init();

//            tpch::DBGenerator<tpch::KuduClient, tpch::KuduFiber> generator;
//            generator.createSchemaAndPopulate(storage, commitManager, baseDir);
//            return 0;
//        } else {
//            tpch::DBGenerator<tpch::TellClient, tpch::TellFiber> generator;
//            generator.createSchemaAndPopulate(storage, commitManager, baseDir);
//            return 0;
//        }
    if (host.empty()) {
        std::cerr << "No host\n";
        return 1;
    }
    auto startTime = tpch::Clock::now();
    auto endTime = startTime + std::chrono::seconds(time);
    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);
    try {
        auto hosts = tpch::split(host.c_str(), ',');
        io_service service;

        // do client creation multi-threaded as it may take a while
        LOG_DEBUG("Start client creation.");
        auto sumClients = hosts.size() * numClients;
        std::vector<tpch::Client> clients;
        clients.reserve(sumClients);
        std::vector<std::thread> threads;
        for (decltype(sumClients) i = 0; i < sumClients; ++i) {
            threads.emplace_back([&, i](){
                clients.emplace_back(service, endTime, batchSize, baseDir, uint(i), !populate);
            });
        }
        for (auto &thread: threads)
            thread.join();
        LOG_DEBUG("Client creation finished.");

        for (size_t i = 0; i < hosts.size(); ++i) {
            auto h = hosts[i];
            auto addr = tpch::split(h, ':');
            assert(addr.size() <= 2);
            auto p = addr.size() == 2 ? addr[1] : port;
            ip::tcp::resolver resolver(service);
            ip::tcp::resolver::iterator iter;
            if (host == "") {
                iter = resolver.resolve(ip::tcp::resolver::query(p));
            } else {
                iter = resolver.resolve(ip::tcp::resolver::query(addr[0], p));
            }
            for (unsigned j = 0; j < numClients; ++j) {
                LOG_INFO("Connected to client " + crossbow::to_string(i*numClients + j));
                boost::asio::connect(clients[i*numClients + j].socket(), iter);
            }
        }

        {
            auto t = std::time(nullptr);
            std::string dateString(20, '\0');
            auto len = std::strftime(&dateString.front(), 20, "%T", std::localtime(&t));
            dateString.resize(len);
            LOG_INFO("Starting client at %1%", dateString);
        }

        if (exit) {
            for (auto& client : clients) {
                auto& cmds = client.commands();
                cmds.execute<tpch::Command::EXIT>([](const err_code& ec){
                    if (ec) {
                        std::cerr << "ERROR: " << ec.message() << std::endl;
                    }
                });
                goto END;
            }
        }

        if (populate) {
            auto& cmds = clients[0].commands();
            double scalingFactor = tpch::getScalingFactor(baseDir);
            cmds.execute<tpch::Command::CREATE_SCHEMA>(
                    [&clients, &baseDir, &scalingFactor](const err_code& ec,
                        const std::tuple<bool, crossbow::string>& res){
                if (ec) {
                    LOG_ERROR(ec.message());
                    return;
                }
                if (!std::get<0>(res)) {
                    LOG_ERROR(std::get<1>(res));
                    return;
                }

                auto& cmds = clients[0].commands();
                cmds.execute<tpch::Command::POPULATE_DIM_TABLES>([&clients, &baseDir, &scalingFactor](const err_code& ec, const std::tuple<bool, crossbow::string>& res){
                    if (ec) {
                        LOG_ERROR(ec.message());
                        return;
                    }
                    if (!std::get<0>(res)) {
                        LOG_ERROR(std::get<1>(res));
                        return;
                    }

                    for (uint32_t i = 0; i < clients.size(); ++i) {
                        clients[i].populate(baseDir, i);
                    }
                }, crossbow::string(baseDir.c_str(), baseDir.size()));
            }, scalingFactor);
        } else {
            for (auto& client : clients) {
                client.run();
            }
        }
END:
        service.run();
        LOG_INFO("Done, writing results");
        std::ofstream out(outFile.c_str());
        out << "start,end,transaction,success,error\n";
        for (const auto& client : clients) {
            const auto& queue = client.log();
            for (const auto& e : queue) {
                crossbow::string tName;
                switch (e.transaction) {
                case tpch::Command::CREATE_SCHEMA:
                    tName = "Create schema";
                    break;
                case tpch::Command::POPULATE_DIM_TABLES:
                    tName = "Populate dim tables";
                    break;
                case tpch::Command::POPULATE:
                    tName = "Populate remaining tables";
                    break;
                case tpch::Command::RF1:
                    tName = "RF1";
                    break;
                case tpch::Command::RF2:
                    tName = "RF2";
                    break;
                case tpch::Command::EXIT:
                    assert(false);
                    break;
                }
                out << std::chrono::duration_cast<std::chrono::milliseconds>(e.start - startTime).count() << ','
                    << std::chrono::duration_cast<std::chrono::milliseconds>(e.end - startTime).count() << ','
                    << tName << ','
                    << (e.success ? "true" : "false") << ','
                    << e.error << std::endl;
            }
        }
        std::cout << '\a';
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
    return 0;
}
