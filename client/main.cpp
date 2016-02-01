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

#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <string>
#include <iostream>
#include <cassert>
#include <fstream>

#include <common/Util.hpp>

#include "Client.hpp"

using namespace crossbow::program_options;
using namespace boost::asio;
using err_code = boost::system::error_code;

int main(int argc, const char** argv) {
    bool help = false;
    bool use_kudu = false;
    bool populate = false;
    crossbow::string host;
    std::string port("8713");
    std::string logLevel("DEBUG");
    std::string outFile("out.csv");
    size_t numClients = 1;
    unsigned time = 5*60;
    std::string storage = "localhost";  // used only for population
    std::string commitManager;  // used only for population
    std::string baseDir = "/mnt/local/tell/tpch_2_17_0/dbgen"; // used only for population
    bool exit = false;
    auto opts = create_options("tpch_client",
            value<'h'>("help", &help, tag::description{"print help"})
            , value<'H'>("host", &host, tag::description{"Comma-separated list of hosts"})
            , value<'l'>("log-level", &logLevel, tag::description{"The log level"})
            , value<'c'>("num-clients", &numClients, tag::description{"Number of Clients to run per host"})
            , value<'P'>("populate", &populate, tag::description{"Populate the database"})
            , value<'t'>("time", &time, tag::description{"Duration of the benchmark in seconds"})
            , value<'o'>("out", &outFile, tag::description{"Path to the output file"})
            , value<'s'>("storage", &storage, tag::description{"address(es) of the storage nodes (only necessary for population)"})
            , value<'x'>("commit-manager", &commitManager, tag::description{"address of the commit manager (only necessary for population)"})
            , value<'d'>("base-dir", &baseDir, tag::description{"Base directory to the generated tbl files (only necessary for population)"})
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
    if (host.empty()) {
        std::cerr << "No host\n";
        return 1;
    }

    //todo: continue here with merging CreateSchema.main into this main...

    auto startTime = tpch::Clock::now();
    auto endTime = startTime + std::chrono::seconds(time);
    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);
    try {
        auto hosts = tpch::split(host.c_str(), ',');
        io_service service;
        auto sumClients = hosts.size() * numClients;
        std::vector<tpch::Client> clients;
        clients.reserve(sumClients);
        for (decltype(sumClients) i = 0; i < sumClients; ++i) {
            clients.emplace_back(service, endTime);
        }
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
            //todo: population code
        } else {
            //todo: commands
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
}
