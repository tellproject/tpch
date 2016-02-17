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
#include <crossbow/allocator.hpp>
#include <crossbow/program_options.hpp>
#include <crossbow/logger.hpp>
#include <telldb/TellDB.hpp>

#include <boost/asio.hpp>
#include <string>
#include <iostream>

#include "Connection.hpp"

using namespace crossbow::program_options;
using namespace boost::asio;

template<class ClientType, class FiberType>
void accept(boost::asio::io_service &service,
        boost::asio::ip::tcp::acceptor &a,
        ClientType& client,
        tpch::DBGenerator<ClientType, FiberType>& generator,
        int partitions) {
    auto conn = new tpch::Connection<ClientType, FiberType>(service, client, generator, partitions);
    a.async_accept(conn->socket(), [conn, &service, &a, &client, &generator, partitions](const boost::system::error_code &err) {
        if (err) {
            delete conn;
            LOG_ERROR(err.message());
            return;
        }
        conn->run();
        accept(service, a, client, generator, partitions);
    });
}

int main(int argc, const char** argv) {
    bool help = false;
    std::string host;
    std::string port("8713");
    std::string logLevel("DEBUG");
    std::string commitManager;
    std::string storageNodes;
    size_t numThreads = 4;
    int partitions = -1;
    bool useKudu = false;
    auto opts = create_options("tpch_server",
            value<'h'>("help", &help, tag::description{"print help"}),
            value<'H'>("host", &host, tag::description{"Host to bind to"}),
            value<'p'>("port", &port, tag::description{"Port to bind to"}),
            value<'P'>("partitions", &partitions, tag::description{"Number of partitions per table"}),
            value<'l'>("log-level", &logLevel, tag::description{"The log level"}),
            value<'c'>("commit-manager", &commitManager, tag::description{"Address to the commit manager"}),
            value<'s'>("storage-nodes", &storageNodes, tag::description{"Semicolon-separated list of storage node addresses"}),
            value<'k'>("kudu", &useKudu, tag::description{"use kudu instead of TellStore"}),
            value<-1>("network-threads", &numThreads, tag::ignore_short<true>{})
            );
    try {
        parse(opts, argc, argv);
    } catch (argument_not_found& e) {
        std::cerr << e.what() << std::endl << std::endl;
        print_help(std::cout, opts);
        return 1;
    }
    if (useKudu && partitions == -1) {
        std::cerr << "Number of partitions needs to be set" << std::endl;
        return 1;
    }
    if (help) {
        print_help(std::cout, opts);
        return 0;
    }

    crossbow::allocator::init();

    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);
    try {
        io_service service;
        boost::asio::io_service::work work(service);
        ip::tcp::acceptor a(service);
        boost::asio::ip::tcp::acceptor::reuse_address option(true);
        ip::tcp::resolver resolver(service);
        ip::tcp::resolver::iterator iter;
        if (host == "") {
            iter = resolver.resolve(ip::tcp::resolver::query(port));
        } else {
            iter = resolver.resolve(ip::tcp::resolver::query(host, port));
        }
        ip::tcp::resolver::iterator end;
        for (; iter != end; ++iter) {
            boost::system::error_code err;
            auto endpoint = iter->endpoint();
            auto protocol = iter->endpoint().protocol();
            a.open(protocol);
            a.set_option(option);
            a.bind(endpoint, err);
            if (err) {
                a.close();
                LOG_WARN("Bind attempt failed " + err.message());
                continue;
            }
            break;
        }
        if (!a.is_open()) {
            LOG_ERROR("Could not bind");
            return 1;
        }
        a.listen();

        // we do not need to delete this object, it will delete itself
        if (useKudu) {
#ifdef USE_KUDU
            tpch::DBGenerator<tpch::KuduClient, tpch::KuduFiber> generator;
            auto client = tpch::Connection<tpch::KuduClient, tpch::KuduFiber>::getClient(
                    storageNodes, commitManager, numThreads);
            accept(service, a, client, generator, partitions);
            std::vector<std::thread> threads;
            for (unsigned i = 0; i < numThreads; ++i) {
                threads.emplace_back([&service](){
                        service.run();
                });
            }
            for (auto& t : threads) {
                t.join();
            }
#else
                std::cerr << "Code was not compiled for Kudu\n";
                return 1;
#endif
        } else {
            tpch::DBGenerator<tpch::TellClient, tpch::TellFiber> generator;
            auto client = tpch::Connection<tpch::TellClient, tpch::TellFiber>::getClient(
                    storageNodes, commitManager, numThreads);
            accept(service, a, client, generator, partitions);
            service.run();
        }
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}
