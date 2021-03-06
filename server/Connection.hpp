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
#pragma once
#include <vector>
#include <boost/asio.hpp>
#include <memory>

#include <common/Protocol.hpp>

#include <telldb/TellDB.hpp>

#ifdef USE_KUDU
#include <kudu/client/client.h>
#endif

#include "CreatePopulate.hpp"

namespace tpch {

template <class T>
class CommandImpl;

template <class ClientType, class FiberType>  // <TellClient, TellFiber> or <KuduClient, KuduFiber>
class Connection {
    boost::asio::ip::tcp::socket mSocket;
    std::unique_ptr<CommandImpl<ClientType>> mImpl;
public:
    Connection(boost::asio::io_service& service, ClientType& client, DBGenerator<ClientType, FiberType>& generator, int partitions);
    ~Connection();
    decltype(mSocket)& socket() { return mSocket; }
    void run();
public:
    static ClientType getClient(std::string &storage, std::string &commitMananger, size_t networkThreads);
};

} // namespace tpch

