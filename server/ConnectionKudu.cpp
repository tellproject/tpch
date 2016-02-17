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
#include "Connection.hpp"

#include <crossbow/logger.hpp>

#include "TransactionsKudu.hpp"
#include "KuduUtil.hpp"

using namespace boost::asio;

namespace tpch {

template<>
Connection<KuduClient, KuduFiber>::~Connection();

template<>
class CommandImpl<KuduClient> {
    Connection<KuduClient, KuduFiber> *mConnection;
    boost::asio::ip::tcp::socket& mSocket;
    server::Server<CommandImpl<KuduClient>> mServer;
    KuduClient &mClient;
    Session mSession;
    TransactionsKudu mTransactions;
    DBGenerator<KuduClient, KuduFiber> &mGenerator;
    const int mPartitions;

public:
    CommandImpl(Connection<KuduClient, KuduFiber> *connection,
                boost::asio::ip::tcp::socket& socket,
                KuduClient &client,
                DBGenerator<KuduClient, KuduFiber> &generator,
                int partitions)
        : mConnection(connection)
        , mSocket(socket)
        , mServer(*this, mSocket)
        , mClient(client)
        , mSession(client->NewSession())
        , mGenerator(generator)
        , mPartitions(partitions)
    {
        assertOk(mSession->SetFlushMode(kudu::client::KuduSession::MANUAL_FLUSH));
        mSession->SetTimeoutMillis(60000);
    }

    void run() {
        mServer.run();
    }

    void close() {
        delete mConnection;
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::CREATE_SCHEMA, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback callback) {
        bool success;
        crossbow::string msg;
        try {
            mGenerator.createSchema(mClient, args, mPartitions);
            success = true;
        } catch (std::exception& ex) {
            success = false;
            msg = ex.what();
        }
        callback(std::make_tuple(success, msg));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::POPULATE, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback callback) {
        bool success;
        crossbow::string msg;
        uint32_t partIndex = std::get<0>(args);
        const crossbow::string &baseDir = std::get<1>(args);
        std::string bd (baseDir.c_str(), baseDir.size());
        try {
            mGenerator.populate(mClient, bd, partIndex);
            success = true;
        } catch (std::exception& ex) {
            success = false;
            msg = ex.what();
        }
        callback(std::make_tuple(success, msg));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::EXIT, void>::type
    execute(const Callback callback) {
        mServer.quit();
        callback();
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::RF1, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        LOG_DEBUG("Received RF1 event at Kudu Connection.");
        callback(mTransactions.rf1(*mSession, args));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::RF2, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
       LOG_DEBUG("Received RF2 event at Kudu Connection.");
       callback(mTransactions.rf2(*mSession, args));
    }
};

template<>
Connection<KuduClient, KuduFiber>::Connection(boost::asio::io_service& service, KuduClient &client, DBGenerator<KuduClient, KuduFiber> &generator, int partitions)
    : mSocket(service)
    , mImpl(new CommandImpl<KuduClient>(this, mSocket, client, generator, partitions))
{}

template<>
Connection<KuduClient, KuduFiber>::~Connection() = default;

template<>
void Connection<KuduClient, KuduFiber>::run() {
    mImpl->run();
}

template<>
KuduClient Connection<KuduClient, KuduFiber>::getClient(std::string &storage, std::string&, size_t) {
    kudu::client::KuduClientBuilder clientBuilder;
    clientBuilder.add_master_server_addr(storage);
    std::tr1::shared_ptr<kudu::client::KuduClient> client;
    clientBuilder.Build(&client);
    return client;
}

} // namespace tpch

