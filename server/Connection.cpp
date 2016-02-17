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

#include <telldb/Transaction.hpp>
#include "Transactions.hpp"

using namespace boost::asio;

namespace tpch {

template<>
Connection<TellClient, TellFiber>::~Connection();

template<>
class CommandImpl<TellClient> {
    Connection<TellClient, TellFiber> *mConnection;
    server::Server<CommandImpl<TellClient>> mServer;
    boost::asio::io_service& mService;
    TellClient mClient;
    std::unique_ptr<tell::db::TransactionFiber<void>> mFiber;
    Transactions mTransactions;
    DBGenerator<TellClient, TellFiber> &mGenerator;

public:
    CommandImpl(
            Connection<TellClient, TellFiber> *connection,
            boost::asio::ip::tcp::socket& socket,
            boost::asio::io_service& service,
            TellClient client,
            DBGenerator<TellClient, TellFiber> &generator
    )
        : mConnection(connection)
        , mServer(*this, socket)
        , mService(service)
        , mClient(client)
        , mGenerator(generator)
    {}

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
            mGenerator.createSchema(mClient, args, 0);
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
        LOG_DEBUG("Received RF1 event at Tell Connection.");
        auto transaction = [this, args, callback](tell::db::Transaction& tx) {
            typename Signature<C>::result res = mTransactions.rf1(tx, args);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<void>(mClient->clientManager.startTransaction(transaction)));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::RF2, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        LOG_DEBUG("Received RF2 event at Tell Connection.");
        auto transaction = [this, args, callback](tell::db::Transaction& tx) {
            typename Signature<C>::result res = mTransactions.rf2(tx, args);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<void>(mClient->clientManager.startTransaction(transaction)));
    }

};

template<>
Connection<TellClient, TellFiber>::Connection(boost::asio::io_service& service, TellClient &client, DBGenerator<TellClient, TellFiber> &generator, int)
    : mSocket(service)
    , mImpl(new CommandImpl<TellClient>(this, mSocket, service, client, generator))
{}

template<>
Connection<TellClient, TellFiber>::~Connection() = default;

template<>
void Connection<TellClient, TellFiber>::run() {
    mImpl->run();
}

template<>
TellClient Connection<TellClient, TellFiber>::getClient(std::string &storage, std::string &commitManager, size_t networkThreads) {
    tell::store::ClientConfig clientConfig;
    clientConfig.tellStore = tell::store::ClientConfig::parseTellStore(storage);
    clientConfig.commitManager = crossbow::infinio::Endpoint(crossbow::infinio::Endpoint::ipv4(), commitManager.c_str());
    clientConfig.numNetworkThreads = networkThreads;
    return std::make_shared<TellClientImpl>(std::move(clientConfig));
}

} // namespace tpch
