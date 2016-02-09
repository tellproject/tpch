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
Connection<TellClient>::~Connection();

template<>
class CommandImpl<TellClient> {
    Connection<TellClient> *mConnection;
    server::Server<CommandImpl<TellClient>> mServer;
    boost::asio::io_service& mService;
    TellClient mClient;
    std::unique_ptr<tell::db::TransactionFiber<void>> mFiber;
    Transactions mTransactions;

public:
    CommandImpl(
            Connection<TellClient> *connection,
            boost::asio::ip::tcp::socket& socket,
            boost::asio::io_service& service,
            TellClient client
    )
        : mConnection(connection)
        , mServer(*this, socket)
        , mService(service)
        , mClient(client)
    {}

    void run() {
        mServer.run();
    }

    void close() {
         delete mConnection;
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
Connection<TellClient>::Connection(boost::asio::io_service& service, TellClient &client)
    : mSocket(service)
    , mImpl(new CommandImpl<TellClient>(this, mSocket, service, client))
{}

template<>
Connection<TellClient>::~Connection() = default;

template<>
void Connection<TellClient>::run() {
    mImpl->run();
}

template<>
TellClient Connection<TellClient>::getClient(std::string &storage, std::string &commitManager) {
    tell::store::ClientConfig clientConfig;
    clientConfig.tellStore = tell::store::ClientConfig::parseTellStore(storage);
    clientConfig.commitManager = crossbow::infinio::Endpoint(crossbow::infinio::Endpoint::ipv4(), commitManager.c_str());
    clientConfig.numNetworkThreads = 7;
    return std::make_shared<TellClientImpl>(std::move(clientConfig));
}

} // namespace tpch

