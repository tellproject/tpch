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

class CommandImpl {
    server::Server<CommandImpl> mServer;
    boost::asio::io_service& mService;
    tell::db::ClientManager<void>& mClientManager;
    std::unique_ptr<tell::db::TransactionFiber<void>> mFiber;
    Transactions mTransactions;

public:
    CommandImpl(boost::asio::ip::tcp::socket& socket,
            boost::asio::io_service& service,
            tell::db::ClientManager<void>& clientManager)
        : mServer(*this, socket)
        , mService(service)
        , mClientManager(clientManager)
    {}

    void run() {
        mServer.run();
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
        auto transaction = [this, args, callback](tell::db::Transaction& tx) {
            typename Signature<C>::result res = mTransactions.rf1(tx, args);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<void>(mClientManager.startTransaction(transaction)));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::RF2, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        auto transaction = [this, args, callback](tell::db::Transaction& tx) {
            typename Signature<C>::result res = mTransactions.rf2(tx, args);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<void>(mClientManager.startTransaction(transaction)));
    }

};

Connection::Connection(boost::asio::io_service& service, tell::db::ClientManager<void>& clientManager)
    : mSocket(service)
    , mImpl(new CommandImpl(mSocket, service, clientManager))
{}

Connection::~Connection() = default;

void Connection::run() {
    mImpl->run();
}

} // namespace tpch

