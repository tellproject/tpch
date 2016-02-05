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
#include "TransactionsKudu.hpp"

#include <crossbow/logger.hpp>

using namespace boost::asio;


namespace tpch {

void assertOk(kudu::Status status) {
    if (!status.ok()) {
        LOG_ERROR("ERROR from Kudu: %1%", status.message().ToString());
        throw std::runtime_error(status.message().ToString().c_str());
    }
}

using Session = std::tr1::shared_ptr<kudu::client::KuduSession>;

template<>
Connection<KuduClient>::~Connection();

template<>
class CommandImpl<KuduClient> {
    Connection<KuduClient> *mConnection;
    boost::asio::ip::tcp::socket& mSocket;
    server::Server<CommandImpl<KuduClient>> mServer;
    Session mSession;
    TransactionsKudu mTransactions;

public:
    CommandImpl(Connection<KuduClient> *connection, boost::asio::ip::tcp::socket& socket, KuduClient &client)
        : mConnection(connection)
        , mSocket(socket)
        , mServer(*this, mSocket)
        , mSession(client->NewSession())
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
    typename std::enable_if<C == Command::EXIT, void>::type
    execute(const Callback callback) {
        mServer.quit();
        callback();
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::RF1, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        callback(mTransactions.rf1(*mSession, args));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::RF2, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
       callback(mTransactions.rf2(*mSession, args));
    }
};

template<>
Connection<KuduClient>::Connection(boost::asio::io_service& service, KuduClient &client)
    : mSocket(service)
    , mImpl(new CommandImpl<KuduClient>(this, mSocket, client))
{}

template<>
Connection<KuduClient>::~Connection() = default;

template<>
void Connection<KuduClient>::run() {
    mImpl->run();
}

template<>
KuduClient getClient(std::string &storage, std::string &commitManager) {
    kudu::client::KuduClientBuilder clientBuilder;
    clientBuilder.add_master_server_addr(storage);
    std::tr1::shared_ptr<kudu::client::KuduClient> client;
    clientBuilder.Build(&client);
    return client;
}

} // namespace tpch

