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
#include <boost/asio.hpp>
#include <common/Protocol.hpp>
#include <random>
#include <chrono>
#include <deque>

#include <common/Util.hpp>

namespace tpch {

using Clock = std::chrono::system_clock;

struct LogEntry {
    bool success;
    crossbow::string error;
    Command transaction;
    decltype(Clock::now()) start;
    decltype(start) end;
};

static const std::string orderFilePrefix = "orders.tbl.u";
static const std::string lineitemFilePrefix = "lineitem.tbl.u";
static const uint orderBatchSize = 100; // size of sub-batches of an update batch to be sent to the server

class Client {
    using Socket = boost::asio::ip::tcp::socket;
    Socket mSocket;
    client::CommandsImpl mCmds;
    std::vector<Order> mOrders;
    std::vector<int32_t> mDeletes;
    uint mCurrentStartIdx;
    bool mDoInsert; // true for RF1, false for RF2
    const uint mUpdateBatchSize;  // batch size to be logged as an update
    uint mBatchCounter;
    bool mIsLast;   // is last subbatch of a batch
    decltype(Clock::now()) mBatchStartTime;
    std::deque<LogEntry> mLog;
    decltype(Clock::now()) mEndTime;

public:
    Client(boost::asio::io_service& service, decltype(Clock::now()) endTime, const uint updateBatchSize,
            const std::string &baseDir, const uint &updateFileIndex);

    Socket& socket() {
        return mSocket;
    }
    const Socket& socket() const {
        return mSocket;
    }
    client::CommandsImpl& commands() {
        return mCmds;
    }
    void run(); // executes RF1 (with mUpdateBatchSize inserted orders), followed by RF2 (the same orders deleted) repeatedly
    const std::deque<LogEntry>& log() const { return mLog; }
private:
    template<Command C>
    void execute(const typename Signature<C>::arguments& arg);
};

} // namespace tpch

