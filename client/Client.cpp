/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and
 * others.
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
#include "Client.hpp"

#include <fstream>
#include <sstream>

#include <boost/unordered_map.hpp>

#include <common/Protocol.hpp>
#include <crossbow/logger.hpp>

#include "common/Util.hpp"

using err_code = boost::system::error_code;

namespace tpch {

Client::Client(boost::asio::io_service& service,
        const uint updateBatchSize)
    : mSocket(service)
    , mCmds(mSocket)
    , mCurrentStartIdx(0)
    , mDoInsert(true)
    , mUpdateBatchSize(updateBatchSize)
    , mBatchCounter(0)
    , mIsLast(false)
    , mBatchStartTime(Clock::now())
    , mEndTime(Clock::now())
{}

template <Command C>
void Client::execute(const typename Signature<C>::arguments &arg) {
    if (mBatchStartTime > mEndTime) {
        // Time's up
        // benchmarking finished
        mSocket.shutdown(Socket::shutdown_both);
        mSocket.close();
        return;
    }
    mCmds.execute<C>(
      [this](const err_code &ec, typename Signature<C>::result result) {
          if (ec) {
              LOG_ERROR("Error: " + ec.message());
              return;
          }
          if (!result.success) {
              LOG_ERROR("Transaction unsuccessful [error = %1%]", result.error);
          }
          LOG_DEBUG("Affected rows: %1%", result.affectedRows);
          if (mIsLast) {
              mBatchCounter = 0;
              mDoInsert = !mDoInsert;
              auto end = Clock::now();
                  mLog.push_back(LogEntry{result.success, result.error, C, mBatchStartTime, end});
              }
              run();
          },
          arg);
    }

    void Client::prepare(const std::string &baseDir, const uint updateFileIndex) {
        // open order file
        std::string fName = baseDir + "/" + orderFilePrefix + std::to_string(updateFileIndex+1);
        if (!file_readable(fName))
            LOG_ERROR("Error: file " + fName + " does not exist!");
        std::fstream orderIn(fName.c_str(), std::ios_base::in);

        fName = baseDir + "/" + lineitemFilePrefix + std::to_string(updateFileIndex+1);
        if (!file_readable(fName))
            LOG_ERROR("Error: file " + fName + " does not exist!");
        std::fstream lineItemIn(fName.c_str(), std::ios_base::in);

        // create order tuples
        using order_t = std::tuple<int32_t, int32_t, crossbow::string, double, date, crossbow::string, crossbow::string, int32_t, crossbow::string>;
        boost::unordered_map<int32_t, size_t> orderIdToIdx;
        getFields<order_t>(orderIn, [&] (const order_t& fields) {
            mOrders.emplace_back();
            Order &order = mOrders.back();
            order.orderkey = std::get<0>(fields);
            order.custkey = std::get<1>(fields);
            order.orderstatus = std::get<2>(fields);
            order.totalprice = std::get<3>(fields);
            order.orderdate = std::get<4>(fields).value;
            order.orderpriority = std::get<5>(fields);
            order.clerk = std::get<6>(fields);
            order.shippriority = std::get<7>(fields);
            order.comment = std::get<8>(fields);
            order.lineitems.reserve(7);
            orderIdToIdx.emplace(std::make_pair(order.orderkey, mOrders.size()-1));
        });

        // create lineitem tuples within orders
        using lineitem_t = std::tuple<int32_t, int32_t, int32_t, int32_t, double, double, double, double, crossbow::string, crossbow::string, date, date, date, crossbow::string, crossbow::string, crossbow::string>;
        getFields<lineitem_t>(lineItemIn, [&] (const lineitem_t& fields) {
            int32_t orderKey = std::get<0>(fields);
            auto it = orderIdToIdx.find(orderKey);
            if (it == orderIdToIdx.end())
                LOG_ERROR("Error: no order found with orderkey " + std::to_string(orderKey));
            Order &order = mOrders[it->second];
            order.lineitems.emplace_back();
            Lineitem &item = order.lineitems.back();
            item.orderkey =      std::get<0>(fields);
            item.partkey =       std::get<1>(fields);
            item.suppkey =       std::get<2>(fields);
            item.linenumber =    std::get<3>(fields);
            item.quantity =      std::get<4>(fields);
            item.extendedprice = std::get<5>(fields);
            item.discount =      std::get<6>(fields);
            item.tax =           std::get<7>(fields);
            item.returnflag =    std::get<8>(fields);
            item.linestatus =    std::get<9>(fields);
            item.shipdate =      std::get<10>(fields).value;
            item.commitdate =    std::get<11>(fields).value;
            item.receiptdate =   std::get<12>(fields).value;
            item.shipinstruct =  std::get<13>(fields);
            item.shipmode =      std::get<14>(fields);
            item.comment =       std::get<15>(fields);
        });

        // create delete orders
        mDeletes.reserve(mOrders.size());
        for (auto &order: mOrders)
            mDeletes.emplace_back(order.orderkey);
    }

    void Client::run(decltype(Clock::now()) endTime) {
        mEndTime = endTime;
    }

    void Client::run() {
        // determine whether we are at that start of a batch and take timestamp if necessary
        if (mBatchCounter == 0)
            mBatchStartTime = Clock::now();

        // determine size of current batch to be sent and whether is the last one
        uint batchSize = mUpdateBatchSize - mBatchCounter;
        mIsLast = batchSize <= orderBatchSize;
        if (!mIsLast)
            batchSize = orderBatchSize;

        // determine batchStartIndex, endIdx and modular endIdx to take data from
        size_t batchStartIndex = mCurrentStartIdx + mBatchCounter;
        size_t endIdx = std::min(size_t(batchStartIndex + batchSize), mOrders.size()); // end of batch or end of vector
        size_t modularEndIdx = (batchStartIndex + batchSize) % mOrders.size();         // eqaul to endIndex if not end of vector

        // do update or insert
        mBatchCounter += batchSize;
        if (mDoInsert) {
            LOG_DEBUG("Start RF1 Transaction");
            RF1In rf1args ({std::vector<Order>(&mOrders[batchStartIndex], &mOrders[endIdx])});
            if (modularEndIdx != endIdx)
                rf1args.orders.insert(rf1args.orders.end(), &mOrders[0], &mOrders[modularEndIdx]);
            LOG_DEBUG("Input has %1$ orders. First order in batch has ID %2.",
                      rf1args.orders.size(), rf1args.orders[0].orderkey);
            execute<Command::RF1>(rf1args);
        } else {
            LOG_DEBUG("Start RF2 Transaction");
            RF2In rf2args ({std::vector<int32_t>(&mDeletes[batchStartIndex], &mDeletes[endIdx])});
            if (modularEndIdx != endIdx)
                rf2args.orderIds.insert(rf2args.orderIds.end(), &mDeletes[0], &mDeletes[modularEndIdx]);
            LOG_DEBUG("Input has $1$ orders. First order in batch has ID %2%.",
                      rf2args.orderIds.size(), rf2args.orderIds[0]);
            execute<Command::RF2>(rf2args);
        }
    }

    bool Client::populate(const std::string &baseDir, const uint32_t updateFileIndex)
    {
        uint32_t partIndex = updateFileIndex+1;
        std::string fName = baseDir + "/orders.tbl." + std::to_string(partIndex);
        if (file_readable(fName)) {
            mCmds.execute<tpch::Command::POPULATE>([partIndex](const err_code& ec, const std::tuple<bool, crossbow::string>& res){
                if (ec) {
                    LOG_ERROR(ec.message());
                    return;
                }
                if (!std::get<0>(res)) {
                    LOG_ERROR(std::get<1>(res));
                    return;
                }
                LOG_INFO("Populated part %1% of the database.", partIndex);
            return;
        }, std::make_pair(partIndex, crossbow::string(baseDir)));
        return true;
    }
    return false;
}

} // namespace tpch
