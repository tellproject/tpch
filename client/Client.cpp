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

#include "CreatePopulate.hpp"

#include <fstream>
#include <sstream>

#include <boost/unordered_map.hpp>

#include <common/Protocol.hpp>
#include <crossbow/logger.hpp>

using err_code = boost::system::error_code;

namespace tpch {

Client::Client(boost::asio::io_service& service, decltype(Clock::now()) endTime, const std::string &baseDir, const uint &updateFileIndex)
    : mSocket(service)
    , mCmds(mSocket)
    , mCurrentIdx(0)
    , mEndTime(endTime)
{
    // open order file
    std::string fName = baseDir + "/" + orderFilePrefix + std::to_string(updateFileIndex);
    if (!file_readable(fName))
        LOG_ERROR("Error: file " + fName + "does not exist!");
    std::fstream orderIn(fName.c_str(), std::ios_base::in);

    fName = baseDir + "/" + lineitemFilePrefix + std::to_string(updateFileIndex);
    if (!file_readable(fName))
        LOG_ERROR("Error: file " + fName + "does not exist!");
    std::fstream lineItemIn(fName.c_str(), std::ios_base::in);

    // create order tuples
    using order_t = std::tuple<int32_t, int32_t, crossbow::string, double, date, crossbow::string, crossbow::string, int32_t, crossbow::string>;
    boost::unordered_map<int32_t, Order*> orderIdToOrder;
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
        orderIdToOrder.emplace(std::make_pair(order.orderkey, &order));
    });

    // create lineitem tuples within orders
    using lineitem_t = std::tuple<int32_t, int32_t, int32_t, int32_t, double, double, double, double, crossbow::string, crossbow::string, date, date, date, crossbow::string, crossbow::string, crossbow::string>;
    getFields<lineitem_t>(lineItemIn, [&] (const lineitem_t& fields) {
        int32_t orderKey = std::get<3>(fields);
        auto it = orderIdToOrder.find(orderKey);
        if (it == orderIdToOrder.end())
            LOG_ERROR("Error: no order found with orderkey " + std::to_string(orderKey));
        Order &order = *(it->second);
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

template <Command C>
void Client::execute(const typename Signature<C>::arguments &arg) {
    auto now = Clock::now();
    if (now > mEndTime) {
        // Time's up
        // benchmarking finished
        mSocket.shutdown(Socket::shutdown_both);
        mSocket.close();
        return;
    }
    mCmds.execute<C>(
      [this, now](const err_code &ec, typename Signature<C>::result result) {
          if (ec) {
              LOG_ERROR("Error: " + ec.message());
              return;
          }
          auto end = Clock::now();
          if (!result.success) {
              LOG_ERROR("Transaction unsuccessful [error = %1%]", result.error);
          }
          mLog.push_back(LogEntry{result.success, result.error, C, now, end});
          run();
      },
      arg);
}

void Client::run() {
    size_t endIdx = std::min(size_t(mCurrentIdx + orderBatchSize), mOrders.size());
    LOG_DEBUG("Start RF1 Transaction");
    RF1In rf1args ({std::vector<Order>(&mOrders[mCurrentIdx], &mOrders[endIdx])});
    execute<Command::RF1>(rf1args);
    LOG_DEBUG("Start RF2 Transaction");
    RF2In rf2args ({std::vector<int32_t>(&mDeletes[mCurrentIdx], &mDeletes[endIdx])});;
    execute<Command::RF2>(rf2args);
    mCurrentIdx = (mCurrentIdx + orderBatchSize) % mOrders.size();
}


} // namespace tpch
