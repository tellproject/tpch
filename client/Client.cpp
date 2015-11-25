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
#include <common/Protocol.hpp>
#include <crossbow/logger.hpp>

using err_code = boost::system::error_code;

namespace tpch {

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
      },
      arg);
}

} // namespace tpcc
