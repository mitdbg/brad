# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

import logging
import time
import pathlib
import random
import pytz
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from io import TextIOWrapper

NAME_TO_IDX = {
    "DELIVERY": 0,
    "NEW_ORDER": 1,
    "ORDER_STATUS": 2,
    "PAYMENT": 3,
    "STOCK_LEVEL": 4,
}


class Results:
    def __init__(self, options: Optional[Dict[str, Any]] = None) -> None:
        self.start = None
        self.stop = None
        self.txn_id = 0

        self.txn_counters: Dict[str, int] = {}
        self.txn_abort_counters: Dict[str, int] = {}
        self.txn_times: Dict[str, float] = {}
        self.running: Dict[str, Tuple[str, float, datetime]] = {}

        if options is not None and "record_individual" in options:
            worker_index = options["worker_index"]
            output_prefix = pathlib.Path(options["output_prefix"])
            self._lat_file: Optional[TextIOWrapper] = open(
                output_prefix / "oltp_latency_{}.csv".format(worker_index),
                "w",
                encoding="UTF-8",
            )
            self._stats_file: Optional[TextIOWrapper] = open(
                output_prefix / "oltp_stats_{}.csv".format(worker_index),
                "w",
                encoding="UTF-8",
            )
            self._lat_sample_prob = options["lat_sample_prob"]
            self._prng: Optional[random.Random] = random.Random(
                worker_index
            )  # Deterministic pseudorandom.
            print("txn_idx,timestamp,run_time_s", file=self._lat_file, flush=True)
            print("stat,value", file=self._stats_file, flush=True)
        else:
            self._lat_file = None
            self._stats_file = None
            self._lat_sample_prob = 0.0
            self._prng = None

    def startBenchmark(self):
        """Mark the benchmark as having been started"""
        assert self.start == None
        logging.debug("Starting benchmark statistics collection")
        self.start = time.time()
        return self.start

    def stopBenchmark(self):
        """Mark the benchmark as having been stopped"""
        assert self.start != None
        assert self.stop == None
        logging.debug("Stopping benchmark statistics collection")
        self.stop = time.time()

        if self._lat_file is not None:
            self._lat_file.close()
            self._lat_file = None

        if self._stats_file is not None:
            for txn_name in NAME_TO_IDX.keys():
                commits = self.txn_counters.get(txn_name, 0)
                aborts = self.txn_abort_counters.get(txn_name, 0)
                print(f"{txn_name.lower()}_commits,{commits}", file=self._stats_file)
                print(f"{txn_name.lower()}_aborts,{aborts}", file=self._stats_file)
            self._stats_file.close()
            self._stats_file = None

    def startTransaction(self, txn: str) -> int:
        self.txn_id += 1
        id = self.txn_id
        self.running[id] = (txn, time.time(), datetime.now(tz=pytz.utc))
        return id

    def abortTransaction(self, id: int) -> None:
        """Abort a transaction and discard its times"""
        assert id in self.running
        txn_name, _, _ = self.running[id]
        del self.running[id]

        if txn_name not in self.txn_abort_counters:
            self.txn_abort_counters[txn_name] = 1
        else:
            self.txn_abort_counters[txn_name] += 1

    def stopTransaction(self, id: int) -> None:
        """Record that the benchmark completed an invocation of the given transaction"""
        assert id in self.running
        txn_name, txn_start, start_ts = self.running[id]
        del self.running[id]

        duration = time.time() - txn_start
        total_time = self.txn_times.get(txn_name, 0)
        self.txn_times[txn_name] = total_time + duration

        total_cnt = self.txn_counters.get(txn_name, 0)
        self.txn_counters[txn_name] = total_cnt + 1

        if self._prng is not None and self._lat_file is not None:
            if self._prng.random() < self._lat_sample_prob:
                print(
                    f"{NAME_TO_IDX[txn_name]},{start_ts},{duration}",
                    file=self._lat_file,
                    flush=True,
                )

    def append(self, r: "Results") -> None:
        for txn_name in r.txn_counters.keys():
            orig_cnt = self.txn_counters.get(txn_name, 0)
            orig_time = self.txn_times.get(txn_name, 0)

            self.txn_counters[txn_name] = orig_cnt + r.txn_counters[txn_name]
            self.txn_times[txn_name] = orig_time + r.txn_times[txn_name]
            # logging.debug("%s [cnt=%d, time=%d]" % (txn_name, self.txn_counters[txn_name], self.txn_times[txn_name]))
        ## HACK
        self.start = r.start
        self.stop = r.stop

    def __str__(self) -> str:
        return self.show()

    def show(self, load_time=None):
        if self.start == None:
            return "Benchmark not started"
        if self.stop == None:
            duration = time.time() - self.start
        else:
            duration = self.stop - self.start

        col_width = 16
        total_width = (col_width * 4) + 2
        f = "\n  " + (("%-" + str(col_width) + "s") * 4)
        line = "-" * total_width

        ret = "" + "=" * total_width + "\n"
        if load_time != None:
            ret += "Data Loading Time: %d seconds\n\n" % (load_time)

        ret += "Execution Results after %d seconds\n%s" % (duration, line)
        ret += f % ("", "Executed", "Time (µs)", "Rate")

        total_time = 0
        total_cnt = 0
        for txn in sorted(self.txn_counters.keys()):
            txn_time = self.txn_times[txn]
            txn_cnt = self.txn_counters[txn]
            rate = "%.02f txn/s" % ((txn_cnt / txn_time))
            ret += f % (txn, str(txn_cnt), str(txn_time * 1000000), rate)

            total_time += txn_time
            total_cnt += txn_cnt
        ret += "\n" + ("-" * total_width)
        if total_time > 0:
            total_rate = "%.02f txn/s" % ((total_cnt / total_time))
        else:
            total_rate = "-- txn/s"
        ret += f % ("TOTAL", str(total_cnt), str(total_time * 1000000), total_rate)

        return ret.encode("utf-8")


## CLASS
