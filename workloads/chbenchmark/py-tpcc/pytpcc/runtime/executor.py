# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
#
# Original Java Version:
# Copyright (C) 2008
# Evan Jones
# Massachusetts Institute of Technology
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

import sys
import multiprocessing
import time
import random
import traceback
import logging
import os
import pathlib
import numpy as np
from datetime import datetime
from pprint import pprint, pformat
from brad.utils.rand_exponential_backoff import RandomizedExponentialBackoff
from brad.utils import create_custom_logger
from typing import Optional

from .. import constants
from ..util import *

RECORD_DETAILED_STATS_VAR = "RECORD_DETAILED_STATS"


class Executor:
    def __init__(self, driver, scaleParameters, stop_on_error=False, pct_remote=0.1):
        self.driver = driver
        self.scaleParameters = scaleParameters
        self.stop_on_error = stop_on_error
        self.pct_remote = pct_remote

        self.local_warehouse_range = (
            self.scaleParameters.starting_warehouse,
            self.scaleParameters.ending_warehouse,
        )
        self.total_workers = 1
        self.worker_index = 0

        self.skew_alpha = None
        self.skew_prng = None

    ## DEF

    def execute(
        self,
        duration: float,
        worker_index: int,
        total_workers: int,
        lat_sample_prob: float,
        zipfian_alpha: Optional[float],
    ) -> results.Results:
        if RECORD_DETAILED_STATS_VAR in os.environ:
            import conductor.lib as cond

            try:
                out_path = cond.get_output_path()
                logging.info("Writing detailed stats to %s", str(out_path))
            except ImportError:
                logging.warning(
                    "Conductor not installed. Detailed stats will be saved to the current working directory."
                )
                out_path = pathlib.Path(".")
            except RuntimeError:
                logging.warning(
                    "Workload is not orchestrated by Conductor. Detailed stats will be saved to the current working directory."
                )
                out_path = pathlib.Path(".")

            options = {
                "record_detailed": True,
                "worker_index": worker_index,
                "output_prefix": out_path,
                "lat_sample_prob": lat_sample_prob,
            }
        else:
            logging.info("Not recording detailed stats.")
            options = {}

        verbose_log_dir = out_path / "verbose_logs"
        verbose_log_dir.mkdir(exist_ok=True)
        verbose_logger = create_custom_logger(
            "txn_runner_verbose", str(verbose_log_dir / f"runner_{worker_index}.log")
        )
        verbose_logger.info("[T %d] Workload starting...", worker_index)

        # Compute warehouse ranges.
        self.worker_index = worker_index
        self.total_workers = total_workers
        warehouses_per_worker = self.scaleParameters.warehouses // total_workers
        min_warehouse = worker_index * warehouses_per_worker
        # N.B. Warehouse IDs are 1-based and this range is supposed to be
        # inclusive.
        self.local_warehouse_range = (
            min_warehouse + 1,
            min_warehouse + warehouses_per_worker,
        )
        logging.info(
            "Worker index %d - Warehouse range: %d to %d (inclusive)",
            self.worker_index,
            *self.local_warehouse_range,
        )

        if zipfian_alpha is not None:
            self.skew_alpha = zipfian_alpha
            self.skew_prng = np.random.default_rng(seed=42 ^ worker_index)
            logging.info(
                "Worker index %d - Selecting warehouse and items using a Zipfian distribution; a = %.2f",
                worker_index,
                self.skew_alpha,
            )
        else:
            logging.info("Worker index %d - Not using a Zipfian distribution")

        r = results.Results(options)
        assert r
        logging.info("Executing benchmark for %d seconds" % duration)
        start = r.startBenchmark()
        debug = logging.getLogger().isEnabledFor(logging.DEBUG)
        backoff: Optional[RandomizedExponentialBackoff] = None

        while (time.time() - start) <= duration:
            txn, params = self.doOne()
            txn_id = r.startTransaction(txn)

            if debug:
                logging.debug("Executing '%s' transaction" % txn)
            try:
                verbose_logger.info("[T %d] Issuing transaction %s", worker_index, txn)
                val = self.driver.executeTransaction(txn, params)
                backoff = None
                # if debug: logging.debug("%s\nParameters:\n%s\nResult:\n%s" % (txn, pformat(params), pformat(val)))
                r.stopTransaction(txn_id)
                verbose_logger.info(
                    "[T %d] Finished transaction %s, %d", worker_index, txn, txn_id
                )

            except KeyboardInterrupt:
                verbose_logger.info(
                    "[T %d] Aborting early due to KeyboardInterrupt", worker_index
                )
                return -1
            except (Exception, AssertionError) as ex:
                if debug:
                    logging.warn("Failed to execute Transaction '%s': %s" % (txn, ex))
                    traceback.print_exc(file=sys.stdout)
                elif random.random() < 0.01:
                    logging.warning("Aborted transaction: %s: %s", txn, ex)
                    traceback.print_exc(file=sys.stdout)
                verbose_logger.exception("[T %d] Ran into error", worker_index)
                if self.stop_on_error:
                    raise
                r.abortTransaction(txn_id)

                try:
                    self.driver.ensureRollback()
                except:  # pylint: disable=bare-except
                    # This may happen if we try to issue a rollback when the connection has dropped.
                    verbose_logger.exception(
                        "[T %d] Ran into error when running rollback.", worker_index
                    )

                # Back off slightly.
                if backoff is None:
                    backoff = RandomizedExponentialBackoff(
                        max_retries=10, base_delay_s=0.001, max_delay_s=1.0
                    )
                wait_s = backoff.wait_time_s()
                if wait_s is not None:
                    verbose_logger.info(
                        "[T %d] Backing off for %.4f seconds", worker_index, wait_s
                    )
                    time.sleep(wait_s)

        ## WHILE

        verbose_logger.info("[T %d] Benchmark stopping...", worker_index)
        r.stopBenchmark()
        verbose_logger.info("[T %d] Benchmark done.", worker_index)
        return r

    ## DEF

    def doOne(self):
        """Selects and executes a transaction at random. The number of new order transactions executed per minute is the official "tpmC" metric. See TPC-C 5.4.2 (page 71)."""

        ## This is not strictly accurate: The requirement is for certain
        ## *minimum* percentages to be maintained. This is close to the right
        ## thing, but not precisely correct. See TPC-C 5.2.4 (page 68).
        x = rand.number(1, 100)
        params = None
        txn = None
        if x <= 4:  ## 4%
            txn, params = (
                constants.TransactionTypes.STOCK_LEVEL,
                self.generateStockLevelParams(),
            )
        elif x <= 4 + 4:  ## 4%
            txn, params = (
                constants.TransactionTypes.DELIVERY,
                self.generateDeliveryParams(),
            )
        elif x <= 4 + 4 + 4:  ## 4%
            txn, params = (
                constants.TransactionTypes.ORDER_STATUS,
                self.generateOrderStatusParams(),
            )
        elif x <= 43 + 4 + 4 + 4:  ## 43%
            txn, params = (
                constants.TransactionTypes.PAYMENT,
                self.generatePaymentParams(),
            )
        else:  ## 45%
            assert x > 100 - 45
            txn, params = (
                constants.TransactionTypes.NEW_ORDER,
                self.generateNewOrderParams(),
            )

        return (txn, params)

    ## DEF

    ## ----------------------------------------------
    ## generateDeliveryParams
    ## ----------------------------------------------
    def generateDeliveryParams(self):
        """Return parameters for DELIVERY"""
        w_id = self.makeWarehouseId()
        o_carrier_id = rand.number(constants.MIN_CARRIER_ID, constants.MAX_CARRIER_ID)
        ol_delivery_d = datetime.now()
        return makeParameterDict(locals(), "w_id", "o_carrier_id", "ol_delivery_d")

    ## DEF

    ## ----------------------------------------------
    ## generateNewOrderParams
    ## ----------------------------------------------
    def generateNewOrderParams(self):
        """Return parameters for NEW_ORDER"""
        w_id = self.makeWarehouseId()
        d_id = self.makeDistrictId()
        c_id = self.makeCustomerId()
        ol_cnt = rand.number(constants.MIN_OL_CNT, constants.MAX_OL_CNT)
        o_entry_d = datetime.now()

        ## 1% of transactions roll back
        rollback = False  # FIXME rand.number(1, 100) == 1

        i_ids = []
        i_w_ids = []
        i_qtys = []
        for i in range(0, ol_cnt):
            if rollback and i + 1 == ol_cnt:
                i_ids.append(self.scaleParameters.items + 1)
            else:
                i_id = self.makeItemId()
                while i_id in i_ids:
                    i_id = self.makeItemId()
                i_ids.append(i_id)

            ## 1% of items are from a remote warehouse
            remote = rand.number(1, 100) == 1
            if self.scaleParameters.warehouses > 1 and remote:
                i_w_ids.append(
                    rand.numberExcluding(
                        self.scaleParameters.starting_warehouse,
                        self.scaleParameters.ending_warehouse,
                        w_id,
                    )
                )
            else:
                i_w_ids.append(w_id)

            i_qtys.append(rand.number(1, constants.MAX_OL_QUANTITY))
        ## FOR

        return makeParameterDict(
            locals(), "w_id", "d_id", "c_id", "o_entry_d", "i_ids", "i_w_ids", "i_qtys"
        )

    ## DEF

    ## ----------------------------------------------
    ## generateOrderStatusParams
    ## ----------------------------------------------
    def generateOrderStatusParams(self):
        """Return parameters for ORDER_STATUS"""
        w_id = self.makeWarehouseId()
        d_id = self.makeDistrictId()
        c_last = None
        c_id = None

        ## 60%: order status by last name
        if rand.number(1, 100) <= 60:
            c_last = rand.makeRandomLastName(self.scaleParameters.customersPerDistrict)

        ## 40%: order status by id
        else:
            c_id = self.makeCustomerId()

        return makeParameterDict(locals(), "w_id", "d_id", "c_id", "c_last")

    ## DEF

    ## ----------------------------------------------
    ## generatePaymentParams
    ## ----------------------------------------------
    def generatePaymentParams(self):
        """Return parameters for PAYMENT"""
        x = rand.number(1, 100)
        y = rand.number(1, 100)

        w_id = self.makeWarehouseId()
        d_id = self.makeDistrictId()
        c_w_id = None
        c_d_id = None
        c_id = None
        c_last = None
        h_amount = rand.fixedPoint(2, constants.MIN_PAYMENT, constants.MAX_PAYMENT)
        h_date = datetime.now()

        ## 85%: paying through own warehouse (or there is only 1 warehouse)
        if self.scaleParameters.warehouses == 1 or x <= 85:
            c_w_id = w_id
            c_d_id = d_id
        ## 15%: paying through another warehouse:
        else:
            ## select in range [1, num_warehouses] excluding w_id
            c_w_id = rand.numberExcluding(
                self.scaleParameters.starting_warehouse,
                self.scaleParameters.ending_warehouse,
                w_id,
            )
            assert c_w_id != w_id
            c_d_id = self.makeDistrictId()

        ## 60%: payment by last name
        if y <= 60:
            c_last = rand.makeRandomLastName(self.scaleParameters.customersPerDistrict)
        ## 40%: payment by id
        else:
            assert y > 60
            c_id = self.makeCustomerId()

        return makeParameterDict(
            locals(),
            "w_id",
            "d_id",
            "h_amount",
            "c_w_id",
            "c_d_id",
            "c_id",
            "c_last",
            "h_date",
        )

    ## DEF

    ## ----------------------------------------------
    ## generateStockLevelParams
    ## ----------------------------------------------
    def generateStockLevelParams(self):
        """Returns parameters for STOCK_LEVEL"""
        w_id = self.makeWarehouseId()
        d_id = self.makeDistrictId()
        threshold = rand.number(
            constants.MIN_STOCK_LEVEL_THRESHOLD, constants.MAX_STOCK_LEVEL_THRESHOLD
        )
        return makeParameterDict(locals(), "w_id", "d_id", "threshold")

    ## DEF

    def makeWarehouseId(self):
        if random.random() < self.pct_remote:
            # Generate remote.
            while True:
                w_id = rand.number(
                    self.scaleParameters.starting_warehouse,
                    self.scaleParameters.ending_warehouse,
                )
                if self.total_workers == 1 or not (
                    w_id >= self.local_warehouse_range[0]
                    and w_id <= self.local_warehouse_range[1]
                ):
                    break
        else:
            if self.skew_prng is not None:
                # Skewed warehouse choice
                min_warehouse, max_warehouse = self.local_warehouse_range
                warehouse_span = max_warehouse - min_warehouse + 1
                while True:
                    # Chosen in range [1, inf)
                    candidate = self.skew_prng.zipf(a=self.skew_alpha)
                    if candidate <= warehouse_span:
                        break
                return min_warehouse + (candidate - 1)
            else:
                # Uniformly randomly chosen warehouse
                w_id = rand.number(*self.local_warehouse_range)

        assert w_id >= self.scaleParameters.starting_warehouse, (
            "Invalid W_ID: %d" % w_id
        )
        assert w_id <= self.scaleParameters.ending_warehouse, "Invalid W_ID: %d" % w_id
        return w_id

    ## DEF

    def makeDistrictId(self):
        return rand.number(1, self.scaleParameters.districtsPerWarehouse)

    ## DEF

    def makeCustomerId(self):
        return rand.NURand(1023, 1, self.scaleParameters.customersPerDistrict)

    ## DEF

    def makeItemId(self):
        if self.skew_alpha is None:
            return rand.NURand(8191, 1, self.scaleParameters.items)
        else:
            # Select item ID using a zipfian distribution.
            while True:
                candidate = self.skew_prng.zipf(a=self.skew_alpha)
                if candidate <= self.scaleParameters.items:
                    return candidate

    ## DEF


## CLASS


def makeParameterDict(values, *args):
    return dict(map(lambda x: (x, values[x]), args))


## DEF
