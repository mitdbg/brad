import logging
import traceback
import decimal
import os
import time
from typing import Dict, Tuple, Any, Optional, List

from .abstractdriver import *
from .. import constants

from brad.connection.psycopg_connection import PsycopgConnection
from brad.connection.psycopg_cursor import PsycopgCursor
import conductor.lib as cond

Config = Dict[str, Tuple[str, Any]]

logger = logging.getLogger(__name__)


TXN_QUERIES = {
    "DELIVERY": {
        "getNewOrder": "SELECT no_o_id FROM new_order WHERE no_d_id = {} AND no_w_id = {} AND no_o_id > -1 LIMIT 1",  #
        "deleteNewOrder": "DELETE FROM new_order WHERE no_d_id = {} AND no_w_id = {} AND no_o_id = {}",  # d_id, w_id, no_o_id
        "getCId": "SELECT o_c_id FROM orders WHERE o_id = {} AND o_d_id = {} AND o_w_id = {}",  # no_o_id, d_id, w_id
        "updateOrders": "UPDATE orders SET o_carrier_id = {} WHERE o_id = {} AND o_d_id = {} AND o_w_id = {}",  # o_carrier_id, no_o_id, d_id, w_id
        "updateOrderLine": "UPDATE order_line SET ol_delivery_d = '{}' WHERE ol_o_id = {} AND ol_d_id = {} AND ol_w_id = {}",  # o_entry_d, no_o_id, d_id, w_id
        "sumOLAmount": "SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = {} AND ol_d_id = {} AND ol_w_id = {}",  # no_o_id, d_id, w_id
        "updateCustomer": "UPDATE customer SET c_balance = c_balance + {} WHERE c_id = {} AND c_d_id = {} AND c_w_id = {}",  # ol_total, c_id, d_id, w_id
    },
    "NEW_ORDER": {
        "getWarehouseTaxRate": "SELECT w_tax FROM warehouse WHERE w_id = {}",  # w_id
        "getDistrict": "SELECT d_tax, d_next_o_id FROM district WHERE d_id = {} AND d_w_id = {}",  # d_id, w_id
        "incrementNextOrderId": "UPDATE district SET d_next_o_id = {} WHERE d_id = {} AND d_w_id = {}",  # d_next_o_id, d_id, w_id
        "getCustomer": "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",  # w_id, d_id, c_id
        "createOrder": "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES ({}, {}, {}, {}, '{}', {}, {}, {})",  # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
        "createNewOrder": "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES ({}, {}, {})",  # o_id, d_id, w_id
        "getItemInfo": "SELECT i_price, i_name, i_data FROM item WHERE i_id = {}",  # ol_i_id
        "getStockInfo": "SELECT s_quantity, s_data, s_ytd, s_order_cnt, s_remote_cnt, s_dist_{:02d} FROM stock WHERE s_i_id = {} AND s_w_id = {}",  # d_id, ol_i_id, ol_supply_w_id
        "updateStock": "UPDATE stock SET s_quantity = {}, s_ytd = {}, s_order_cnt = {}, s_remote_cnt = {} WHERE s_i_id = {} AND s_w_id = {}",  # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
        "createOrderLine": "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES ({}, {}, {}, {}, {}, {}, '{}', {}, {}, '{}')",  # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
        "createOrderLineMultivalue": "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES ",
        "createOrderLineValues": "({}, {}, {}, {}, {}, {}, '{}', {}, {}, '{}')",
    },
    "ORDER_STATUS": {
        "getCustomerByCustomerId": "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",  # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}' ORDER BY c_first",  # w_id, d_id, c_last
        "getLastOrder": "SELECT o_id, o_carrier_id, o_entry_d FROM orders WHERE o_w_id = {} AND o_d_id = {} AND o_c_id = {} ORDER BY o_id DESC LIMIT 1",  # w_id, d_id, c_id
        "getOrderLines": "SELECT ol_supply_w_id, ol_i_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = {} AND ol_d_id = {} AND ol_o_id = {}",  # w_id, d_id, o_id
    },
    "PAYMENT": {
        "getWarehouse": "SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip FROM warehouse WHERE w_id = {}",  # w_id
        "updateWarehouseBalance": "UPDATE warehouse SET w_ytd = w_ytd + {} WHERE w_id = {}",  # h_amount, w_id
        "getDistrict": "SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip FROM district WHERE d_w_id = {} AND d_id = {}",  # w_id, d_id
        "updateDistrictBalance": "UPDATE district SET d_ytd = d_ytd + {} WHERE d_w_id = {} AND d_id = {}",  # h_amount, d_w_id, d_id
        "getCustomerByCustomerId": "SELECT c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_data FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",  # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_data FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}' ORDER BY c_first",  # w_id, d_id, c_last
        "updateBCCustomer": "UPDATE customer SET c_balance = {}, c_ytd_payment = {}, c_payment_cnt = {}, c_data = '{}' WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",  # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id
        "updateGCCustomer": "UPDATE customer SET c_balance = {}, c_ytd_payment = {}, c_payment_cnt = {} WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",  # c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id
        "insertHistory": "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES ({}, {}, {}, {}, {}, '{}', {}, '{}')",
    },
    "STOCK_LEVEL": {
        "getOId": "SELECT d_next_o_id FROM district WHERE d_w_id = {} AND d_id = {}",
        "getStockCount": """
            SELECT COUNT(DISTINCT(ol_i_id)) FROM order_line, stock
            WHERE ol_w_id = {}
              AND ol_d_id = {}
              AND ol_o_id < {}
              AND ol_o_id >= {}
              AND s_w_id = {}
              AND s_i_id = ol_i_id
              AND s_quantity < {}
        """,
    },
}


class AuroraTimingDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "host": ("Host running the database.", "localhost"),
        "port": ("Port on which the database is listening.", 5432),
        "user": ("Username", "postgres"),
        "password": ("Password", ""),
        "database": ("Database", "chbenchmark"),
        "isolation_level": ("The isolation level to use.", "REPEATABLE READ"),
    }

    def __init__(self, ddl: str) -> None:
        super().__init__("aurora timing", ddl)
        self._connection: Optional[PsycopgConnection] = None
        self._cursor: Optional[PsycopgCursor] = None
        self._config: Dict[str, Any] = {}
        self._nonsilent_errs = constants.NONSILENT_ERRORS_VAR in os.environ
        self._measure_file = None
        self._wdc_stats_file = None
        self._ol_stats_file = None
        self._ins_ol_counter = 0

        if "LOG_QUERIES" in os.environ:
            query_log_file_path = cond.in_output_dir("queries.log")
            self._query_log_file = open(query_log_file_path, "w", encoding="UTF-8")
        else:
            self._query_log_file = None

    def makeDefaultConfig(self) -> Config:
        return AuroraTimingDriver.DEFAULT_CONFIG

    def loadConfig(self, config: Config) -> None:
        self._config = config
        address = self._config["host"]
        port = int(self._config["port"])
        user = self._config["user"]
        password = self._config["password"]
        database = self._config["database"]
        cstr = f"host={address} port={port} user={user} password={password} dbname={database}"
        self._connection = PsycopgConnection.connect_sync(cstr, autocommit=True)
        self._cursor = self._connection.cursor_sync()

    def loadTuples(self, tableName: str, tuples) -> None:
        # We don't support data loading directly here.
        pass

    def executeStart(self):
        assert self._cursor is not None
        # We use this callback to set the isolation level.
        logger.info("Setting isolation level to %s", self._config["isolation_level"])
        self._cursor.execute_sync(
            f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {self._config['isolation_level']}"
        )
        measure_file_path = cond.in_output_dir("aurora_timing.csv")
        self._measure_file = open(measure_file_path, "w", encoding="UTF-8")
        print(
            "init,begin,getitems,getwdc,getorder,insertorder,commit,collect,total",
            file=self._measure_file,
        )

        stats_file = cond.in_output_dir("wdc_stats.csv")
        self._wdc_stats_file = open(stats_file, "w", encoding="UTF-8")
        print("tax_rate,district,customer,total", file=self._wdc_stats_file)

        stats_file2 = cond.in_output_dir("item_stats.csv")
        self._ol_stats_file = open(stats_file2, "w", encoding="UTF-8")
        print(
            "txn_counter,init,fetch_stock,stock_prep,update_stock,ol_prep,ol_insert,ol_append,total",
            file=self._ol_stats_file,
        )

    def __del__(self):
        if self._measure_file is not None:
            self._measure_file.close()
            self._measure_file = None

        if self._wdc_stats_file is not None:
            self._wdc_stats_file.close()
            self._wdc_stats_file = None

        if self._ol_stats_file is not None:
            self._ol_stats_file.close()
            self._ol_stats_file = None

        if self._query_log_file is not None:
            self._query_log_file.close()
            self._query_log_file = None

    def doDelivery(self, params: Dict[str, Any]) -> List[Tuple[Any, ...]]:
        try:
            assert self._cursor is not None

            q = TXN_QUERIES["DELIVERY"]
            w_id = params["w_id"]
            o_carrier_id = params["o_carrier_id"]
            ol_delivery_d = params["ol_delivery_d"]

            result: List[Tuple[Any, ...]] = []
            self._cursor.execute_sync("BEGIN")
            for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1):
                self._cursor.execute_sync(q["getNewOrder"].format(d_id, w_id))
                r = self._cursor.fetchall_sync()
                if len(r) == 0:
                    ## No orders for this district: skip it. Note: This must be reported if > 1%
                    continue
                no_o_id = r[0][0]

                self._cursor.execute_sync(q["getCId"].format(no_o_id, d_id, w_id))
                r = self._cursor.fetchall_sync()
                c_id = r[0][0]

                self._cursor.execute_sync(q["sumOLAmount"].format(no_o_id, d_id, w_id))
                r = self._cursor.fetchall_sync()
                ol_total = decimal.Decimal(r[0][0])

                self._cursor.execute_sync(
                    q["deleteNewOrder"].format(d_id, w_id, no_o_id)
                )
                updateOrders = q["updateOrders"].format(
                    o_carrier_id, no_o_id, d_id, w_id
                )
                self._cursor.execute_sync(updateOrders)
                updateOrderLine = q["updateOrderLine"].format(
                    ol_delivery_d.strftime("%Y-%m-%d %H:%M:%S"), no_o_id, d_id, w_id
                )
                self._cursor.execute_sync(updateOrderLine)

                # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
                # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
                # them out
                # If there are no order lines, SUM returns null. There should always be order lines.
                assert (
                    ol_total != None
                ), "ol_total is NULL: there are no order lines. This should not happen"
                assert ol_total > 0.0

                self._cursor.execute_sync(
                    q["updateCustomer"].format(
                        ol_total.quantize(decimal.Decimal("1.00")), c_id, d_id, w_id
                    )
                )

                result.append((d_id, no_o_id))

            self._cursor.execute_sync("COMMIT")
            return result

        except Exception as ex:
            if self._nonsilent_errs:
                print("Error in DELIVERY", str(ex))
                print(traceback.format_exc())
            raise

    def doNewOrderOriginal(self, params: Dict[str, Any]) -> List[Tuple[Any, ...]]:
        try:
            assert self._cursor is not None

            no_start = time.time()
            q = TXN_QUERIES["NEW_ORDER"]
            w_id = params["w_id"]
            d_id = params["d_id"]
            c_id = params["c_id"]
            o_entry_d = params["o_entry_d"]
            i_ids = params["i_ids"]
            i_w_ids = params["i_w_ids"]
            i_qtys = params["i_qtys"]

            assert len(i_ids) > 0
            assert len(i_ids) == len(i_w_ids)
            assert len(i_ids) == len(i_qtys)

            no_pbegin = time.time()
            self._cursor.execute_sync("BEGIN")
            no_abegin = time.time()
            all_local = True
            items = []
            for i in range(len(i_ids)):
                ## Determine if this is an all local order or not
                all_local = all_local and i_w_ids[i] == w_id
                self._cursor.execute_sync(q["getItemInfo"].format(i_ids[i]))
                r = self._cursor.fetchone_sync()
                items.append(r)
            assert len(items) == len(i_ids)
            no_getitems = time.time()

            ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
            ## Note that this will happen with 1% of transactions on purpose.
            for item in items:
                if len(item) == 0:
                    self._cursor.execute_sync("ROLLBACK")
                    return
            ## FOR

            ## ----------------
            ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
            ## ----------------
            wdc_start = time.time()
            get_warehouse = q["getWarehouseTaxRate"].format(w_id)
            self._cursor.execute_sync(get_warehouse)
            r = self._cursor.fetchone_sync()
            w_tax = r[0]
            wdc_warehouse_tax_rate = time.time()

            get_district = q["getDistrict"].format(d_id, w_id)
            self._cursor.execute_sync(get_district)
            r = self._cursor.fetchone_sync()
            district_info = r
            d_tax = district_info[0]
            d_next_o_id = district_info[1]
            wdc_district = time.time()

            get_customer = q["getCustomer"].format(w_id, d_id, c_id)
            self._cursor.execute_sync(get_customer)
            r = self._cursor.fetchone_sync()
            customer_info = r
            c_discount = customer_info[0]
            no_get_wdc_info = time.time()

            if self._query_log_file is not None:
                print(get_warehouse, file=self._query_log_file)
                print(get_district, file=self._query_log_file)
                print(get_customer, file=self._query_log_file)

            ## ----------------
            ## Insert Order Information
            ## ----------------
            ol_cnt = len(i_ids)
            o_carrier_id = constants.NULL_CARRIER_ID

            self._cursor.execute_sync(
                q["incrementNextOrderId"].format(d_next_o_id + 1, d_id, w_id)
            )
            createOrder = q["createOrder"].format(
                d_next_o_id,
                d_id,
                w_id,
                c_id,
                o_entry_d.strftime("%Y-%m-%d %H:%M:%S"),
                o_carrier_id,
                ol_cnt,
                1 if all_local else 0,
            )
            self._cursor.execute_sync(createOrder)
            self._cursor.execute_sync(
                q["createNewOrder"].format(d_next_o_id, d_id, w_id)
            )
            no_ins_order_info = time.time()

            ## ----------------
            ## Insert Order Item Information
            ## ----------------
            item_data = []
            total = 0
            insert_metadata = []
            for i in range(len(i_ids)):
                io_start = time.time()
                ol_number = i + 1
                ol_supply_w_id = i_w_ids[i]
                ol_i_id = i_ids[i]
                ol_quantity = i_qtys[i]

                itemInfo = items[i]
                i_name = itemInfo[1]
                i_data = itemInfo[2]
                i_price = decimal.Decimal(itemInfo[0])
                io_init = time.time()

                get_stock_info = q["getStockInfo"].format(d_id, ol_i_id, ol_supply_w_id)
                self._cursor.execute_sync(get_stock_info)
                r = self._cursor.fetchone_sync()
                io_fetch_stock = time.time()
                if r is None:
                    logger.warning(
                        "No STOCK record for (ol_i_id=%d, ol_supply_w_id=%d)",
                        ol_i_id,
                        ol_supply_w_id,
                    )
                    continue
                stockInfo = r
                s_quantity = stockInfo[0]
                s_ytd = decimal.Decimal(stockInfo[2])
                s_order_cnt = int(stockInfo[3])
                s_remote_cnt = int(stockInfo[4])
                s_data = stockInfo[1]
                s_dist_xx = stockInfo[5]  # Fetches data from the s_dist_[d_id] column

                ## Update stock
                s_ytd += ol_quantity
                if s_quantity >= ol_quantity + 10:
                    s_quantity = s_quantity - ol_quantity
                else:
                    s_quantity = s_quantity + 91 - ol_quantity
                s_order_cnt += 1

                if ol_supply_w_id != w_id:
                    s_remote_cnt += 1
                io_stock_prep = time.time()

                update_stock = q["updateStock"].format(
                    s_quantity,
                    s_ytd.quantize(decimal.Decimal("1.00")),
                    s_order_cnt,
                    s_remote_cnt,
                    ol_i_id,
                    ol_supply_w_id,
                )
                self._cursor.execute_sync(update_stock)
                io_update_stock = time.time()

                if (
                    i_data.find(constants.ORIGINAL_STRING) != -1
                    and s_data.find(constants.ORIGINAL_STRING) != -1
                ):
                    brand_generic = "B"
                else:
                    brand_generic = "G"

                ## Transaction profile states to use "ol_quantity * i_price"
                ol_amount = ol_quantity * i_price
                total += ol_amount
                io_ol_prep = time.time()

                createOrderLine = q["createOrderLine"].format(
                    d_next_o_id,
                    d_id,
                    w_id,
                    ol_number,
                    ol_i_id,
                    ol_supply_w_id,
                    o_entry_d.strftime("%Y-%m-%d %H:%M:%S"),
                    ol_quantity,
                    ol_amount,
                    s_dist_xx,
                )
                self._cursor.execute_sync(createOrderLine)
                io_ol_insert = time.time()

                ## Add the info to be returned
                item_data.append(
                    (i_name, s_quantity, brand_generic, i_price, ol_amount)
                )
                io_ol_append = time.time()

                insert_metadata.append(
                    (
                        io_init - io_start,
                        io_fetch_stock - io_init,
                        io_stock_prep - io_fetch_stock,
                        io_update_stock - io_stock_prep,
                        io_ol_prep - io_update_stock,
                        io_ol_insert - io_ol_prep,
                        io_ol_append - io_ol_insert,
                        io_ol_append - io_start,
                    )
                )

                if self._query_log_file is not None:
                    print(get_stock_info, file=self._query_log_file)
                    print(update_stock, file=self._query_log_file)
                    print(createOrderLine, file=self._query_log_file)

            ## FOR
            no_insert_order_line = time.time()

            ## Commit!
            self._cursor.execute_sync("COMMIT")
            no_commit = time.time()

            ## Adjust the total for the discount
            # print "c_discount:", c_discount, type(c_discount)
            # print "w_tax:", w_tax, type(w_tax)
            # print "d_tax:", d_tax, type(d_tax)
            total = int(
                total
                * (1 - decimal.Decimal(c_discount))
                * (1 + decimal.Decimal(w_tax) + decimal.Decimal(d_tax))
            )

            ## Pack up values the client is missing (see TPC-C 2.4.3.5)
            misc = [(w_tax, d_tax, d_next_o_id, total)]
            no_collect = time.time()

            if self._measure_file is not None:
                init_time = no_pbegin - no_start
                begin_time = no_abegin - no_pbegin
                getitems_time = no_getitems - no_abegin
                getwdc_time = no_get_wdc_info - no_getitems
                getorder_time = no_ins_order_info - no_get_wdc_info
                insertorder_time = no_insert_order_line - no_ins_order_info
                commit_time = no_commit - no_insert_order_line
                collect_time = no_collect - no_commit
                total_time = no_collect - no_start
                print(
                    f"{init_time},{begin_time},{getitems_time},{getwdc_time},{getorder_time},{insertorder_time},{commit_time},{collect_time},{total_time}",
                    file=self._measure_file,
                )

            if self._wdc_stats_file is not None:
                tax_rate_time = wdc_warehouse_tax_rate - wdc_start
                district_time = wdc_district - wdc_warehouse_tax_rate
                customer_time = no_get_wdc_info - wdc_district
                total_time = no_get_wdc_info - wdc_start
                print(
                    f"{tax_rate_time},{district_time},{customer_time},{total_time}",
                    file=self._wdc_stats_file,
                )

            if self._ol_stats_file is not None:
                for im in insert_metadata:
                    print(
                        "{},{},{},{},{},{},{},{},{}".format(self._ins_ol_counter, *im),
                        file=self._ol_stats_file,
                    )
                self._ins_ol_counter += 1

            return [customer_info, misc, item_data]

        except Exception as ex:
            if self._nonsilent_errs:
                print("Error in NEWORDER", str(ex))
                print(traceback.format_exc())
            raise

    def doNewOrder(self, params: Dict[str, Any]) -> List[Tuple[Any, ...]]:
        try:
            assert self._cursor is not None

            no_start = time.time()
            q = TXN_QUERIES["NEW_ORDER"]
            w_id = params["w_id"]
            d_id = params["d_id"]
            c_id = params["c_id"]
            o_entry_d = params["o_entry_d"]
            i_ids = params["i_ids"]
            i_w_ids = params["i_w_ids"]
            i_qtys = params["i_qtys"]

            assert len(i_ids) > 0
            assert len(i_ids) == len(i_w_ids)
            assert len(i_ids) == len(i_qtys)

            no_pbegin = time.time()
            self._cursor.execute_sync("BEGIN")
            no_abegin = time.time()
            all_local = True
            items = []
            for i in range(len(i_ids)):
                ## Determine if this is an all local order or not
                all_local = all_local and i_w_ids[i] == w_id
                self._cursor.execute_sync(q["getItemInfo"].format(i_ids[i]))
                r = self._cursor.fetchone_sync()
                items.append(r)
            assert len(items) == len(i_ids)
            no_getitems = time.time()

            ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
            ## Note that this will happen with 1% of transactions on purpose.
            for item in items:
                if item is None or len(item) == 0:
                    self._cursor.execute_sync("ROLLBACK")
                    return
            ## FOR

            ## ----------------
            ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
            ## ----------------
            wdc_start = time.time()
            get_warehouse = q["getWarehouseTaxRate"].format(w_id)
            self._cursor.execute_sync(get_warehouse)
            r = self._cursor.fetchone_sync()
            w_tax = r[0]
            wdc_warehouse_tax_rate = time.time()

            get_district = q["getDistrict"].format(d_id, w_id)
            self._cursor.execute_sync(get_district)
            r = self._cursor.fetchone_sync()
            district_info = r
            d_tax = district_info[0]
            d_next_o_id = district_info[1]
            wdc_district = time.time()

            get_customer = q["getCustomer"].format(w_id, d_id, c_id)
            self._cursor.execute_sync(get_customer)
            r = self._cursor.fetchone_sync()
            customer_info = r
            c_discount = customer_info[0]
            no_get_wdc_info = time.time()

            if self._query_log_file is not None:
                print(get_warehouse, file=self._query_log_file)
                print(get_district, file=self._query_log_file)
                print(get_customer, file=self._query_log_file)

            ## ----------------
            ## Insert Order Information
            ## ----------------
            ol_cnt = len(i_ids)
            o_carrier_id = constants.NULL_CARRIER_ID

            self._cursor.execute_sync(
                q["incrementNextOrderId"].format(d_next_o_id + 1, d_id, w_id)
            )
            createOrder = q["createOrder"].format(
                d_next_o_id,
                d_id,
                w_id,
                c_id,
                o_entry_d.strftime("%Y-%m-%d %H:%M:%S"),
                o_carrier_id,
                ol_cnt,
                1 if all_local else 0,
            )
            self._cursor.execute_sync(createOrder)
            self._cursor.execute_sync(
                q["createNewOrder"].format(d_next_o_id, d_id, w_id)
            )
            no_ins_order_info = time.time()

            ## ----------------
            ## Insert Order Item Information
            ## ----------------
            item_data = []
            total = 0
            insert_metadata = []
            insert_value_strs = []
            for i in range(len(i_ids)):
                io_start = time.time()
                ol_number = i + 1
                ol_supply_w_id = i_w_ids[i]
                ol_i_id = i_ids[i]
                ol_quantity = i_qtys[i]

                itemInfo = items[i]
                i_name = itemInfo[1]
                i_data = itemInfo[2]
                i_price = decimal.Decimal(itemInfo[0])
                io_init = time.time()

                get_stock_info = q["getStockInfo"].format(d_id, ol_i_id, ol_supply_w_id)
                self._cursor.execute_sync(get_stock_info)
                r = self._cursor.fetchone_sync()
                io_fetch_stock = time.time()
                if r is None:
                    logger.warning(
                        "No STOCK record for (ol_i_id=%d, ol_supply_w_id=%d)",
                        ol_i_id,
                        ol_supply_w_id,
                    )
                    continue
                stockInfo = r
                s_quantity = stockInfo[0]
                s_ytd = decimal.Decimal(stockInfo[2])
                s_order_cnt = int(stockInfo[3])
                s_remote_cnt = int(stockInfo[4])
                s_data = stockInfo[1]
                s_dist_xx = stockInfo[5]  # Fetches data from the s_dist_[d_id] column

                ## Update stock
                s_ytd += ol_quantity
                if s_quantity >= ol_quantity + 10:
                    s_quantity = s_quantity - ol_quantity
                else:
                    s_quantity = s_quantity + 91 - ol_quantity
                s_order_cnt += 1

                if ol_supply_w_id != w_id:
                    s_remote_cnt += 1
                io_stock_prep = time.time()

                update_stock = q["updateStock"].format(
                    s_quantity,
                    s_ytd.quantize(decimal.Decimal("1.00")),
                    s_order_cnt,
                    s_remote_cnt,
                    ol_i_id,
                    ol_supply_w_id,
                )
                self._cursor.execute_sync(update_stock)
                io_update_stock = time.time()

                if (
                    i_data.find(constants.ORIGINAL_STRING) != -1
                    and s_data.find(constants.ORIGINAL_STRING) != -1
                ):
                    brand_generic = "B"
                else:
                    brand_generic = "G"

                ## Transaction profile states to use "ol_quantity * i_price"
                ol_amount = ol_quantity * i_price
                total += ol_amount
                io_ol_prep = time.time()

                createOrderLineValues = q["createOrderLineValues"].format(
                    d_next_o_id,
                    d_id,
                    w_id,
                    ol_number,
                    ol_i_id,
                    ol_supply_w_id,
                    o_entry_d.strftime("%Y-%m-%d %H:%M:%S"),
                    ol_quantity,
                    ol_amount,
                    s_dist_xx,
                )
                insert_value_strs.append(createOrderLineValues)
                io_ol_insert = time.time()

                ## Add the info to be returned
                item_data.append(
                    (i_name, s_quantity, brand_generic, i_price, ol_amount)
                )
                io_ol_append = time.time()

                insert_metadata.append(
                    (
                        io_init - io_start,
                        io_fetch_stock - io_init,
                        io_stock_prep - io_fetch_stock,
                        io_update_stock - io_stock_prep,
                        io_ol_prep - io_update_stock,
                        io_ol_insert - io_ol_prep,
                        io_ol_append - io_ol_insert,
                        io_ol_append - io_start,
                    )
                )

                if self._query_log_file is not None:
                    print(get_stock_info, file=self._query_log_file)
                    print(update_stock, file=self._query_log_file)

            ## FOR
            insert_order_line_query = q["createOrderLineMultivalue"] + ", ".join(
                insert_value_strs
            )
            self._cursor.execute_sync(insert_order_line_query)
            if self._query_log_file is not None:
                print(insert_order_line_query, file=self._query_log_file)
            no_insert_order_line = time.time()

            ## Commit!
            self._cursor.execute_sync("COMMIT")
            no_commit = time.time()

            ## Adjust the total for the discount
            # print "c_discount:", c_discount, type(c_discount)
            # print "w_tax:", w_tax, type(w_tax)
            # print "d_tax:", d_tax, type(d_tax)
            total = int(
                total
                * (1 - decimal.Decimal(c_discount))
                * (1 + decimal.Decimal(w_tax) + decimal.Decimal(d_tax))
            )

            ## Pack up values the client is missing (see TPC-C 2.4.3.5)
            misc = [(w_tax, d_tax, d_next_o_id, total)]
            no_collect = time.time()

            if self._measure_file is not None:
                init_time = no_pbegin - no_start
                begin_time = no_abegin - no_pbegin
                getitems_time = no_getitems - no_abegin
                getwdc_time = no_get_wdc_info - no_getitems
                getorder_time = no_ins_order_info - no_get_wdc_info
                insertorder_time = no_insert_order_line - no_ins_order_info
                commit_time = no_commit - no_insert_order_line
                collect_time = no_collect - no_commit
                total_time = no_collect - no_start
                print(
                    f"{init_time},{begin_time},{getitems_time},{getwdc_time},{getorder_time},{insertorder_time},{commit_time},{collect_time},{total_time}",
                    file=self._measure_file,
                )

            if self._wdc_stats_file is not None:
                tax_rate_time = wdc_warehouse_tax_rate - wdc_start
                district_time = wdc_district - wdc_warehouse_tax_rate
                customer_time = no_get_wdc_info - wdc_district
                total_time = no_get_wdc_info - wdc_start
                print(
                    f"{tax_rate_time},{district_time},{customer_time},{total_time}",
                    file=self._wdc_stats_file,
                )

            if self._ol_stats_file is not None:
                for im in insert_metadata:
                    print(
                        "{},{},{},{},{},{},{},{},{}".format(self._ins_ol_counter, *im),
                        file=self._ol_stats_file,
                    )
                self._ins_ol_counter += 1

            return [customer_info, misc, item_data]

        except Exception as ex:
            if self._nonsilent_errs:
                print("Error in NEWORDER", str(ex))
                print(traceback.format_exc())
            raise

    def doOrderStatus(self, params: Dict[str, Any]) -> List[Tuple[Any, ...]]:
        try:
            assert self._cursor is not None

            q = TXN_QUERIES["ORDER_STATUS"]
            w_id = params["w_id"]
            d_id = params["d_id"]
            c_id = params["c_id"]
            c_last = params["c_last"]

            self._cursor.execute_sync("BEGIN")
            if c_id != None:
                self._cursor.execute_sync(
                    q["getCustomerByCustomerId"].format(w_id, d_id, c_id)
                )
                r = self._cursor.fetchall_sync()
                customer = r[0]
            else:
                # Get the midpoint customer's id
                self._cursor.execute_sync(
                    q["getCustomersByLastName"].format(w_id, d_id, c_last)
                )
                r = self._cursor.fetchall_sync()
                all_customers = r
                assert len(all_customers) > 0
                namecnt = len(all_customers)
                index = (namecnt - 1) // 2
                customer = all_customers[index]
                c_id = customer[0]
            assert len(customer) > 0
            assert c_id != None

            getLastOrder = q["getLastOrder"].format(w_id, d_id, c_id)
            self._cursor.execute_sync(getLastOrder)
            r = self._cursor.fetchall_sync()
            order = r[0]
            if order:
                self._cursor.execute_sync(
                    q["getOrderLines"].format(w_id, d_id, order[0])
                )
                r = self._cursor.fetchall_sync()
                orderLines = r
            else:
                orderLines = []

            self._cursor.execute_sync("COMMIT")
            return [customer, order, orderLines]

        except Exception as ex:
            if self._nonsilent_errs:
                print("Error in ORDER_STATUS", str(ex))
                print(traceback.format_exc())
            raise

    def doPayment(self, params: Dict[str, Any]) -> List[Tuple[Any, ...]]:
        try:
            assert self._cursor is not None

            q = TXN_QUERIES["PAYMENT"]
            w_id = params["w_id"]
            d_id = params["d_id"]
            h_amount = decimal.Decimal(params["h_amount"])
            c_w_id = params["c_w_id"]
            c_d_id = params["c_d_id"]
            c_id = params["c_id"]
            c_last = params["c_last"]
            h_date = params["h_date"]  # Python datetime

            self._cursor.execute_sync("BEGIN")
            if c_id != None:
                self._cursor.execute_sync(
                    q["getCustomerByCustomerId"].format(w_id, d_id, c_id)
                )
                r = self._cursor.fetchall_sync()
                customer = r[0]
            else:
                # Get the midpoint customer's id
                self._cursor.execute_sync(
                    q["getCustomersByLastName"].format(w_id, d_id, c_last)
                )
                r = self._cursor.fetchall_sync()
                all_customers = r
                assert len(all_customers) > 0
                namecnt = len(all_customers)
                index = (namecnt - 1) // 2
                customer = all_customers[index]
                c_id = customer[0]
            assert len(customer) > 0
            c_balance = decimal.Decimal(customer[14]) - h_amount
            c_ytd_payment = decimal.Decimal(customer[15]) + h_amount
            c_payment_cnt = int(customer[16]) + 1
            c_data = customer[17]

            self._cursor.execute_sync(q["getWarehouse"].format(w_id))
            r = self._cursor.fetchall_sync()
            warehouse = r[0]

            self._cursor.execute_sync(q["getDistrict"].format(w_id, d_id))
            r = self._cursor.fetchall_sync()
            district = r[0]

            self._cursor.execute_sync(
                q["updateWarehouseBalance"].format(h_amount, w_id)
            )
            self._cursor.execute_sync(
                q["updateDistrictBalance"].format(h_amount, w_id, d_id)
            )

            # Customer Credit Information
            if customer[11] == constants.BAD_CREDIT:
                newData = " ".join(
                    map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount])
                )
                c_data = newData + "|" + c_data
                if len(c_data) > constants.MAX_C_DATA:
                    c_data = c_data[: constants.MAX_C_DATA]
                updateCustomer = q["updateBCCustomer"].format(
                    c_balance,
                    c_ytd_payment,
                    c_payment_cnt,
                    c_data,
                    c_w_id,
                    c_d_id,
                    c_id,
                )
                self._cursor.execute_sync(updateCustomer)
            else:
                c_data = ""
                self._cursor.execute_sync(
                    q["updateGCCustomer"].format(
                        c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id
                    ),
                )

            # Concatenate w_name, four spaces, d_name
            h_data = "%s    %s" % (warehouse[0], district[0])
            # Create the history record
            insertHistory = q["insertHistory"].format(
                c_id,
                c_d_id,
                c_w_id,
                d_id,
                w_id,
                h_date.strftime("%Y-%m-%d %H:%M:%S"),
                h_amount.quantize(decimal.Decimal("1.00")),
                h_data,
            )
            self._cursor.execute_sync(insertHistory)

            self._cursor.execute_sync("COMMIT")

            # TPC-C 2.5.3.3: Must display the following fields:
            # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
            # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
            # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
            # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
            # H_AMOUNT, and H_DATE.

            # Hand back all the warehouse, district, and customer data
            return [warehouse, district, customer]

        except Exception as ex:
            if self._nonsilent_errs:
                print("Error in PAYMENT", str(ex))
                print(traceback.format_exc())
            raise

    def doStockLevel(self, params: Dict[str, Any]) -> int:
        try:
            assert self._cursor is not None

            q = TXN_QUERIES["STOCK_LEVEL"]
            w_id = params["w_id"]
            d_id = params["d_id"]
            threshold = params["threshold"]

            self._cursor.execute_sync("BEGIN")
            self._cursor.execute_sync(q["getOId"].format(w_id, d_id))
            r = self._cursor.fetchall_sync()
            result = r[0]
            assert result
            o_id = result[0]

            self._cursor.execute_sync(
                q["getStockCount"].format(
                    w_id, d_id, o_id, (o_id - 20), w_id, threshold
                )
            )
            r = self._cursor.fetchall_sync()
            result = r[0]

            self._cursor.execute_sync("COMMIT")
            return int(result[0])

        except Exception as ex:
            if self._nonsilent_errs:
                print("Error in STOCK_LEVEL", str(ex))
                print(traceback.format_exc())
            raise

    def ensureRollback(self) -> None:
        """
        Makes sure the transaction has rolled back.
        """
        self._cursor.execute_sync("ROLLBACK")
