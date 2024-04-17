import logging
import traceback
import decimal
from typing import Dict, Tuple, Any, Optional, List

from .abstractdriver import *
from .. import constants

from brad.grpc_client import BradGrpcClient

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


class BradDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "host": ("Host running the BRAD front end.", "localhost"),
        "port": ("Port on which the BRAD front end is listening.", 6583),
        "isolation_level": ("The isolation level to use.", "REPEATABLE READ"),
    }

    def __init__(self, ddl: str) -> None:
        super().__init__("brad", ddl)
        self._client: Optional[BradGrpcClient] = None
        self._config: Dict[str, Any] = {}

    def makeDefaultConfig(self) -> Config:
        return BradDriver.DEFAULT_CONFIG

    def loadConfig(self, config: Config) -> None:
        self._config = config
        self._client = BradGrpcClient(host=config["host"], port=config["port"])
        self._client.connect()

    def loadTuples(self, tableName: str, tuples) -> None:
        # We don't support data loading directly here.
        pass

    def executeStart(self):
        # We use this callback to set the isolation level.
        logger.info("Setting isolation level to %s", self._config["isolation_level"])
        self._client.run_query_ignore_results(
            f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {self._config['isolation_level']}"
        )
        return None

    def doDelivery(self, params: Dict[str, Any]) -> List[Tuple[Any, ...]]:
        try:
            assert self._client is not None

            q = TXN_QUERIES["DELIVERY"]
            w_id = params["w_id"]
            o_carrier_id = params["o_carrier_id"]
            ol_delivery_d = params["ol_delivery_d"]

            result: List[Tuple[Any, ...]] = []
            self._client.run_query_json("BEGIN")
            for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1):
                r, _ = self._client.run_query_json(q["getNewOrder"].format(d_id, w_id))
                if len(r) == 0:
                    ## No orders for this district: skip it. Note: This must be reported if > 1%
                    continue
                no_o_id = r[0][0]

                r, _ = self._client.run_query_json(
                    q["getCId"].format(no_o_id, d_id, w_id)
                )
                c_id = r[0][0]

                r, _ = self._client.run_query_json(
                    q["sumOLAmount"].format(no_o_id, d_id, w_id)
                )
                ol_total = decimal.Decimal(r[0][0])

                self._client.run_query_json(
                    q["deleteNewOrder"].format(d_id, w_id, no_o_id)
                )
                updateOrders = q["updateOrders"].format(
                    o_carrier_id, no_o_id, d_id, w_id
                )
                self._client.run_query_json(updateOrders)
                updateOrderLine = q["updateOrderLine"].format(
                    ol_delivery_d.strftime("%Y-%m-%d %H:%M:%S"), no_o_id, d_id, w_id
                )
                self._client.run_query_json(updateOrderLine)

                # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
                # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
                # them out
                # If there are no order lines, SUM returns null. There should always be order lines.
                assert (
                    ol_total != None
                ), "ol_total is NULL: there are no order lines. This should not happen"
                assert ol_total > 0.0

                self._client.run_query_json(
                    q["updateCustomer"].format(
                        ol_total.quantize(decimal.Decimal("1.00")), c_id, d_id, w_id
                    )
                )

                result.append((d_id, no_o_id))

            self._client.run_query_json("COMMIT")
            return result

        except Exception as ex:
            print("Error in DELIVERY", str(ex))
            print(traceback.format_exc())
            raise

    def doNewOrder(self, params: Dict[str, Any]) -> List[Tuple[Any, ...]]:
        try:
            assert self._client is not None

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

            self._client.run_query_json("BEGIN")
            all_local = True
            items = []
            for i in range(len(i_ids)):
                ## Determine if this is an all local order or not
                all_local = all_local and i_w_ids[i] == w_id
                r, _ = self._client.run_query_json(q["getItemInfo"].format(i_ids[i]))
                items.append(r[0])
            assert len(items) == len(i_ids)

            ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
            ## Note that this will happen with 1% of transactions on purpose.
            for item in items:
                if len(item) == 0:
                    self._client.run_query_json("ROLLBACK")
                    return
            ## FOR

            ## ----------------
            ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
            ## ----------------
            r, _ = self._client.run_query_json(q["getWarehouseTaxRate"].format(w_id))
            w_tax = r[0][0]

            r, _ = self._client.run_query_json(q["getDistrict"].format(d_id, w_id))
            district_info = r[0]
            d_tax = district_info[0]
            d_next_o_id = district_info[1]

            r, _ = self._client.run_query_json(
                q["getCustomer"].format(w_id, d_id, c_id)
            )
            customer_info = r[0]
            c_discount = customer_info[0]

            ## ----------------
            ## Insert Order Information
            ## ----------------
            ol_cnt = len(i_ids)
            o_carrier_id = constants.NULL_CARRIER_ID

            self._client.run_query_json(
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
            self._client.run_query_json(createOrder)
            self._client.run_query_json(
                q["createNewOrder"].format(d_next_o_id, d_id, w_id)
            )

            ## ----------------
            ## Insert Order Item Information
            ## ----------------
            item_data = []
            total = 0
            for i in range(len(i_ids)):
                ol_number = i + 1
                ol_supply_w_id = i_w_ids[i]
                ol_i_id = i_ids[i]
                ol_quantity = i_qtys[i]

                itemInfo = items[i]
                i_name = itemInfo[1]
                i_data = itemInfo[2]
                i_price = decimal.Decimal(itemInfo[0])

                r, _ = self._client.run_query_json(
                    q["getStockInfo"].format(d_id, ol_i_id, ol_supply_w_id)
                )
                if len(r) == 0:
                    logger.warning(
                        "No STOCK record for (ol_i_id=%d, ol_supply_w_id=%d)",
                        ol_i_id,
                        ol_supply_w_id,
                    )
                    continue
                stockInfo = r[0]
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

                self._client.run_query_json(
                    q["updateStock"].format(
                        s_quantity,
                        s_ytd.quantize(decimal.Decimal("1.00")),
                        s_order_cnt,
                        s_remote_cnt,
                        ol_i_id,
                        ol_supply_w_id,
                    ),
                )

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
                self._client.run_query_json(createOrderLine)

                ## Add the info to be returned
                item_data.append(
                    (i_name, s_quantity, brand_generic, i_price, ol_amount)
                )
            ## FOR

            ## Commit!
            self._client.run_query_json("COMMIT")

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

            return [customer_info, misc, item_data]

        except Exception as ex:
            print("Error in NEWORDER", str(ex))
            print(traceback.format_exc())
            raise

    def doOrderStatus(self, params: Dict[str, Any]) -> List[Tuple[Any, ...]]:
        try:
            assert self._client is not None

            q = TXN_QUERIES["ORDER_STATUS"]
            w_id = params["w_id"]
            d_id = params["d_id"]
            c_id = params["c_id"]
            c_last = params["c_last"]

            self._client.run_query_json("BEGIN")
            if c_id != None:
                r, _ = self._client.run_query_json(
                    q["getCustomerByCustomerId"].format(w_id, d_id, c_id)
                )
                customer = r[0]
            else:
                # Get the midpoint customer's id
                r, _ = self._client.run_query_json(
                    q["getCustomersByLastName"].format(w_id, d_id, c_last)
                )
                all_customers = r
                assert len(all_customers) > 0
                namecnt = len(all_customers)
                index = (namecnt - 1) // 2
                customer = all_customers[index]
                c_id = customer[0]
            assert len(customer) > 0
            assert c_id != None

            getLastOrder = q["getLastOrder"].format(w_id, d_id, c_id)
            r, _ = self._client.run_query_json(getLastOrder)
            order = r[0]
            if order:
                r, _ = self._client.run_query_json(
                    q["getOrderLines"].format(w_id, d_id, order[0])
                )
                orderLines = r
            else:
                orderLines = []

            self._client.run_query_json("COMMIT")
            return [customer, order, orderLines]

        except Exception as ex:
            print("Error in ORDER_STATUS", str(ex))
            print(traceback.format_exc())
            raise

    def doPayment(self, params: Dict[str, Any]) -> List[Tuple[Any, ...]]:
        try:
            assert self._client is not None

            q = TXN_QUERIES["PAYMENT"]
            w_id = params["w_id"]
            d_id = params["d_id"]
            h_amount = decimal.Decimal(params["h_amount"])
            c_w_id = params["c_w_id"]
            c_d_id = params["c_d_id"]
            c_id = params["c_id"]
            c_last = params["c_last"]
            h_date = params["h_date"]  # Python datetime

            self._client.run_query_json("BEGIN")
            if c_id != None:
                r, _ = self._client.run_query_json(
                    q["getCustomerByCustomerId"].format(w_id, d_id, c_id)
                )
                customer = r[0]
            else:
                # Get the midpoint customer's id
                r, _ = self._client.run_query_json(
                    q["getCustomersByLastName"].format(w_id, d_id, c_last)
                )
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

            r, _ = self._client.run_query_json(q["getWarehouse"].format(w_id))
            warehouse = r[0]

            r, _ = self._client.run_query_json(q["getDistrict"].format(w_id, d_id))
            district = r[0]

            self._client.run_query_json(
                q["updateWarehouseBalance"].format(h_amount, w_id)
            )
            self._client.run_query_json(
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
                self._client.run_query_json(updateCustomer)
            else:
                c_data = ""
                self._client.run_query_json(
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
            self._client.run_query_json(insertHistory)

            self._client.run_query_json("COMMIT")

            # TPC-C 2.5.3.3: Must display the following fields:
            # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
            # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
            # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
            # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
            # H_AMOUNT, and H_DATE.

            # Hand back all the warehouse, district, and customer data
            return [warehouse, district, customer]

        except Exception as ex:
            print("Error in PAYMENT", str(ex))
            print(traceback.format_exc())
            raise

    def doStockLevel(self, params: Dict[str, Any]) -> int:
        try:
            assert self._client is not None

            q = TXN_QUERIES["STOCK_LEVEL"]
            w_id = params["w_id"]
            d_id = params["d_id"]
            threshold = params["threshold"]

            self._client.run_query_json("BEGIN")
            r, _ = self._client.run_query_json(q["getOId"].format(w_id, d_id))
            result = r[0]
            assert result
            o_id = result[0]

            r, _ = self._client.run_query_json(
                q["getStockCount"].format(
                    w_id, d_id, o_id, (o_id - 20), w_id, threshold
                )
            )
            result = r[0]

            self._client.run_query_json("COMMIT")
            return int(result[0])

        except Exception as ex:
            print("Error in STOCK_LEVEL", str(ex))
            print(traceback.format_exc())
            raise
