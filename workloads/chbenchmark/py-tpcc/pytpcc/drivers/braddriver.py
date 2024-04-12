import logging
from typing import Dict, Tuple, Any, Optional, List

from abstractdriver import *
import constants

from brad.grpc_client import BradGrpcClient

Config = Dict[str, Tuple[str, Any]]

logger = logging.getLogger(__name__)


TXN_QUERIES = {
    "DELIVERY": {
        "getNewOrder": "SELECT no_o_id FROM new_order WHERE no_d_id = {} AND no_w_id = {} AND no_o_id > -1 LIMIT 1",  #
        "deleteNewOrder": "DELETE FROM new_order WHERE no_d_id = {} AND no_w_id = {} AND no_o_id = {}",  # d_id, w_id, no_o_id
        "getCId": "SELECT o_c_id FROM orders WHERE o_id = {} AND o_d_id = {} AND o_w_id = {}",  # no_o_id, d_id, w_id
        "updateOrders": "UPDATE orders SET o_carrier_id = {} WHERE o_id = {} AND o_d_id = {} AND o_w_id = {}",  # o_carrier_id, no_o_id, d_id, w_id
        "updateOrderLine": "UPDATE order_line SET ol_delivery_d = {} WHERE ol_o_id = {} AND ol_d_id = {} AND ol_w_id = {}",  # o_entry_d, no_o_id, d_id, w_id
        "sumOLAmount": "SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = {} AND ol_d_id = {} AND ol_w_id = {}",  # no_o_id, d_id, w_id
        "updateCustomer": "UPDATE customer SET c_balance = c_balance + {} WHERE c_id = {} AND c_d_id = {} AND c_w_id = {}",  # ol_total, c_id, d_id, w_id
    },
    "NEW_ORDER": {
        "getWarehouseTaxRate": "SELECT w_tax FROM warehouse WHERE w_id = {}",  # w_id
        "getDistrict": "SELECT d_tax, d_next_o_id FROM district WHERE d_id = {} AND d_w_id = {}",  # d_id, w_id
        "incrementNextOrderId": "UPDATE district SET d_next_o_id = {} WHERE d_id = {} AND d_w_id = {}",  # d_next_o_id, d_id, w_id
        "getCustomer": "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",  # w_id, d_id, c_id
        "createOrder": "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES ({}, {}, {}, {}, {}, {}, {}, {})",  # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
        "createNewOrder": "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES ({}, {}, {})",  # o_id, d_id, w_id
        "getItemInfo": "SELECT i_price, i_name, i_data FROM item WHERE i_id = {}",  # ol_i_id
        "getStockInfo": "SELECT s_quantity, s_data, s_ytd, s_order_cnt, s_remote_cnt, s_dist_{:02d} FROM stock WHERE s_i_id = {} AND s_w_id = {}",  # d_id, ol_i_id, ol_supply_w_id
        "updateStock": "UPDATE stock SET s_quantity = {}, s_ytd = {}, s_order_cnt = {}, s_remote_cnt = {} WHERE s_i_id = {} AND s_w_id = {}",  # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
        "createOrderLine": "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {})",  # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
    },
    "ORDER_STATUS": {
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?",  # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST",  # w_id, d_id, c_last
        "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1",  # w_id, d_id, c_id
        "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?",  # w_id, d_id, o_id
    },
    "PAYMENT": {
        "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?",  # w_id
        "updateWarehouseBalance": "UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?",  # h_amount, w_id
        "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?",  # w_id, d_id
        "updateDistrictBalance": "UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID  = ? AND D_ID = ?",  # h_amount, d_w_id, d_id
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?",  # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST",  # w_id, d_id, c_last
        "updateBCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?",  # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id
        "updateGCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?",  # c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id
        "insertHistory": "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    },
    "STOCK_LEVEL": {
        "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?",
        "getStockCount": """
            SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
            WHERE OL_W_ID = ?
              AND OL_D_ID = ?
              AND OL_O_ID < ?
              AND OL_O_ID >= ?
              AND S_W_ID = ?
              AND S_I_ID = OL_I_ID
              AND S_QUANTITY < ?
        """,
    },
}


class BradDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "host": ("Host running the BRAD front end.", "localhost"),
        "port": ("Port on which the BRAD front end is listening.", 6583),
    }

    def __init__(self, ddl: str) -> None:
        super().__init__("BradDriver", ddl)
        self._client: Optional[BradGrpcClient] = None

    def makeDefaultConfig(self) -> Config:
        return BradDriver.DEFAULT_CONFIG

    def loadConfig(self, config: Config) -> None:
        self._client = BradGrpcClient(host=config["host"], port=config["port"])
        self._client.connect()

    def loadTuples(self, tableName: str, tuples) -> None:
        # We don't support data loading directly here.
        pass

    def doDelivery(self, params: Dict[str, Any]) -> List[Tuple[Any, ...]]:
        assert self._client is not None

        q = TXN_QUERIES["DELIVERY"]
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]

        result = []
        self._client.run_query_json("BEGIN")
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1):
            r, _ = self._client.run_query_json(q["getNewOrder"].format(d_id, w_id))
            if len(r) == 0:
                ## No orders for this district: skip it. Note: This must be reported if > 1%
                continue
            no_o_id = r[0][0]

            r, _ = self._client.run_query_json(q["getCId"].format(no_o_id, d_id, w_id))
            c_id = r[0][0]

            r, _ = self._client.run_query_json(
                q["sumOLAmount"].format(no_o_id, d_id, w_id)
            )
            ol_total = r[0][0]

            self._client.run_query_json(q["deleteNewOrder"].format(d_id, w_id, no_o_id))
            self._client.run_query_json(
                q["updateOrders"].format(o_carrier_id, no_o_id, d_id, w_id)
            )
            self._client.run_query_json(
                q["updateOrderLine"].format(ol_delivery_d, no_o_id, d_id, w_id)
            )

            # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
            # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
            # them out
            # If there are no order lines, SUM returns null. There should always be order lines.
            assert (
                ol_total != None
            ), "ol_total is NULL: there are no order lines. This should not happen"
            assert ol_total > 0.0

            self._client.run_query_json(
                q["updateCustomer"].format(ol_total, c_id, d_id, w_id)
            )

            result.append((d_id, no_o_id))

        self._client.run_query_json("COMMIT")
        return result

    def doNewOrder(self, params: Dict[str, Any]) -> List[Tuple[Any, ...]]:
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

        r, _ = self._client.run_query_json(q["getCustomer"].format(w_id, d_id, c_id))
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
        self._client.run_query_json(
            q["createOrder"].format(
                d_next_o_id,
                d_id,
                w_id,
                c_id,
                o_entry_d,
                o_carrier_id,
                ol_cnt,
                all_local,
            ),
        )
        self._client.run_query_json(q["createNewOrder"].format(d_next_o_id, d_id, w_id))

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
            i_price = itemInfo[0]

            self._client.run_query_json(
                q["getStockInfo"].format(d_id, ol_i_id, ol_supply_w_id)
            )
            stockInfo = self.cursor.fetchone()
            if len(stockInfo) == 0:
                logger.warning(
                    "No STOCK record for (ol_i_id=%d, ol_supply_w_id=%d)",
                    ol_i_id,
                    ol_supply_w_id,
                )
                continue
            s_quantity = stockInfo[0]
            s_ytd = stockInfo[2]
            s_order_cnt = stockInfo[3]
            s_remote_cnt = stockInfo[4]
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
                    s_ytd,
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

            self._client.run_query_json(
                q["createOrderLine"].format(
                    d_next_o_id,
                    d_id,
                    w_id,
                    ol_number,
                    ol_i_id,
                    ol_supply_w_id,
                    o_entry_d,
                    ol_quantity,
                    ol_amount,
                    s_dist_xx,
                ),
            )

            ## Add the info to be returned
            item_data.append((i_name, s_quantity, brand_generic, i_price, ol_amount))
        ## FOR

        ## Commit!
        self._client.run_query_json("COMMIT")

        ## Adjust the total for the discount
        # print "c_discount:", c_discount, type(c_discount)
        # print "w_tax:", w_tax, type(w_tax)
        # print "d_tax:", d_tax, type(d_tax)
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        ## Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = [(w_tax, d_tax, d_next_o_id, total)]

        return [customer_info, misc, item_data]
