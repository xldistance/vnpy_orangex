import base64
import csv
import hashlib
import hmac
import json
from collections import defaultdict
from copy import copy
from datetime import datetime, timedelta, timezone
from enum import Enum
from inspect import signature
from pathlib import Path
from threading import Lock
from time import sleep, time
from typing import Any, Dict, List
from urllib.parse import urlencode

from peewee import chunked
from vnpy.api.rest import Request, RestClient
from vnpy.api.websocket import WebsocketClient
from vnpy.event import Event
from vnpy.event.engine import EventEngine
from vnpy.trader.constant import Direction, Exchange, Interval, Offset, Status
from vnpy.trader.database import database_manager
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    AccountData,
    BarData,
    CancelRequest,
    ContractData,
    HistoryRequest,
    OrderData,
    OrderRequest,
    OrderType,
    PositionData,
    Product,
    SubscribeRequest,
    TickData,
    TradeData,
)
from vnpy.trader.setting import orangex_account  # 导入账户字典
from vnpy.trader.utility import (
    TZ_INFO,
    GetFilePath,
    delete_dr_data,
    extract_vt_symbol,
    get_symbol_mark,
    get_local_datetime,
    get_uuid,
    is_target_contract,
    load_json,
    remain_alpha,
    remain_digit,
    save_connection_status,
    save_json,
)

recording_list = GetFilePath.recording_list

# REST API地址
REST_HOST: str = "https://api.orangex.com/api/v1"

# Websocket API地址
WEBSOCKET_HOST: str = "wss://api.orangex.com/ws/api/v1"

# 委托类型映射
ORDERTYPE_VT2ORANGEX = {OrderType.LIMIT: "limit", OrderType.MARKET: "market"}

ORDERTYPE_ORANGEX2VT = {v: k for k, v in ORDERTYPE_VT2ORANGEX.items()}

# 买卖方向映射
DIRECTION_VT2ORANGEX = {
    Direction.LONG: "buy",
    Direction.SHORT: "sell",
}
DIRECTION_ORANGEX2VT = {v: k for k, v in DIRECTION_VT2ORANGEX.items()}

# 委托单状态映射
STATUS_ORANGEX2VT = {
    "open": Status.NOTTRADED,
    "filled": Status.ALLTRADED,
    "rejected": Status.REJECTED,
    "cancelled": Status.CANCELLED,
    "canceled": Status.CANCELLED,
}
# 多空反向映射
OPPOSITE_DIRECTION = {
    Direction.LONG: Direction.SHORT,
    Direction.SHORT: Direction.LONG,
}
TIME_IN_FORCE_VT2ORANGEX = {
    OrderType.LIMIT:"good_til_cancelled",
    OrderType.FAK:"fill_or_kill",
    OrderType.FOK:"immediate_or_cancel",
}

# 鉴权类型
class Security(Enum):
    NONE: int = 0
    SIGNED: int = 1
# ----------------------------------------------------------------------------------------------------
class OrangexGateway(BaseGateway):
    """vn.py用于对接ORANGEX的交易接口"""

    default_setting: Dict[str, Any] = {
        "key": "",
        "secret": "",
        "host": "",
        "port": 0,
    }

    exchanges: Exchange = [Exchange.ORANGEX]
    # ----------------------------------------------------------------------------------------------------
    def __init__(self, event_engine: EventEngine, gateway_name: str = "ORANGEX") -> None:
        """
        构造函数
        """
        super().__init__(event_engine, gateway_name)

        self.ws_api: "OrangexWebsocketApi" = OrangexWebsocketApi(self)
        self.rest_api: "OrangexRestApi" = OrangexRestApi(self)
        self.orders: Dict[str, OrderData] = {}
        self.recording_list = [vt_symbol for vt_symbol in recording_list if is_target_contract(vt_symbol, self.gateway_name)]
        # 查询历史数据合约列表
        self.history_contracts = copy(self.recording_list)
        self.query_contracts = [vt_symbol for vt_symbol in GetFilePath.all_trading_vt_symbols if is_target_contract(vt_symbol, self.gateway_name)]
        self.query_functions = [self.query_account, self.query_order, self.query_position]
        # 查询历史数据状态
        self.history_status = True
        # 订阅逐笔成交数据状态
        self.book_trade_status: bool = False
        self.count:int = 0
    # ----------------------------------------------------------------------------------------------------
    def connect(self, log_account: dict = {}) -> None:
        """
        连接交易接口
        """
        if not log_account:
            log_account = orangex_account
        key: str = log_account["key"]
        secret: str = log_account["secret"]
        proxy_host: str = log_account["host"]
        proxy_port: int = log_account["port"]
        self.account_file_name = log_account["account_file_name"]
        self.rest_api.connect(key, secret, proxy_host, proxy_port)
        self.ws_api.connect(key, secret, proxy_host, proxy_port)
        self.init_query()
    # ----------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅行情
        """
        self.ws_api.subscribe(req)
    # ----------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest) -> str:
        """
        委托下单
        """
        return self.rest_api.send_order(req)
    # ----------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest) -> None:
        """
        委托撤单
        """
        self.rest_api.cancel_order(req)
    # ----------------------------------------------------------------------------------------------------
    def query_account(self) -> None:
        """
        查询资金
        """
        self.rest_api.query_account()
    # ----------------------------------------------------------------------------------------------------
    def query_position(self) -> None:
        """
        查询持仓
        """
        self.rest_api.query_position()
    # ----------------------------------------------------------------------------------------------------
    def query_order(self) -> None:
        """
        查询活动委托单
        """
        self.rest_api.query_order()
    # ----------------------------------------------------------------------------------------------------
    def on_order(self, order: OrderData) -> None:
        """
        推送委托数据
        """
        self.orders[order.orderid] = copy(order)
        super().on_order(order)
    # ----------------------------------------------------------------------------------------------------
    def get_order(self, orderid: str) -> OrderData:
        """
        查询委托数据
        """
        return self.orders.get(orderid, None)
    # ----------------------------------------------------------------------------------------------------
    def query_history(self, event: Event):
        """
        查询合约历史数据
        """
        if len(self.history_contracts) > 0:
            symbol, exchange, gateway_name = extract_vt_symbol(self.history_contracts.pop(0))
            req = HistoryRequest(
                symbol=symbol,
                exchange=exchange,
                interval=Interval.MINUTE,
                start=datetime.now(TZ_INFO) - timedelta(minutes=1440),
                end=datetime.now(TZ_INFO),
                gateway_name=self.gateway_name,
            )
            self.rest_api.query_history(req)
            self.rest_api.set_margin_type(symbol)
            self.rest_api.set_leverage(symbol)
    # ----------------------------------------------------------------------------------------------------
    def process_timer_event(self, event) -> None:
        """
        处理定时事件
        """
        # rest api未获取到更新token直接返回
        if not self.rest_api.refresh_token:
            return
        function = self.query_functions.pop(0)
        function()
        self.query_functions.append(function)
        # 一小时更新一次有效token
        self.count += 1
        if self.count < 3600:
            return
        self.count = 0
        #self.rest_api.update_token()
    # ----------------------------------------------------------------------------------------------------
    def init_query(self):
        """
        """
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        if self.history_status:
            self.event_engine.register(EVENT_TIMER, self.query_history)
    # ----------------------------------------------------------------------------------------------------
    def close(self) -> None:
        """
        关闭连接
        """
        self.rest_api.stop()
        self.ws_api.stop()
# ----------------------------------------------------------------------------------------------------
class OrangexRestApi(RestClient):
    """
    ORANGEX交易所REST API
    """
    # ----------------------------------------------------------------------------------------------------
    def __init__(self, gateway: OrangexGateway) -> None:
        """
        构造函数
        """
        super().__init__()

        self.gateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.ws_api: OrangexWebsocketApi = self.gateway.ws_api

        # 保存用户登陆信息
        self.key: str = ""
        self.secret: str = ""
        self.sign_count = 0
        # refresh_token用于延长有效token
        self.refresh_token = ""
        # 生成委托单号加线程锁
        self.order_count: int = 0
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0
        self.ticks: Dict[str, TickData] = self.gateway.ws_api.ticks
        self.account_date = None  # 账户日期
        self.accounts_info: Dict[str, dict] = {}
        # 账户查询币种
        self.currencies = ["XBT", "USDT"]
        # rest api令牌
        self.access_token = ""
        # 用户自定义委托单id和交易所委托单id映射
        self.orderid_map: Dict[str, str] = defaultdict(str)
    # ----------------------------------------------------------------------------------------------------
    def sign(self, request: Request) -> Request:
        """
        生成ORANGEX签名
        """
        self.sign_count += 1
        # 获取鉴权类型并将其从data中删除
        security = request.data.pop("security")
        request_data = {
            "jsonrpc": "2.0",
            "id": self.sign_count,
            "method": request.path,
            "params": request.data
        }
        request.data = request_data
        request.headers = {"Content-Type": "application/json"}
        # 使用websocket api的access_token认证
        token = self.gateway.ws_api.access_token
        while not token:
            sleep(1)
            token = self.gateway.ws_api.access_token
        if security == Security.SIGNED:
            request.headers.update(
                {
                    "Authorization": f"Bearer {token}",
                }
            )
        return request
    # ----------------------------------------------------------------------------------------------------
    def connect(
        self,
        key: str,
        secret: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """
        连接REST服务器
        """
        self.key = key
        self.secret = secret.encode()
        self.connect_time = int(datetime.now().strftime("%Y%m%d%H%M%S"))
        self.init(REST_HOST, proxy_host, proxy_port, gateway_name=self.gateway_name)
        self.start()
        self.gateway.write_log(f"交易接口：{self.gateway_name}，REST API启动成功")
        #self.login()
        self.query_contract()
    # ----------------------------------------------------------------------------------------------------
    def login(self):
        """
        REST API签名
        """
        path = "/public/auth"
        data = {
            "security": Security.SIGNED,
            "grant_type": "client_credentials",
            "client_id": self.key,
            "client_secret": self.secret
        }
        self.add_request(
            method="POST",
            path=path,
            callback=self.on_login,
            data=data,
        )
    # ----------------------------------------------------------------------------------------------------
    def on_login(self,data: dict, request: Request):
        """
        收到rest api签名成功回报
        """
        self.refresh_token = data['result']["refresh_token"]
        self.access_token = data['result']["access_token"]
    # ----------------------------------------------------------------------------------------------------    
    def update_token(self):
        """
        更新有效token
        """
        path = "/public/auth"
        data = {
            "security": Security.SIGNED,
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
        }
        self.add_request(
            method="POST",
            path=path,
            callback=self.on_update_token,
            data=data,
        )
    # ----------------------------------------------------------------------------------------------------
    def on_update_token(self,data: dict, request: Request):
        """
        收到更新token回报
        """
        self.refresh_token = data['result']["refresh_token"]
        self.access_token = data['result']["access_token"]
    # ----------------------------------------------------------------------------------------------------
    def query_account(self) -> None:
        """
        查询资金
        """
        path: str = "/private/get_assets_info"
        data = {
            "security": Security.SIGNED,
            "asset_type":["ALL"],
        }
        self.add_request(method="POST", path=path, callback=self.on_query_account, data=data)
    # ----------------------------------------------------------------------------------------------------
    def set_margin_type(self,symbol:str):
        """
        修改持仓类型，全仓(cross)
        """
        path: str = "/private/adjust_perpetual_margin_type"
        data = {
            "security": Security.SIGNED,
            "instrument_name":symbol,
            "margin_type":"cross",
        }
        self.add_request(method="POST", path=path, callback=self.on_margin_type, data=data)
    # ----------------------------------------------------------------------------------------------------
    def on_margin_type(self,data:dict,request:Request):
        """
        收到修改持仓类型回报
        """
        pass
    # ----------------------------------------------------------------------------------------------------
    def set_leverage(self,symbol:str):
        """
        设置合约杠杆
        """
        path: str = "/private/adjust_perpetual_leverage"
        data = {
            "security": Security.SIGNED,
            "instrument_name":symbol,
            "leverage":"20",
        }
        self.add_request(method="POST", path=path, callback=self.on_leverage, data=data)
    # ----------------------------------------------------------------------------------------------------
    def on_leverage(self,data:dict,request:Request):
        """
        收到杠杆数据回报
        """
        pass
    # ----------------------------------------------------------------------------------------------------
    def query_position(self) -> None:
        """
        查询持仓
        """
        data: dict = {"security": Security.SIGNED,"currency":"PERPETUAL"}
        path: str = "/private/get_positions"
        self.add_request(
            method="POST",
            path=path,
            callback=self.on_query_position,
            data=data,
        )
    # ----------------------------------------------------------------------------------------------------
    def query_order(self) -> None:
        """
        查询活动委托单
        """
        data: dict = {
            "security": Security.SIGNED,
            "currency":"PERPETUAL"
            }
        path: str = "/private/get_open_orders_by_currency"

        self.add_request(
            method="POST",
            path=path,
            callback=self.on_query_order,
            data=data,
        )
    # ----------------------------------------------------------------------------------------------------
    def query_contract(self) -> None:
        """
        查询合约信息
        """
        data: dict = {
            "security": Security.NONE,
            "currency":"PERPETUAL",
            }
        path: str = "/public/get_instruments"

        self.add_request(method="POST", path=path, callback=self.on_query_contract, data=data)
    # ----------------------------------------------------------------------------------------------------
    def _new_order_id(self) -> int:
        """
        生成本地委托号
        """
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count
    # ----------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest) -> str:
        """
        委托下单，orderid不能使用特殊符号
        """
        # 生成本地委托号
        orderid: str = str(self.connect_time + self._new_order_id())

        # 推送提交中事件
        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)
        if req.direction == Direction.LONG:
            path = "/private/buy"
        elif req.direction == Direction.SHORT:
            path = "/private/sell"
        data: dict = {
            "security": Security.SIGNED,
            "instrument_name": req.symbol,
            "side": DIRECTION_VT2ORANGEX[req.direction],
            "price": str(req.price),
            "amount": str(req.volume),
            "type": ORDERTYPE_VT2ORANGEX[req.type],
            "custom_order_id": orderid,
            "time_in_force":TIME_IN_FORCE_VT2ORANGEX[req.type]
        }
        if req.offset == Offset.CLOSE:
            data["reduce_only"] = True
        else:
            data["reduce_only"] = False
        self.add_request(
            method="POST",
            path=path,
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed,
        )
        return order.vt_orderid
    # ----------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest) -> None:
        """
        委托撤单
        必须用交易所订单编号撤单
        """
        gateway_id = self.orderid_map[req.orderid]
        if not gateway_id:
            gateway_id = req.orderid
        data: dict = {"security": Security.SIGNED,"order_id":gateway_id}
        path: str = "/private/cancel"

        order: OrderData = self.gateway.get_order(req.orderid)
        self.add_request(method="POST", path=path, callback=self.on_cancel_order, data=data, on_failed=self.on_cancel_failed, extra=order)
    # ----------------------------------------------------------------------------------------------------
    def on_query_account(self, data: dict, request: Request) -> None:
        """
        资金查询回报
        """
        asset = data["result"]["PERPETUAL"]
        account: AccountData = AccountData(
            accountid="USDT" + "_" + self.gateway_name,
            balance=float(asset["wallet_balance"]),
            available=float(asset["available_funds"]),
            position_profit=float(asset["total_upl"]),
            close_profit=float(asset["total_pl"]),
            datetime=datetime.now(TZ_INFO),
            file_name=self.gateway.account_file_name,
            gateway_name=self.gateway_name,
        )
        account.frozen = account.balance - account.available
        if account.balance:
            self.gateway.on_account(account)
            # 保存账户资金信息
            self.accounts_info[account.accountid] = account.__dict__

        if not self.accounts_info:
            return
        accounts_info = list(self.accounts_info.values())
        account_date = accounts_info[-1]["datetime"].date()
        account_path = str(GetFilePath.ctp_account_path).replace("ctp_account_main", self.gateway.account_file_name)
        write_header = not Path(account_path).exists()
        additional_writing = self.account_date and self.account_date != account_date
        self.account_date = account_date
        # 文件不存在则写入文件头，否则只在日期变更后追加写入文件
        if not write_header and not additional_writing:
            return
        write_mode = "w" if write_header else "a"
        for account_data in accounts_info:
            with open(account_path, write_mode, newline="") as f1:
                w1 = csv.DictWriter(f1, list(account_data))
                if write_header:
                    w1.writeheader()
                w1.writerow(account_data)
    # ----------------------------------------------------------------------------------------------------
    def on_query_position(self, data: dict, request: Request) -> None:
        """
        持仓查询回报
        """
        for raw in data["result"]:
            direction = raw["direction"]
            if direction == "zero":
                position_1 = PositionData(
                    symbol=raw["instrument_name"],
                    exchange=Exchange.ORANGEX,
                    gateway_name=self.gateway_name,
                    direction=Direction.LONG,
                    volume=0,
                    price=0,
                    pnl=0,
                )
            else:
                position_1: PositionData = PositionData(
                    symbol=raw["instrument_name"],
                    exchange=Exchange.ORANGEX,
                    direction=DIRECTION_ORANGEX2VT[raw["direction"]],
                    volume=abs(float(raw["size"])),
                    price=float(raw["average_price"]),
                    pnl=float(raw["floating_profit_loss"]),
                    gateway_name=self.gateway_name,
                )
            position_2 = PositionData(
                symbol=raw["instrument_name"],
                exchange=Exchange.ORANGEX,
                gateway_name=self.gateway_name,
                direction=OPPOSITE_DIRECTION[position_1.direction],
                volume=0,
                price=0,
                pnl=0,
            )
            self.gateway.on_position(position_1)
            self.gateway.on_position(position_2)
    # ----------------------------------------------------------------------------------------------------
    def on_query_order(self, data: dict, request: Request) -> None:
        """
        委托查询回报
        """
        data = data["result"]
        if not data:
            return
        for raw in data:
            orderid = raw["custom_order_id"]
            systemid = raw["order_id"]
            order: OrderData = OrderData(
                orderid=orderid if orderid not in ["-",""] else systemid,
                symbol=raw["instrument_name"],
                exchange=Exchange.ORANGEX,
                price=float(raw["price"]),
                volume=float(raw["amount"]),
                type=ORDERTYPE_ORANGEX2VT[raw["order_type"]],
                direction=DIRECTION_ORANGEX2VT[raw["direction"]],
                traded=float(raw["filled_amount"]),
                status=STATUS_ORANGEX2VT[raw["order_state"]],
                datetime=get_local_datetime(raw["creation_timestamp"]),
                gateway_name=self.gateway_name,
            )
            # 更新部分成交委托状态
            if order.status not in [Status.CANCELLED,Status.REJECTED]:
                if order.traded and order.traded < order.volume:
                    order.status = Status.PARTTRADED
            # 调整市价单成交价格
            if order.price < 0:
                order.price = float(raw["average_price"])
            self.orderid_map[orderid] = systemid
            if raw["reduce_only"]:
                order.offset = Offset.CLOSE
            self.gateway.on_order(order)
    # ----------------------------------------------------------------------------------------------------
    def on_query_contract(self, data: dict, request: Request):
        """
        合约信息查询回报
        """
        for raw in data["result"]:
            contract: ContractData = ContractData(
                symbol=raw["instrument_name"],
                exchange=Exchange.ORANGEX,
                name=raw["instrument_name"],
                price_tick=float(raw["tick_size"]),
                size=raw["leverage"],
                min_volume=float(raw["min_trade_amount"]),
                open_commission_ratio=float(raw["taker_commission"]),
                product=Product.FUTURES,
                gateway_name=self.gateway_name,
            )
            self.gateway.on_contract(contract)
        self.gateway.write_log(f"交易接口：{self.gateway_name}，合约信息查询成功")
    # ----------------------------------------------------------------------------------------------------
    def on_send_order(self, data: dict, request: Request) -> None:
        """
        委托下单回报
        """
        if "error" in data:
            msg = data["error"]
            order: OrderData = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)
            self.gateway.write_log(f"合约：{order.vt_symbol}委托失败，状态码：{msg['code']}，信息：{msg}")
            return
        data = data["result"]
        if "order" in data:
            id_map = data["order"]
            self.orderid_map[id_map["custom_order_id"]] = id_map["order_id"]
    # ----------------------------------------------------------------------------------------------------
    def on_send_order_error(self, exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        """
        委托下单回报函数报错回报
        """
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)
    # ----------------------------------------------------------------------------------------------------
    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """
        委托下单失败服务器报错回报
        """
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)
        msg: str = "委托失败，状态码：{0}，信息：{1}".format(status_code, request.response.text)
        self.gateway.write_log(msg)
    # ----------------------------------------------------------------------------------------------------
    def on_cancel_order(self, status_code: str, request: Request) -> None:
        """
        委托撤单回报
        """
        data = request.response.json()
        if "order_id" not in data["result"]:
            order = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)
            self.gateway.write_log(f"合约：{order.symbol}撤单失败，收到的REST API请求数据：{data}")
    # ----------------------------------------------------------------------------------------------------
    def on_cancel_failed(self, status_code: str, request: Request):
        """
        撤单回报函数报错回报
        """
        if request.extra:
            order = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)
        msg = f"撤单失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)
    # ----------------------------------------------------------------------------------------------------
    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """
        查询历史数据
        """
        history = []
        limit = 200
        start_time = req.start
        time_consuming_start = time()
        # 已经获取了所有可用的历史数据或者start已经到了请求的终止时间则终止循环
        while start_time < req.end:
            end_time = start_time + timedelta(minutes=limit)
            resp = self.request("POST","/public/get_tradingview_chart_data", data={
                "security": Security.NONE,
                "instrument_name": req.symbol,
                "resolution": "1",
                "start_timestamp":int(start_time.timestamp() * 1000),
                "end_timestamp": int(end_time.timestamp() * 1000),
                }
                )

            if not resp or resp.status_code != 200:
                msg = f"标的：{req.vt_symbol}获取历史数据失败，状态码：{getattr(resp, 'status_code', '无响应')}, 信息：{getattr(resp, 'text', '')}"
                self.gateway.write_log(msg)
                break

            data = resp.json()
            if "result" not in data:
                delete_dr_data(req.symbol, self.gateway_name)
                msg = f"标的：{req.vt_symbol}获取历史数据为空，收到数据：{data}"
                self.gateway.write_log(msg)
                break

            buf = []
            for raw_data in data["result"]:
                bar = BarData(
                    symbol=req.symbol,
                    exchange=req.exchange,
                    datetime=get_local_datetime(raw_data["tick"]),
                    interval=req.interval,
                    open_price=raw_data["open"],
                    high_price=raw_data["high"],
                    low_price=raw_data["low"],
                    close_price=raw_data["close"],
                    volume=raw_data["tick"],
                    gateway_name=self.gateway_name,
                )
                buf.append(bar)

            history.extend(buf)
            start_time = end_time + timedelta(minutes=1)

        if history:
            try:
                database_manager.save_bar_data(history, False)
            except Exception as err:
                self.gateway.write_log(f"{err}")
                return

            time_consuming_end = time()
            query_time = round(time_consuming_end - time_consuming_start, 3)
            msg = f"载入{req.vt_symbol}:bar数据，开始时间：{history[0].datetime}，结束时间：{history[-1].datetime}，数据量：{len(history)}，耗时:{query_time}秒"
            self.gateway.write_log(msg)
        else:
            msg = f"未获取到合约：{req.vt_symbol}历史数据"
            self.gateway.write_log(msg)
# ----------------------------------------------------------------------------------------------------
class OrangexWebsocketApi(WebsocketClient):
    """
    ORANGEX交易所Websocket接口
    """
    # ----------------------------------------------------------------------------------------------------
    def __init__(self, gateway: OrangexGateway) -> None:
        """
        构造函数
        """
        super().__init__()

        self.gateway: OrangexGateway = gateway
        self.gateway_name: str = gateway.gateway_name
        self.ticks: Dict[str, TickData] = {}
        self.subscribed: Dict[str, SubscribeRequest] = {}
        # 成交委托号
        self.trade_id: int = 0
        self.ws_connected: bool = False
        self.ping_count: int = 0
        self.token_count: int = 0
        self.gateway.event_engine.register(EVENT_TIMER, self.send_ping)
        self.gateway.event_engine.register(EVENT_TIMER, self.update_token)
        self.func_map = {
            "ticker":self.on_ticker,
            "trades": self.on_public_trade,
            "book": self.on_depth,
            "user.changes": self.on_changes,
            }
        self.order_book_bids = defaultdict(dict)  # 订单簿买单字典
        self.order_book_asks = defaultdict(dict)  # 订单簿卖单字典
        self.access_token = ""
        self.refresh_token = ""
    # ----------------------------------------------------------------------------------------------------
    def send_ping(self, event):
        """
        发送ping
        """
        self.ping_count += 1
        if self.ping_count < 10:
            return
        self.ping_count = 0
        self.send_topics("/public/ping",{})
        # 定时清理委托单id映射
        orderid_map = self.gateway.rest_api.orderid_map
        if len(orderid_map) < 100:
            return
        orderid = list(orderid_map)[0]
        order = self.gateway.get_order(orderid)
        if not order or not order.is_active():
            if orderid in orderid_map:
                orderid_map.pop(orderid)
    # ----------------------------------------------------------------------------------------------------
    def update_token(self,event):
        """
        一小时更新一次有效token
        """
        self.token_count += 1
        if self.token_count < 3600:
            return
        self.token_count = 0
        data = {
            "jsonrpc": "2.0",
            "id": get_uuid(),
            "method": "/public/auth",
            "params": {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
            }
        }
        self.send_packet(data)
    # ----------------------------------------------------------------------------------------------------
    def connect(self, api_key: str, api_secret: str, proxy_host: str, proxy_port: int) -> None:
        """
        连接Websocket交易频道
        """
        self.api_key = api_key
        self.api_secret = api_secret

        self.init(WEBSOCKET_HOST, proxy_host, proxy_port, gateway_name=self.gateway_name)
        self.start()
    # ----------------------------------------------------------------------------------------------------
    def login(self):
        """
        WEBSOCKET API签名
        """
        # 发送身份验证请求
        request_data = {
            "jsonrpc": "2.0",
            "id": get_uuid(),
            "method": "/public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": self.api_key,
                "client_secret": self.api_secret
            }
        }
        self.send_packet(request_data)
    # ----------------------------------------------------------------------------------------------------
    def on_connected(self) -> None:
        """
        连接成功回报
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，Websocket API连接成功")
        self.login()
        for req in list(self.subscribed.values()):
            self.subscribe(req)
        self.ws_connected = True
    # ----------------------------------------------------------------------------------------------------
    def on_disconnected(self) -> None:
        """
        连接断开回报
        """
        self.ws_connected = False
        self.gateway.write_log(f"交易接口：{self.gateway_name}，Websocket API连接断开")
    # ----------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅行情
        """
        # 等待ws连接成功后再订阅行情
        while not self.ws_connected:
            sleep(1)
        self.ticks[req.symbol] = TickData(
            symbol=req.symbol,
            name=req.symbol,
            exchange=req.exchange,
            gateway_name=self.gateway_name,
            datetime=datetime.now(TZ_INFO),
        )

        self.subscribed[req.symbol] = req
        self.subscribe_topics(req)
    # ----------------------------------------------------------------------------------------------------    
    def subscribe_topics(self, req:SubscribeRequest):
        """
        订阅公共主题
        """
        topics = [
            f"book.{req.symbol}.raw",  # 订阅深度
            f"ticker.{req.symbol}.raw",       # 订阅tick
        ]
        if self.gateway.book_trade_status:
            topics.append(f"trades.{req.symbol}.raw")  # 逐笔一档深度(占用大量带宽)
        for topic in topics:
            self.send_topics("/public/subscribe",{"channels":[topic]})
    # ----------------------------------------------------------------------------------------------------
    def send_topics(self,method:str,params:dict):
        """
        发送订阅数据到websocket
        """
        self.send_packet({"jsonrpc" : "2.0","id": get_uuid(), "method": method, "params" : params})
    # ----------------------------------------------------------------------------------------------------
    def on_packet(self, packet: Any) -> None:
        """
        推送数据回报
        """
        # 过滤订阅成功回报
        if "result"  in packet:
            if "access_token" in packet["result"]:
                self.access_token = packet["result"]["access_token"]
                self.refresh_token = packet["result"]["refresh_token"]
                self.gateway.rest_api.access_token = self.access_token
                self.gateway.rest_api.refresh_token = self.refresh_token
                # WEBSOCKET API认证成功后订阅私有主题
                self.send_topics("/private/subscribe",{"channels":["user.changes.perpetual.PERPETUAL.raw"],"access_token":self.access_token})
            return
        data = packet["params"]["data"]
        channel = packet["params"]["channel"]
        if channel.startswith("user.changes"):  # 交易推送
            self.on_changes(data)
        elif channel.startswith("ticker"):  # 行情推送
            self.on_ticker(data)
        elif channel.startswith("book"):  # 深度推送
            self.on_depth(data)
        elif channel.startswith("trade"):  # 逐笔成交推送
            self.on_public_trade(data)
    # ----------------------------------------------------------------------------------------------------
    def on_ticker(self, data: dict):
        """
        收到ticker行情推送
        """
        symbol = data["instrument_name"]
        tick = self.ticks[symbol]
        tick.high = float(data["stats"]["high"])
        tick.low = float(data["stats"]["low"])
        tick.volume = float(data["stats"]["volume"])

        tick.last_price = float(data["last_price"])
        tick.bid_price_1 = float(data["best_bid_price"])
        tick.bid_volume_1 = float(data["best_bid_amount"])
        tick.ask_price_1 = float(data["best_ask_price"])
        tick.ask_volume_1 = float(data["best_ask_amount"])
        tick.open_interest = float(data["open_interest"])
        if tick.last_price:
            self.gateway.on_tick(copy(tick))
    # ----------------------------------------------------------------------------------------------------
    def on_public_trade(self, data: dict):
        """
        收到逐笔成交事件回报
        """
        data = data[0]
        symbol = data["instrument_name"]
        tick = self.ticks[symbol]
        tick.last_price = float(data["price"])
        tick.datetime = get_local_datetime(int(data["timestamp"]))
        self.gateway.on_tick(tick)
    # ----------------------------------------------------------------------------------------------------
    def on_depth(self, data: dict):
        """
        收到orderbook事件回报
        """
        symbol = data["instrument_name"]
        tick = self.ticks[symbol]
        tick.datetime = get_local_datetime(data["timestamp"])

        # 辅助函数：更新order book
        def update_order_book(order_book, data):
            for type_,price, volume in data:
                if type_ == "delete":
                    order_book.pop(float(price), None)  # 委托量为0则删除
                else:
                    order_book[float(price)] = float(volume)  # 更新或添加

        update_order_book(self.order_book_bids[symbol], data["bids"])
        update_order_book(self.order_book_asks[symbol], data["asks"])

        # 辅助函数：设置tick的价格和量
        def set_tick_attributes(tick, sorted_data, prefix):
            for index, (price, volume) in enumerate(sorted_data, start=1):
                setattr(tick, f"{prefix}_price_{index}", float(price))
                setattr(tick, f"{prefix}_volume_{index}", float(volume))         
        # 排序并更新tick
        # 买单价格从高到低排序
        sort_bids = sorted(self.order_book_bids[symbol].items(), key=lambda x: float(x[0]), reverse=True)[:50]
        # 卖单价格从低到高排序
        sort_asks = sorted(self.order_book_asks[symbol].items(), key=lambda x: float(x[0]))[:50]
        set_tick_attributes(tick, sort_bids, "bid")
        set_tick_attributes(tick, sort_asks, "ask")
        # 只保留委托簿50档数据
        if len(self.order_book_bids[symbol]) > 50 and len(self.order_book_asks[symbol]) > 50:
            self.order_book_bids[symbol].clear()
            self.order_book_asks[symbol].clear()
            for price,volume in sort_bids:
                self.order_book_bids[symbol][price] = volume
            for price,volume in sort_asks:
                self.order_book_asks[symbol][price] = volume
        # 如果有最新价格，触发tick更新
        if tick.last_price:
            self.gateway.on_tick(copy(tick))
    # ----------------------------------------------------------------------------------------------------
    def on_changes(self, data: dict):
        """
        收到交易，持仓推送
        """
        positions = data["positions"]
        orders = data["orders"]
        for raw in positions:
            direction = raw["direction"]
            if direction == "zero":
                position_1 = PositionData(
                    symbol=raw["instrument_name"],
                    exchange=Exchange.ORANGEX,
                    gateway_name=self.gateway_name,
                    direction=Direction.LONG,
                    volume=0,
                    price=0,
                    pnl=0,
                )
            else:
                position_1: PositionData = PositionData(
                    symbol=raw["instrument_name"],
                    exchange=Exchange.ORANGEX,
                    direction=DIRECTION_ORANGEX2VT[raw["direction"]],
                    volume=abs(float(raw["size"])),
                    price=float(raw["average_price"]),
                    pnl=float(raw["floating_profit_loss"]),
                    gateway_name=self.gateway_name,
                )
            position_2 = PositionData(
                symbol=raw["instrument_name"],
                exchange=Exchange.ORANGEX,
                gateway_name=self.gateway_name,
                direction=OPPOSITE_DIRECTION[position_1.direction],
                volume=0,
                price=0,
                pnl=0,
            )
            self.gateway.on_position(position_1)
            self.gateway.on_position(position_2)

        for raw in orders:
            orderid = raw["custom_order_id"]
            systemid = raw["order_id"]
            order: OrderData = OrderData(
                orderid=orderid if orderid not in ["-",""] else systemid,
                symbol=raw["instrument_name"],
                exchange=Exchange.ORANGEX,
                price=float(raw["price"]),
                volume=float(raw["amount"]),
                type= ORDERTYPE_ORANGEX2VT[raw["order_type"]],
                direction=DIRECTION_ORANGEX2VT[raw["direction"]],
                traded=float(raw["filled_amount"]),
                status=STATUS_ORANGEX2VT[raw["order_state"]],
                datetime=get_local_datetime(raw["creation_timestamp"]),
                gateway_name=self.gateway_name,
            )
            # 更新部分成交委托状态
            if order.status not in [Status.CANCELLED,Status.REJECTED]:
                if order.traded and order.traded < order.volume:
                    order.status = Status.PARTTRADED
            # 调整市价单成交价格
            if order.price < 0:
                order.price = float(raw["average_price"])
            if raw["reduce_only"]:
                order.offset = Offset.CLOSE
            self.gateway.on_order(order)
            self.gateway.rest_api.orderid_map[orderid] = systemid

            if order.traded:
                self.trade_id += 1
                trade: TradeData = TradeData(
                    symbol=order.symbol,
                    exchange=Exchange.ORANGEX,
                    orderid=order.orderid,
                    tradeid=self.trade_id,
                    direction=order.direction,
                    price=order.price,
                    volume=order.traded,
                    datetime=get_local_datetime(raw["creation_timestamp"]),
                    gateway_name=self.gateway_name,
                )
                self.gateway.on_trade(trade)
