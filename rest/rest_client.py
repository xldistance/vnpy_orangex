import sys
import traceback
from datetime import datetime
from enum import Enum
from multiprocessing.dummy import Pool
from multiprocessing import cpu_count
from queue import Empty, Queue
from typing import Any, Callable, Optional, Union, Type
from types import TracebackType
import requests
from requests.exceptions import JSONDecodeError

from vnpy.trader.utility import save_connection_status,write_log

CALLBACK_TYPE = Callable[[dict, "Request"], Any]
ON_FAILED_TYPE = Callable[[int, "Request"], Any]
ON_ERROR_TYPE = Callable[[Type, Exception, TracebackType, "Request"], Any]
Response = requests.Response
#----------------------------------------------------------------------------------------------------
class RequestStatus(Enum):
    """
    请求状态
    """

    ready = 0       # 请求已创建
    success = 1     # 请求成功(status code 2xx)
    failed = 2      # 请求失败(status code not 2xx)
    error = 3       # 请求异常
#----------------------------------------------------------------------------------------------------
class Request(object):
    """
    """
    def __init__(
        self,
        method: str,
        path: str,
        params: dict,
        data: Union[dict, str, bytes],
        headers: dict,
        callback: CALLBACK_TYPE = None,
        on_failed: ON_FAILED_TYPE = None,
        on_error: ON_ERROR_TYPE = None,
        extra: Any = None,
    ):
        """"""
        self.method: str = method
        self.path: str = path
        self.callback: CALLBACK_TYPE = callback
        self.params: dict = params
        self.data: Union[dict, str, bytes] = data
        self.headers: dict = headers

        self.on_failed: ON_FAILED_TYPE = on_failed
        self.on_error: ON_ERROR_TYPE = on_error
        self.extra: Any = extra

        self.response: requests.Response = None
        self.status: RequestStatus = RequestStatus.ready
    #----------------------------------------------------------------------------------------------------
    def __str__(self):
        """"""
        if self.response is None:
            status_code = 0
        else:
            status_code = self.response.status_code

        return (
            "request : {} {} {} because {}: \n"
            "headers: {}\n"
            "params: {}\n"
            "data: {}\n"
            "response:"
            "{}\n".format(
                self.method,
                self.path,
                self.status.name,
                status_code,
                self.headers,
                self.params,
                self.data,
                "" if self.response is None else self.response.text,
            )
        )

#----------------------------------------------------------------------------------------------------
class RestClient(object):
    """
    HTTP 客户端，专为各种交易而设计 RESTFul API。
    * 重新实现 sign 函数，添加签名功能。
    * 重新实现 on_failed 函数，以处理非 2xx 响应。
    * 在 add_request 函数中使用 on_failed 参数来处理个别非 2xx 响应。
    * 重新实现 on_error 函数，以处理异常 msg。
    """
    #----------------------------------------------------------------------------------------------------
    def __init__(self):
        """"""
        self.url_base: str = ""
        self._active: bool = False

        self._queue: Queue = Queue()
        self._pool: Pool = None

        self.proxies: dict = None
    #----------------------------------------------------------------------------------------------------
    def init(
        self,
        url_base: str,
        proxy_host: str = "",
        proxy_port: int = 0,
        gateway_name: str = "",
    ) -> None:
        """
        使用作为 API 根地址的 url_base 启动rest客户端
        """
        self.url_base = url_base
        self.gateway_name = gateway_name
        if proxy_host and proxy_port:
            proxy = f"http://{proxy_host}:{proxy_port}"
            self.proxies = {"http": proxy, "https": proxy}
        assert self.gateway_name, "请到交易接口REST API connect函数里面的self.init函数中添加gateway_name参数"
    #----------------------------------------------------------------------------------------------------
    def start(self) -> None:
        """
        启动rest客户端
        """
        if self._active:
            return

        self._active = True
        # 创建线程池,processes大小应该为CPU核心数量
        self._pool = Pool(cpu_count())
        self._pool.apply_async(self._run)
    #----------------------------------------------------------------------------------------------------
    def stop(self) -> None:
        """
        停止rest客户端
        """
        self._active = False
    #----------------------------------------------------------------------------------------------------
    def join(self) -> None:
        """
        等待所有请求处理完毕
        """
        self._queue.join()
    #----------------------------------------------------------------------------------------------------
    def add_request(
        self,
        method: str,
        path: str,
        callback: CALLBACK_TYPE,
        params: dict = None,
        data: Union[dict, str, bytes] = None,
        headers: dict = None,
        on_failed: ON_FAILED_TYPE = None,
        on_error: ON_ERROR_TYPE = None,
        extra: Any = None,
    ) -> Request:
        """
        添加一个新请求。
        :param method： GET、POST、PUT、DELETE、QUERY
        :param path：查询的 URL 路径
        :param callback：2xx 状态下的回调函数，类型：（dict, Request）
        :param params: 查询字符串的 dict
        :param data： Http body。如果是 dict，将转换为表单数据。否则，将转换为字节。
        :param headers：头信息的 dict
        :param on_failed：Non-2xx status 时的回调函数，type：（status_code, Request）
        :on_error：捕获 Python 异常时的回调函数，类型：（exception_type, exception_value, tracebacks, request）
        :param extra： 处理回调时可以使用的任何额外数据
        :return： 请求
        """
        request = Request(
            method,
            path,
            params,
            data,
            headers,
            callback,
            on_failed,
            on_error,
            extra,
        )
        self._queue.put(request)
        return request
    #----------------------------------------------------------------------------------------------------
    def _run(self) -> None:
        """
        """
        try:
            session = requests.session()
            while self._active:
                try:
                    request = self._queue.get(timeout=0.01)
                    try:
                        self._process_request(request, session)
                    finally:
                        self._queue.task_done()
                except Empty:
                    pass
        except Exception:
            self.on_error(*sys.exc_info())
    #----------------------------------------------------------------------------------------------------
    def sign(self, request: Request) -> None:
        """
        该函数在发送任何请求之前被调用。
        请在此处实现签名方法。
        """
        return request
    #----------------------------------------------------------------------------------------------------
    def on_failed(self, status_code: int, request: Request) -> None:
        """
        请求失败的默认回调
        """
        try:
            data = request.response.json()
        except JSONDecodeError as err:
            write_log(f"交易接口：{self.gateway_name}，REST API解码json数据出错，错误代码：{status_code}，请求路径：{request.path}，错误信息：{err}")
            return
        filter_msg = ["Endpoint request timeout. ","No need to change position side."]
        #过滤OKX请求超时错误和币安重复设置持仓模式错误
        if isinstance(data, dict) and data.get("msg", None) in filter_msg:
            return
        write_log(f"交易接口：{self.gateway_name}，REST API请求失败代码：{status_code}，请求路径：{request.path}，完整请求：{request}")
    #----------------------------------------------------------------------------------------------------
    def on_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tracebacks,
        request: Optional[Request],
    ) -> None:
        """
        请求触发异常的默认回调
        """
        write_log(self.exception_detail(exception_type, exception_value, tracebacks, request))
    #----------------------------------------------------------------------------------------------------
    def exception_detail(
        self,
        exception_type: type,
        exception_value: Exception,
        tracebacks,
        request: Optional[Request],
    ) -> None:
        """
        将异常信息转化生成字符串
        """
        text = "[{}]: Unhandled RestClient Error:{}\n".format(datetime.now().isoformat(), exception_type)
        text += "request:{}\n".format(request)
        text += "Exception trace: \n"
        text += "".join(traceback.format_exception(exception_type, exception_value, tracebacks))
        return text
    #----------------------------------------------------------------------------------------------------
    def _process_request(
        self, request: Request, session: requests.Session
    ) -> None:
        """
        向服务器发送请求并获取结果。
        """
        try:
            request = self.sign(request)

            url = self.make_full_url(request.path)
            response = session.request(
                request.method,
                url,
                headers=request.headers,
                params=request.params,
                data=request.data if request.data and "jsonrpc" not in request.data else None, # jsonrpc不传data参数
                proxies=self.proxies,
                json = request.data if request.data and "jsonrpc" in request.data else None      # 兼容 jsonrpc 
            )
            request.response = response
            status_code = response.status_code
            if status_code // 100 == 2:  # 2xx codes are all successful
                if status_code == 204:
                    json_body = None
                else:
                    json_body = response.json()

                request.callback(json_body, request)
                request.status = RequestStatus.success
            else:
                request.status = RequestStatus.failed

                if request.on_failed:
                    request.on_failed(status_code, request)
                else:
                    self.on_failed(status_code, request)
        except Exception:
            request.status = RequestStatus.error
            exception_type, exception_value, tracebacks = sys.exc_info()
            if request.on_error:
                request.on_error(exception_type, exception_value, tracebacks, request)
            else:
                self.on_error(exception_type, exception_value, tracebacks, request)
            save_connection_status(self.gateway_name, False)
    #----------------------------------------------------------------------------------------------------
    def make_full_url(self, path: str) -> str:
        """
        将相对 api 路径转换为完整url
        """
        url = self.url_base + path
        return url
    #----------------------------------------------------------------------------------------------------
    def request(
        self,
        method: str,
        path: str,
        params: dict = None,
        data: dict = None,
        headers: dict = None,
    ) -> requests.Response:
        """
        添加一个新请求。
        :param method： GET、POST、PUT、DELETE、QUERY
        :param path：查询的网址路径
        :param params: 查询字符串的 dict
        :param data：dict，表示正文
        :param headers：头信息的 dict
        :return: requests.Response
        """
        request = Request(
            method,
            path,
            params,
            data,
            headers
        )
        request = self.sign(request)

        url = self.make_full_url(request.path)
        try:
            response = requests.request(
                request.method,
                url,
                headers=request.headers,
                params=request.params,
                data=request.data if request.data and "jsonrpc" not in request.data else None,
                json = request.data if request.data and "jsonrpc" in request.data else None,
                proxies=self.proxies,
            )
            return response
        except Exception as err:
            msg = f"交易接口：{self.gateway_name}，REST API连接出错，错误信息：{err}，重启交易子进程"
            write_log(msg)
            save_connection_status(self.gateway_name,False)
