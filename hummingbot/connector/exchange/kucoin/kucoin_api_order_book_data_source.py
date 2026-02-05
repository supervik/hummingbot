import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.kucoin import kucoin_constants as CONSTANTS, kucoin_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.event.events import OrderBookBestBidAskEvent, OrderBookDataSourceEvent
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.kucoin.kucoin_exchange import KucoinExchange


class KucoinAPIOrderBookDataSource(OrderBookTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(
            self,
            trading_pairs: List[str],
            connector: 'KucoinExchange',
            api_factory: WebAssistantsFactory,
            domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._domain = domain
        self._api_factory = api_factory
        self._last_ws_message_sent_timestamp = 0
        self._ping_interval = 0
        self._trading_pair_cache = {}  # symbol -> trading_pair

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp = float(snapshot_response["data"]["time"]) * 1e-3
        update_id: int = int(snapshot_response["data"]["sequence"])

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": snapshot_response["data"]["bids"],
            "asks": snapshot_response["data"]["asks"]
        }
        snapshot_msg: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            order_book_message_content,
            snapshot_timestamp)

        return snapshot_msg

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        params = {
            "symbol": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.SNAPSHOT_NO_AUTH_PATH_URL),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.SNAPSHOT_NO_AUTH_PATH_URL,
        )

        return data

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trade_data: Dict[str, Any] = raw_message["data"]
        timestamp: float = int(trade_data["time"]) * 1e-9
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=trade_data["symbol"])
        message_content = {
            "trade_id": trade_data["tradeId"],
            "update_id": trade_data["sequence"],
            "trading_pair": trading_pair,
            "trade_type": float(TradeType.BUY.value) if trade_data["side"] == "buy" else float(
                TradeType.SELL.value),
            "amount": trade_data["size"],
            "price": trade_data["price"]
        }
        trade_message: Optional[OrderBookMessage] = OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content=message_content,
            timestamp=timestamp)

        message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        diff_data: [str, Any] = raw_message["data"]
        timestamp: float = self._time()
        update_id: int = diff_data["sequenceEnd"]

        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=diff_data["symbol"])

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "first_update_id": diff_data["sequenceStart"],
            "bids": diff_data["changes"]["bids"],
            "asks": diff_data["changes"]["asks"],
        }
        diff_message: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.DIFF,
            order_book_message_content,
            timestamp)

        message_queue.put_nowait(diff_message)

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            symbols = ",".join([await self._connector.exchange_symbol_associated_to_pair(trading_pair=pair)
                                for pair in self._trading_pairs])

            trades_payload = {
                "id": web_utils.next_message_id(),
                "type": "subscribe",
                "topic": f"/market/match:{symbols}",
                "privateChannel": False,
                "response": False,
            }
            subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=trades_payload)

            order_book_payload = {
                "id": web_utils.next_message_id(),
                "type": "subscribe",
                "topic": f"/market/level2:{symbols}",
                "privateChannel": False,
                "response": False,
            }
            subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=order_book_payload)

            best_bidask_payload = {
                "id": web_utils.next_message_id(),
                "type": "subscribe",
                "topic": f"/spotMarket/level1:{symbols}",
                "privateChannel": False,
                "response": False,
            }
            subscribe_best_bidask_request: WSJSONRequest = WSJSONRequest(payload=best_bidask_payload)

            # await ws.send(subscribe_trade_request)
            await ws.send(subscribe_orderbook_request)
            await ws.send(subscribe_best_bidask_request)

            self._last_ws_message_sent_timestamp = self._time()
            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to order book trading and delta streams...")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        if "data" in event_message and event_message.get("type") == "message":
            event_channel = event_message.get("subject")
            if event_channel == CONSTANTS.TRADE_EVENT_TYPE:
                channel = self._trade_messages_queue_key
            elif event_channel == CONSTANTS.DIFF_EVENT_TYPE:
                channel = self._diff_messages_queue_key
            elif event_channel == CONSTANTS.LEVEL1_EVENT_TYPE:
                safe_ensure_future(self._trigger_best_bidask_event(event_message))

        return channel

    async def _trigger_best_bidask_event(self, event_message: Dict[str, Any]) -> None:
        if event_message.get("data"):
            data = event_message["data"]
            # Extract symbol from topic: "/spotMarket/level1:BTC-USDT" -> "BTC-USDT"
            topic = event_message.get("topic", "")
            exchange_symbol = topic.split(":")[-1] if ":" in topic else ""
            
            if not exchange_symbol:
                return
                
            trading_pair = self._get_trading_pair_from_cache(exchange_symbol)

            # Level1 data format: bids/asks are arrays [price, size]
            best_bid_price = Decimal(str(data["bids"][0]))
            best_bid_size = Decimal(str(data["bids"][1]))
            best_ask_price = Decimal(str(data["asks"][0]))
            best_ask_size = Decimal(str(data["asks"][1]))
            ticker_id = data.get("timestamp", "")
            
            self._connector.trigger_event(
                OrderBookDataSourceEvent.BEST_BID_ASK_EVENT,
                OrderBookBestBidAskEvent(
                    trading_pair,
                    ticker_id,
                    best_bid_price,
                    best_ask_price,
                    best_bid_size,
                    best_ask_size
                )
            )

    def _get_trading_pair_from_cache(self, exchange_symbol: str) -> str:
        """
        Get trading pair from cache using exchange symbol.
        Builds cache lazily on first call.

        Args:
            exchange_symbol: Exchange symbol (e.g., 'BTC-USDT')

        Returns:
            str: Trading pair (e.g., 'BTC-USDT')
        """
        # Check if symbol is already in cache
        if exchange_symbol in self._trading_pair_cache:
            return self._trading_pair_cache[exchange_symbol]

        # If not in cache, get it using the sync method and cache it
        trading_pair = self._connector.trading_pair_associated_to_exchange_symbol_sync(exchange_symbol)
        self.logger().info(f">>>(kucoin) New pair cache: {exchange_symbol} -> {trading_pair}")
        self._trading_pair_cache[exchange_symbol] = trading_pair

        return trading_pair

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        while True:
            try:
                seconds_until_next_ping = self._ping_interval - (self._time() - self._last_ws_message_sent_timestamp)
                await asyncio.wait_for(super()._process_websocket_messages(websocket_assistant=websocket_assistant),
                                       timeout=seconds_until_next_ping)
            except asyncio.TimeoutError:
                payload = {
                    "id": web_utils.next_message_id(),
                    "type": "ping",
                }
                ping_request = WSJSONRequest(payload=payload)
                self._last_ws_message_sent_timestamp = self._time()
                await websocket_assistant.send(request=ping_request)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        rest_assistant = await self._api_factory.get_rest_assistant()
        connection_info = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.PUBLIC_WS_DATA_PATH_URL, domain=self._domain),
            method=RESTMethod.POST,
            throttler_limit_id=CONSTANTS.PUBLIC_WS_DATA_PATH_URL,
        )

        ws_url = connection_info["data"]["instanceServers"][0]["endpoint"]
        self._ping_interval = int(connection_info["data"]["instanceServers"][0]["pingInterval"]) * 0.8 * 1e-3
        token = connection_info["data"]["token"]

        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=f"{ws_url}?token={token}", message_timeout=self._ping_interval)
        return ws
