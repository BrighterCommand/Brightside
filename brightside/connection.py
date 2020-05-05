class Connection:
    """Contains the details required to connect to a RMQ broker: the amqp uri and the exchange"""
    def __init__(self, amqp_uri: str, exchange: str, exchange_type: str = "direct", is_durable: bool = False, connect_timeout: int = 30, heartbeat: int = 30) -> None:
        self._amqp_uri = amqp_uri
        self._exchange = exchange
        self._exchange_type = exchange_type
        self._is_durable = is_durable
        self._connect_timeout = connect_timeout
        self._heartbeat = heartbeat

    @property
    def amqp_uri(self) -> str:
        return self._amqp_uri

    @amqp_uri.setter
    def amqp_uri(self, value: str):
        self._amqp_uri = value

    @property
    def exchange(self) -> str:
        return self._exchange

    @exchange.setter
    def exchange(self, value: str):
        self._exchange = value

    @property
    def exchange_type(self) -> str:
        return self._exchange_type

    @exchange_type.setter
    def exchange_type(self, value: str):
        self._exchange_type = value

    @property
    def is_durable(self):
        return self._is_durable

    @is_durable.setter
    def is_durable(self, value):
        self._is_durable = value

    @property
    def connect_timeout(self) -> int:
        return self._connect_timeout
    
    @connect_timeout.setter
    def connect_timeout(self, value:int):
        self._connect_timeout = value

    @property
    def heartbeat(self) -> int:
        return self._heartbeat
    
    @heartbeat.setter
    def heartbeat(self, value:int):
        self._heartbeat = value