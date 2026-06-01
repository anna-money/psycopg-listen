import asyncio
import dataclasses
import enum
import logging
import sys
from collections.abc import Callable, Coroutine
from typing import Any

import psycopg
from psycopg import sql

logger = logging.getLogger(__package__)


class ListenPolicy(enum.StrEnum):
    ALL = enum.auto()
    LAST = enum.auto()


@dataclasses.dataclass(frozen=True, slots=True)
class Timeout:
    channel: str


@dataclasses.dataclass(frozen=True, slots=True)
class Notification:
    channel: str
    payload: str


ConnectFunc = Callable[[], Coroutine[Any, Any, psycopg.AsyncConnection]]
NotificationOrTimeout = Notification | Timeout
NotificationHandler = Callable[[NotificationOrTimeout], Coroutine]

NO_TIMEOUT: float = -1
MAX_SILENCED_FAILED_CONNECT_ATTEMPTS = 3

_CONNECT_DEFAULTS: dict[str, Any] = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 3,
}


def connect_func(*args: Any, **kwargs: Any) -> ConnectFunc:
    options: dict[str, Any] = {**_CONNECT_DEFAULTS, **kwargs, "autocommit": True}

    async def _connect() -> psycopg.AsyncConnection:
        return await psycopg.AsyncConnection.connect(*args, **options)

    return _connect


class NotificationListener:
    __slots__ = (
        "__connect",
        "__reconnect_delay",
    )

    def __init__(self, connect: ConnectFunc, reconnect_delay: float = 5) -> None:
        self.__reconnect_delay = reconnect_delay
        self.__connect = connect

    async def run(
        self,
        handler_per_channel: dict[str, NotificationHandler],
        *,
        policy: ListenPolicy = ListenPolicy.ALL,
        notification_timeout: float = 30,
    ) -> None:
        queue_per_channel: dict[str, asyncio.Queue[Notification]] = {
            channel: asyncio.Queue() for channel in handler_per_channel
        }
        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                self.__read_notifications(queue_per_channel=queue_per_channel),
                name=__package__,
            )
            for channel, handler in handler_per_channel.items():
                tg.create_task(
                    self.__process_notifications(
                        channel,
                        notifications=queue_per_channel[channel],
                        handler=handler,
                        policy=policy,
                        notification_timeout=notification_timeout,
                    ),
                    name=f"{__package__}.{channel}",
                )

    @staticmethod
    async def __process_notifications(
        channel: str,
        *,
        notifications: asyncio.Queue[Notification],
        handler: NotificationHandler,
        policy: ListenPolicy,
        notification_timeout: float,
    ) -> None:
        # to have independent async context per run
        # to protect from misuse of contextvars
        if sys.version_info >= (3, 12):
            loop = asyncio.get_running_loop()

            async def run_coro(c: Coroutine) -> None:
                await asyncio.Task(c, loop=loop, eager_start=True, name=f"{__package__}.{channel}")

        else:

            async def run_coro(c: Coroutine) -> None:
                await asyncio.create_task(c, name=f"{__package__}.{channel}")

        while True:
            notification: NotificationOrTimeout | None = None

            if notifications.empty():
                if notification_timeout == NO_TIMEOUT:
                    notification = await notifications.get()
                else:
                    try:
                        async with asyncio.timeout(notification_timeout):
                            notification = await notifications.get()
                    except TimeoutError:
                        notification = Timeout(channel)
            else:
                while not notifications.empty():
                    notification = notifications.get_nowait()
                    if policy == ListenPolicy.ALL:
                        break

            if notification is None:
                continue

            try:
                await run_coro(handler(notification))
            except Exception:
                logger.exception("Failed to handle %s", notification)

    async def __read_notifications(self, queue_per_channel: dict[str, asyncio.Queue[Notification]]) -> None:
        failed_connect_attempts = 0
        while True:
            try:
                connection = await self.__connect()
                failed_connect_attempts = 0
                try:
                    if not connection.autocommit:
                        await connection.set_autocommit(True)
                    for channel in queue_per_channel:
                        await connection.execute(sql.SQL("LISTEN {}").format(sql.Identifier(channel)))

                    async for notify in connection.notifies():
                        queue = queue_per_channel.get(notify.channel)
                        if queue is not None:
                            queue.put_nowait(Notification(notify.channel, notify.payload))
                    logger.warning("Connection was lost")
                finally:
                    await connection.close()
            except Exception:
                if failed_connect_attempts < MAX_SILENCED_FAILED_CONNECT_ATTEMPTS:
                    logger.warning("Connection was lost or not established", exc_info=True)
                else:
                    logger.exception("Connection was lost or not established")
                await asyncio.sleep(self.__reconnect_delay * failed_connect_attempts)
                failed_connect_attempts += 1
