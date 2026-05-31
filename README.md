# psycopg-listen

This library simplifies usage of listen/notify with [psycopg](https://github.com/psycopg/psycopg) (psycopg 3):
1. Handles loss of a connection
2. Simplifies notifications processing from multiple channels
3. Setups a timeout for receiving a notification
4. Allows to receive all notifications/only last notification depending on ListenPolicy.

Liveness follows the [psycopg recommendation](https://www.psycopg.org/psycopg3/docs/advanced/async.html#detecting-disconnections): no `SELECT 1` polling. The `notifies()` generator waits on the socket and raises as soon as the connection is dropped, and TCP keepalives (set by `connect_func`) cover silent deaths. If you pass your own connect function, set the keepalive parameters yourself.

```python
import asyncio
import psycopg
import psycopg_listen


async def handle_notifications(notification: psycopg_listen.NotificationOrTimeout) -> None:
    print(f"{notification} has been received")


async def main() -> None:
    listener = psycopg_listen.NotificationListener(psycopg_listen.connect_func(user="postgres"))
    listener_task = asyncio.create_task(
        listener.run(
            {"simple": handle_notifications},
            policy=psycopg_listen.ListenPolicy.LAST,
            notification_timeout=5,
        )
    )

    await asyncio.sleep(1)

    connection = await psycopg.AsyncConnection.connect(user="postgres", autocommit=True)
    try:
        for i in range(42):
            await connection.execute(f"NOTIFY simple, '{i}'")
    finally:
        await connection.close()

    await asyncio.sleep(1)

    listener_task.cancel()


asyncio.run(main())
```
