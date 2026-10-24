import json
import socket
from unittest.mock import MagicMock

from opentakserver.eud_handler.EudHandler import EudHandler


def make_handler(uid="ANDROID-1234", connected_at=100.0, session_id="b" * 32):
    handler = EudHandler.__new__(EudHandler)  # skip socketserver's setup/handle/finish
    handler.app = MagicMock()
    handler.app.config = {"OTS_RABBITMQ_TTL": "86400000", "OTS_EUD_IDLE_TIMEOUT": 300}
    handler.logger = MagicMock()
    handler.client_address = ("192.0.2.10", 50000)
    handler.request = MagicMock()
    handler.rabbit_connection = MagicMock(is_closing=False, is_closed=False)
    handler.rabbit_channel = MagicMock(is_open=True, is_closing=False, is_closed=False)
    handler.uid = uid
    handler.callsign = "ALPHA"
    handler.user = None
    handler.bound_queues = []
    handler.session_id = session_id
    handler.connected_at = connected_at
    return handler


def session_message(uid="ANDROID-1234", connected_at=200.0, session_id="c" * 32):
    return json.dumps({"uid": uid, "session_id": session_id, "connected_at": connected_at})


def test_idle_timeout_closes_the_connection():
    handler = make_handler()
    handler.request.recv.side_effect = TimeoutError

    handler.handle()

    assert handler.closed
    handler.request.shutdown.assert_called_once_with(socket.SHUT_RDWR)
    handler.rabbit_connection.ioloop.add_callback_threadsafe.assert_called_once_with(
        handler.close_rabbitmq
    )


def test_close_connection_runs_once_and_survives_no_rabbitmq():
    handler = make_handler()
    handler.rabbit_connection = None
    handler.rabbit_channel = None
    handler.request.shutdown.side_effect = OSError("not connected")

    handler.close_connection()
    handler.close_connection()

    handler.request.shutdown.assert_called_once()


def test_close_rabbitmq_reports_disconnect_and_unbinds():
    handler = make_handler()
    handler.bound_queues = [{"exchange": "dms", "routing_key": "ALPHA", "queue": "ALPHA"}]

    handler.close_rabbitmq()

    exchanges = [c.kwargs["exchange"] for c in handler.rabbit_channel.basic_publish.call_args_list]
    assert exchanges == ["cot_parser"]
    assert handler.rabbit_channel.queue_unbind.called
    handler.rabbit_channel.close.assert_called_once()


def test_close_rabbitmq_on_a_closing_channel_does_not_raise():
    handler = make_handler()
    handler.rabbit_channel = MagicMock(is_open=False, is_closing=True, is_closed=False)

    handler.close_rabbitmq()

    handler.rabbit_channel.basic_publish.assert_not_called()
    handler.rabbit_connection.close.assert_called_once()


def test_replaced_handler_leaves_the_queues_to_the_new_connection():
    handler = make_handler()
    handler.bound_queues = [{"exchange": "dms", "routing_key": "ALPHA", "queue": "ALPHA"}]
    handler.replaced = True

    handler.close_rabbitmq()

    handler.rabbit_channel.basic_publish.assert_not_called()
    handler.rabbit_channel.queue_unbind.assert_not_called()
    handler.rabbit_channel.close.assert_called_once()


def test_newer_connection_for_the_same_uid_replaces_this_one():
    handler = make_handler(connected_at=100.0)

    handler.on_session_message(None, None, None, session_message(connected_at=200.0))

    assert handler.replaced
    assert handler.closed
    handler.request.shutdown.assert_called_once_with(socket.SHUT_RDWR)


def test_older_own_or_other_uid_session_messages_are_ignored():
    handler = make_handler(connected_at=100.0)

    for body in (
        session_message(connected_at=50.0),  # an older connection announcing itself late
        session_message(session_id=handler.session_id),  # our own announcement
        session_message(uid="ANDROID-other"),
        "not json",
    ):
        handler.on_session_message(None, None, None, body)

    assert not handler.replaced
    assert not handler.closed


def test_claim_uid_announces_this_session_to_other_handlers():
    handler = make_handler()

    handler.claim_uid()

    queue = f"eud_session.{handler.session_id}"
    handler.rabbit_channel.queue_declare.assert_called_once_with(
        queue=queue, exclusive=True, auto_delete=True
    )
    handler.rabbit_channel.queue_bind.assert_called_once_with(
        exchange="eud_sessions", queue=queue, routing_key=handler.uid
    )
    publish = handler.rabbit_channel.basic_publish.call_args.kwargs
    assert publish["exchange"] == "eud_sessions"
    assert publish["routing_key"] == handler.uid
    assert json.loads(publish["body"])["session_id"] == handler.session_id


def test_configure_socket_sets_idle_timeout_and_keepalive():
    handler = make_handler()
    handler.idle_timeout = 300
    handler.request = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        handler.configure_socket()

        assert handler.request.gettimeout() == 300
        assert handler.request.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 1
    finally:
        handler.request.close()


def test_configure_socket_ignores_udp_requests():
    handler = make_handler()
    handler.request = (b"<event/>", MagicMock())

    handler.configure_socket()
