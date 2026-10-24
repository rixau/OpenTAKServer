import base64
import datetime
import json
import logging
import os
import platform
import random
import re
import socket as socket_module
import socketserver
import sys
import time
import traceback
import uuid
from logging.handlers import TimedRotatingFileHandler
from socket import socket, SHUT_RDWR, SOL_SOCKET, SO_KEEPALIVE, IPPROTO_TCP
from threading import Thread, current_thread
from xml.etree.ElementTree import Element, SubElement, tostring, fromstring, ParseError

import bleach
import colorlog
import flask_wtf
import pika
import sqlalchemy
import yaml
from bs4 import BeautifulSoup
from flask import Flask
from flask_ldap3_login import AuthenticationResponseStatus
from flask_security import SQLAlchemyUserDatastore, Security, verify_password
from flask_security.models import fsqla
from pika.channel import Channel
from sqlalchemy import insert, update, select

from opentakserver.EmailValidator import EmailValidator
from opentakserver.PasswordValidator import PasswordValidator
from opentakserver.defaultconfig import DefaultConfig
from opentakserver.extensions import logger as ots_logger, db, ldap_manager
from opentakserver.functions import iso8601_string_from_datetime, datetime_from_iso8601_string

# These unused imports are required by SQLAlchemy, don't remove them
from opentakserver.models.Alert import Alert
from opentakserver.models.CasEvac import CasEvac
from opentakserver.models.Certificate import Certificate
from opentakserver.models.Chatrooms import Chatroom
from opentakserver.models.ChatroomsUids import ChatroomsUids
from opentakserver.models.CoT import CoT
from opentakserver.models.DataPackage import DataPackage
from opentakserver.models.DeviceProfiles import DeviceProfiles
from opentakserver.models.EUD import EUD
from opentakserver.models.EUDStats import EUDStats
from opentakserver.models.Group import Group
from opentakserver.models.GroupMission import GroupMission
from opentakserver.models.GroupUser import GroupUser
from opentakserver.models.Marker import Marker
from opentakserver.models.Mission import Mission
from opentakserver.models.MissionChange import MissionChange
from opentakserver.models.MissionContentMission import MissionContentMission
from opentakserver.models.MissionInvitation import MissionInvitation
from opentakserver.models.MissionLogEntry import MissionLogEntry
from opentakserver.models.MissionUID import MissionUID
from opentakserver.models.Point import Point
from opentakserver.models.RBLine import RBLine
from opentakserver.models.Team import Team
from opentakserver.models.VideoRecording import VideoRecording
from opentakserver.models.VideoStream import VideoStream
from opentakserver.models.WebAuthn import WebAuthn
from opentakserver.models.ZMIST import ZMIST
from opentakserver.models.CITrap import CITrap


class EudHandler(socketserver.BaseRequestHandler):

    timeout = 1.0
    shutdown = False
    common_name = None
    user = None
    is_ssl = False
    logger = ots_logger
    app = None
    rabbit_connection = None
    rabbit_channel = None
    iothread = None
    is_consuming = False
    is_authenticated = False
    cached_messages = []
    eud = None
    callsign = None
    uid = None
    bound_queues = []
    phone_number = None
    group_memberships = []
    idle_timeout = None
    closed = False
    # Set when a newer connection for the same uid takes over this one
    replaced = False
    session_id = None
    connected_at = None

    def __init__(self, request: socket, client_address, server):
        super().__init__(request, client_address, server)
        self.logger = logging.getLogger()
        self.socket: socket = request

    def handle(self):
        cot = ""

        while not self.shutdown:
            try:
                data = self.request.recv(65536)
            except TimeoutError:
                self.logger.info(
                    f"Nothing received from {self.callsign or self.client_address[0]} in {self.idle_timeout} seconds, closing"
                )
                break
            except Exception as e:
                self.logger.debug(f"recv failed: {e}")
                break
            if not data:
                self.logger.debug("no data")
                break

            cot += data.decode("utf-8")
            cot_list = re.split("</event>|</auth>", cot)

            if len(cot_list) < 2:
                continue

            for c in cot_list:
                try:
                    if "<event" in c:
                        fromstring(c + "</event>")
                        self.handle_cot(c + "</event>")
                    elif "<auth>" in c:
                        fromstring(c + "</auth>")
                        self.handle_auth(c + "</auth>")
                except ParseError as e:
                    self.logger.error(f"Failed to parse: {e}")
                    cot = c
                    break

            cot = ""

        self.close_connection()

    def pong(self, event):
        if event.attrs.get("type") == "t-x-c-t":
            now = datetime.datetime.now(datetime.timezone.utc)
            stale = now + datetime.timedelta(seconds=10)

            cot = Element(
                "event",
                {
                    "how": "h-g-i-g-o",
                    "type": "t-x-c-t-r",
                    "version": "2.0",
                    "uid": "{}-pong".format(event.attrs.get("uid")),
                    "start": iso8601_string_from_datetime(now),
                    "time": iso8601_string_from_datetime(now),
                    "stale": iso8601_string_from_datetime(stale),
                },
            )
            SubElement(
                cot, "point", {"ce": "9999999", "le": "9999999", "hae": "0", "lat": "0", "lon": "0"}
            )

            try:
                self.request.send(event.encode())
                return True
            except BaseException as e:
                self.logger.error(f"Pong error: {e}")

        return False

    def setup(self):
        self.create_app()
        self.session_id = uuid.uuid4().hex
        self.connected_at = time.time()
        self.idle_timeout = self.app.config.get("OTS_EUD_IDLE_TIMEOUT") or None
        self.configure_socket()

        # RabbitMQ
        try:
            rabbit_credentials = pika.PlainCredentials(
                self.app.config.get("OTS_RABBITMQ_USERNAME"),
                self.app.config.get("OTS_RABBITMQ_PASSWORD"),
            )
            rabbit_host = self.app.config.get("OTS_RABBITMQ_SERVER_ADDRESS")
            self.rabbit_connection = pika.SelectConnection(
                pika.ConnectionParameters(host=rabbit_host, credentials=rabbit_credentials),
                self.on_connection_open,
                on_close_callback=self.on_close,
            )
            self.rabbit_channel: Channel | None = None
            # Start the pika ioloop in a thread or else it blocks and we can't receive any CoT messages
            self.iothread = Thread(
                target=self.rabbit_connection.ioloop.start, name=f"IOLOOP_{self.common_name}"
            )
            # self.iothread.daemon = True
            self.iothread.start()
            self.is_consuming = False
        except BaseException as e:
            self.logger.error("Failed to connect to rabbitmq: {}".format(e))
            return

    def configure_socket(self):
        # The UDP server hands handlers a (data, socket) tuple
        if not isinstance(self.request, socket):
            return

        # recv() raises TimeoutError after this long without data, handle() treats that as dead
        self.request.settimeout(self.idle_timeout)

        # Let the kernel notice peers that vanished without a FIN or RST
        self.request.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
        for option, value in (("TCP_KEEPIDLE", 60), ("TCP_KEEPINTVL", 10), ("TCP_KEEPCNT", 6)):
            if hasattr(socket_module, option):
                self.request.setsockopt(IPPROTO_TCP, getattr(socket_module, option), value)

    def finish(self):
        # This process exits as soon as finish() returns, so give the ioloop a moment to close
        # the RabbitMQ connection cleanly. Closing it is what cancels this connection's consumers.
        if self.iothread and self.iothread.is_alive() and self.iothread is not current_thread():
            self.iothread.join(timeout=5)

    def close_connection(self):
        # Called from the socket thread, the ioloop thread, or both
        if self.closed:
            return
        self.closed = True
        self.shutdown = True

        self.logger.info("{} disconnected".format(self.callsign or self.client_address[0]))

        # pika isn't thread safe, so RabbitMQ is closed from the ioloop thread
        if (
            self.rabbit_connection
            and not self.rabbit_connection.is_closing
            and not self.rabbit_connection.is_closed
        ):
            self.rabbit_connection.ioloop.add_callback_threadsafe(self.close_rabbitmq)

        # Wakes handle() if it's blocked in recv(). socketserver closes the socket after finish()
        try:
            self.request.shutdown(SHUT_RDWR)
        except OSError:
            pass

    def close_rabbitmq(self):
        try:
            if self.rabbit_channel and self.rabbit_channel.is_open:
                # A newer connection for this uid owns the queues now, so leave them bound
                # and don't tell cot_parser that the EUD disconnected
                if not self.replaced:
                    self.rabbit_channel.basic_publish(
                        exchange="cot_parser",
                        body=json.dumps(
                            {
                                "uid": self.uid,
                                "cot": None,
                                "disconnected": True,
                                "user_id": self.user.id if self.user else None,
                            }
                        ),
                        routing_key="cot_parser",
                        properties=pika.BasicProperties(
                            expiration=self.app.config.get("OTS_RABBITMQ_TTL")
                        ),
                    )
                    self.unbind_rabbitmq_queues()

                # on_channel_close closes the connection, which stops the ioloop
                self.rabbit_channel.close()
            elif (
                self.rabbit_connection
                and not self.rabbit_connection.is_closing
                and not self.rabbit_connection.is_closed
            ):
                self.rabbit_connection.close()
        except BaseException as e:
            self.logger.error(f"Failed to close RabbitMQ for {self.callsign}: {e}")
            self.logger.debug(traceback.format_exc())

    def create_app(self):
        app = Flask(__name__)
        app.config.from_object(DefaultConfig)

        # Load config.yml if it exists
        if os.path.exists(os.path.join(app.config.get("OTS_DATA_FOLDER"), "config.yml")):
            app.config.from_file(
                os.path.join(app.config.get("OTS_DATA_FOLDER"), "config.yml"), load=yaml.safe_load
            )
        else:
            # First run, created config.yml based on default settings
            self.logger.info("Creating config.yml")
            with open(os.path.join(app.config.get("OTS_DATA_FOLDER"), "config.yml"), "w") as config:
                conf = {}
                for option in DefaultConfig.__dict__:
                    # Fix the sqlite DB path on Windows
                    if (
                        option == "SQLALCHEMY_DATABASE_URI"
                        and platform.system() == "Windows"
                        and DefaultConfig.__dict__[option].startswith("sqlite")
                    ):
                        conf[option] = (
                            DefaultConfig.__dict__[option].replace("////", "///").replace("\\", "/")
                        )
                    elif option.isupper():
                        conf[option] = DefaultConfig.__dict__[option]
                config.write(yaml.safe_dump(conf))

        db.init_app(app)

        if app.config.get("OTS_ENABLE_LDAP"):
            self.logger.info("Enabling LDAP")
            ldap_manager.init_app(app)

        # The rest is required by flask, leave it in
        try:
            fsqla.FsModels.set_db_info(db)
        except sqlalchemy.exc.InvalidRequestError:
            pass

        from opentakserver.models.role import Role
        from opentakserver.models.user import User

        flask_wtf.CSRFProtect(app)
        user_datastore = SQLAlchemyUserDatastore(db, User, Role)
        app.security = Security(
            app, user_datastore, mail_util_cls=EmailValidator, password_util_cls=PasswordValidator
        )

        self.app = app
        return app

    def on_connection_open(self, connection: pika.SelectConnection):
        self.rabbit_connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel: Channel):
        self.logger.debug(f"Opening RabbitMQ channel for {self.callsign or self.client_address[0]}")
        self.rabbit_channel = channel
        self.rabbit_channel.add_on_close_callback(self.on_channel_close)

        self.rabbit_channel.exchange_declare(
            "flask-socketio", durable=False, exchange_type="fanout"
        )
        # Used to tell an older connection for a uid that the EUD has reconnected
        self.rabbit_channel.exchange_declare("eud_sessions", durable=True, exchange_type="direct")

        for message in self.cached_messages:
            self.publish_cot(message)

        self.cached_messages.clear()

        # Publish the EUD info to flask-socketio for the web UI map
        if self.eud:
            message = {
                "method": "emit",
                "event": "eud",
                "data": self.eud.to_json(),
                "namespace": "/socket.io",
                "room": None,
                "skip_sid": [],
                "callback": None,
                "binary": False,
                "host_id": uuid.uuid4().hex,
            }
            self.rabbit_channel.basic_publish(
                "flask-socketio",
                "",
                json.dumps(message),
                properties=pika.BasicProperties(expiration=self.app.config.get("OTS_RABBITMQ_TTL")),
            )

    def on_channel_close(self, channel: Channel, error):
        if self.closed:
            self.logger.debug(f"RabbitMQ channel closed for {self.callsign}")
        else:
            self.logger.error(f"RabbitMQ channel closed for {self.callsign}: {error}, shut it down")

        if (
            self.rabbit_connection
            and not self.rabbit_connection.is_closing
            and not self.rabbit_connection.is_closed
        ):
            self.rabbit_connection.close()

        # Without a channel nothing reaches this EUD, so disconnect it and let it reconnect
        self.close_connection()

    def on_close(self, connection, error):
        # Stop the ioloop using add_callback_threadsafe because ioloop.stop() isn't threadsafe
        connection.ioloop.add_callback_threadsafe(self.rabbit_connection.ioloop.stop)
        self.logger.info("Connection closed for {}: {}".format(self.client_address[0], error))

    def on_message(self, unused_channel, basic_deliver, properties, body):
        try:
            body = json.loads(body)
            if body["uid"] != self.uid:
                self.request.send(body["cot"].encode())
        except BaseException as e:
            self.logger.error(f"{self.callsign}: {e}, closing socket")
            self.close_connection()
            self.logger.error(traceback.format_exc())

    def claim_uid(self):
        """Tell any other handler serving this uid to close. Runs on the ioloop thread."""
        if not self.rabbit_channel or not self.rabbit_channel.is_open:
            return

        queue = f"eud_session.{self.session_id}"
        self.rabbit_channel.queue_declare(queue=queue, exclusive=True, auto_delete=True)
        self.rabbit_channel.queue_bind(exchange="eud_sessions", queue=queue, routing_key=self.uid)
        self.rabbit_channel.basic_consume(
            queue=queue, on_message_callback=self.on_session_message, auto_ack=True
        )
        self.rabbit_channel.basic_publish(
            exchange="eud_sessions",
            routing_key=self.uid,
            body=json.dumps(
                {
                    "uid": self.uid,
                    "session_id": self.session_id,
                    "connected_at": self.connected_at,
                }
            ),
        )

    def on_session_message(self, unused_channel, basic_deliver, properties, body):
        # Another handler, probably in another process, has identified the same uid.
        # The newest connection wins: an EUD that reconnects has given up on its old socket,
        # even if this side never saw it close, and two consumers on one queue split its messages.
        try:
            body = json.loads(body)
            if body.get("uid") != self.uid or body.get("session_id") == self.session_id:
                return
            if (body.get("connected_at", 0), body.get("session_id")) < (
                self.connected_at,
                self.session_id,
            ):
                return

            self.logger.info(
                f"{self.callsign} ({self.uid}) connected again from another socket, closing this one"
            )
            self.replaced = True
            self.close_connection()
        except BaseException as e:
            self.logger.error(f"Failed to handle session message: {e}")
            self.logger.debug(traceback.format_exc())

    def handle_auth(self, auth: str):
        self.logger.debug(auth)
        if auth:
            auth = BeautifulSoup(auth, "xml")
        if self.is_ssl and not self.is_authenticated and (auth or self.common_name):
            user = None
            with self.app.app_context():
                if auth:
                    cot = auth.find("cot")
                    if cot:
                        username = cot.attrs["username"]
                        password = cot.attrs["password"]
                        uid = cot.attrs["uid"]

                        if self.app.config.get("OTS_ENABLE_LDAP"):
                            result = ldap_manager.authenticate(username, password)

                            if result.status == AuthenticationResponseStatus.success:
                                # Keep this import here to avoid a circular import when OTS is started
                                from opentakserver.blueprints.ots_api.ldap_api import save_user

                                self.user = save_user(
                                    result.user_dn,
                                    result.user_id,
                                    result.user_info,
                                    result.user_groups,
                                )

                                try:
                                    eud = db.session.execute(
                                        db.session.query(EUD).filter_by(uid=uid)
                                    ).first()[0]
                                    self.logger.debug(
                                        "Associating EUD uid {} to user {}".format(
                                            eud.uid, self.user.username
                                        )
                                    )
                                    eud.user_id = self.user.id
                                    db.session.commit()
                                except:
                                    self.logger.debug(
                                        "This is a new eud: {} {}".format(uid, self.user.username)
                                    )
                                    eud = EUD()
                                    eud.uid = uid
                                    eud.user_id = self.user.id
                                    eud.callsign = self.callsign
                                    db.session.add(eud)
                                    db.session.commit()

                            else:
                                self.close_connection()
                                return

                        else:
                            user = self.app.security.datastore.find_user(username=username)
                elif self.common_name:
                    user = self.app.security.datastore.find_user(username=self.common_name)

                if not user:
                    self.logger.warning("User {} does not exist".format(self.common_name))
                    self.close_connection()
                    return
                elif not user.active:
                    self.logger.warning("User {} is deactivated, disconnecting".format(username))
                    self.close_connection()
                    return
                elif self.common_name:
                    self.logger.info("{} is ID'ed by cert".format(user.username))
                    self.is_authenticated = True
                    self.user = user
                elif verify_password(password, user.password):
                    self.logger.info("Successful login from {}".format(username))
                    self.is_authenticated = True
                    self.user = user
                    try:
                        eud = db.session.execute(db.session.query(EUD).filter_by(uid=uid)).first()[
                            0
                        ]
                        self.logger.debug(
                            "Associating EUD uid {} to user {}".format(eud.uid, user.username)
                        )
                        eud.user_id = user.id
                        db.session.commit()
                    except:
                        self.logger.debug("This is a new eud: {} {}".format(uid, user.username))
                        eud = EUD()
                        eud.uid = uid
                        eud.user_id = user.id
                        db.session.add(eud)
                        db.session.commit()

                else:
                    self.logger.warning("Wrong password for user {}".format(username))
                    self.close_connection()
                    return

    def handle_cot(self, cot):
        self.logger.debug(cot)
        event = BeautifulSoup(cot, "xml").find("event")

        # If this client is connected via ssl, make sure they're authenticated
        # before accepting any data from them
        if self.is_ssl and not self.is_authenticated:
            self.logger.warning("EUD isn't authenticated, ignoring")
            return

        if self.pong(event):
            return

        if event and not self.uid:
            self.parse_device_info(event)
            # Close the DB connection once the EUD is authenticated and identified
            with self.app.app_context():
                db.session.close()
                db.engine.dispose()

        self.publish_cot(event)

    def publish_cot(self, event):
        if not event:
            return

        if not self.rabbit_channel or not self.rabbit_channel.is_open:
            self.cached_messages.append(event)
            self.logger.error("RabbitMQ channel is closed, not publishing cot")
            return

        # Route all CoTs to the firehose exchange for plugins and users that connect directly to RabbitMQ
        self.rabbit_channel.basic_publish(
            exchange="firehose",
            body=json.dumps({"uid": self.uid, "cot": str(event)}),
            routing_key="",
            properties=pika.BasicProperties(expiration=self.app.config.get("OTS_RABBITMQ_TTL")),
        )

        # Route all cots to the cot_parser direct exchange to be processed by a pool of cot_parser processes
        self.rabbit_channel.basic_publish(
            exchange="cot_parser",
            body=json.dumps(
                {"uid": self.uid, "cot": str(event), "user_id": self.user.id if self.user else None}
            ),
            routing_key="cot_parser",
            properties=pika.BasicProperties(expiration=self.app.config.get("OTS_RABBITMQ_TTL")),
        )

    def parse_device_info(self, event):
        link = event.find("link")
        fileshare = event.find("fileshare")

        # EUDs running the Meshtastic and dmrcot plugins can relay messages from their RF networks to the server
        # so we want to use the UID of the "off grid" EUD, not the relay EUD
        contact = event.find("contact")
        takv = event.find("takv")
        if takv or contact:
            uid = event.attrs.get("uid")
        else:
            return

        contact = event.find("contact")

        # Only assume it's an EUD if it's got a <contact> tag
        if contact and uid and not uid.endswith("ping") and (self.user or not self.is_ssl):
            self.uid = uid
            device = operating_system = platform = version = None
            if takv:
                device = takv.attrs["device"] if "device" in takv.attrs else None
                operating_system = takv.attrs["os"] if "os" in takv.attrs else None
                platform = takv.attrs["platform"] if "platform" in takv.attrs else None
                version = takv.attrs["version"] if "version" in takv.attrs else None

            if "callsign" in contact.attrs:
                self.callsign = contact.attrs["callsign"]

                # Declare a RabbitMQ Queue for this uid and join the 'dms' and 'cot' exchanges
                if (
                    self.rabbit_channel
                    and self.rabbit_channel.is_open
                    and platform != "OpenTAK ICU"
                    and platform != "Meshtastic"
                    and platform != "DMRCOT"
                ):

                    self.logger.debug(f"Declaring queue for {self.callsign} {self.uid}")
                    self.rabbit_channel.queue_declare(queue=self.callsign)
                    self.rabbit_channel.queue_declare(queue=self.uid)

                    with self.app.app_context():
                        if self.is_ssl:
                            group_memberships = db.session.execute(
                                db.session.query(GroupUser).filter_by(
                                    user_id=self.user.id, direction=Group.OUT
                                )
                            ).all()
                            if not group_memberships:
                                self.logger.debug(
                                    f"{self.callsign} doesn't belong to any groups, adding them to the __ANON__ group"
                                )
                                self.rabbit_channel.queue_bind(
                                    exchange="groups", queue=self.uid, routing_key="__ANON__.OUT"
                                )
                                if {
                                    "exchange": "groups",
                                    "routing_key": "__ANON__.OUT",
                                    "queue": self.uid,
                                } not in self.bound_queues:
                                    self.bound_queues.append(
                                        {
                                            "exchange": "groups",
                                            "routing_key": "__ANON__.OUT",
                                            "queue": self.uid,
                                        }
                                    )

                            elif group_memberships and self.is_ssl:
                                for membership in group_memberships:
                                    membership: GroupUser = membership[0]
                                    self.group_memberships.append(membership)

                                    if membership.enabled:
                                        self.rabbit_channel.queue_bind(
                                            exchange="groups",
                                            queue=self.uid,
                                            routing_key=f"{membership.group.name}.OUT",
                                        )

                                    if {
                                        "exchange": "groups",
                                        "routing_key": f"{membership.group.name}.OUT",
                                        "queue": self.uid,
                                    } not in self.bound_queues:
                                        self.bound_queues.append(
                                            {
                                                "exchange": "groups",
                                                "routing_key": f"{membership.group.name}.OUT",
                                                "queue": self.uid,
                                            }
                                        )

                        self.rabbit_channel.queue_bind(
                            exchange="missions", routing_key="missions", queue=self.uid
                        )
                        if {
                            "exchange": "missions",
                            "routing_key": "missions",
                            "queue": self.uid,
                        } not in self.bound_queues:
                            self.bound_queues.append(
                                {
                                    "exchange": "missions",
                                    "routing_key": "missions",
                                    "queue": self.uid,
                                }
                            )

                        # The DMs queue also binds by callsign since the <dest> tag in CoT messages can be by callsign instead of UID
                        self.rabbit_channel.queue_bind(
                            exchange="dms", queue=self.uid, routing_key=self.uid
                        )
                        self.rabbit_channel.queue_bind(
                            exchange="dms", queue=self.callsign, routing_key=self.callsign
                        )

                        if {
                            "exchange": "dms",
                            "routing_key": self.uid,
                            "queue": self.uid,
                        } not in self.bound_queues:
                            self.bound_queues.append(
                                {"exchange": "dms", "routing_key": self.uid, "queue": self.uid}
                            )

                        if {
                            "exchange": "dms",
                            "routing_key": self.callsign,
                            "queue": self.callsign,
                        } not in self.bound_queues:
                            self.bound_queues.append(
                                {
                                    "exchange": "dms",
                                    "routing_key": self.callsign,
                                    "queue": self.callsign,
                                }
                            )

                        if not self.is_ssl:
                            self.logger.debug(
                                f"{self.callsign} is connected via TCP, adding them to the __ANON__ group"
                            )
                            self.rabbit_channel.queue_bind(
                                exchange="groups", queue=self.uid, routing_key="__ANON__.OUT"
                            )
                            self.bound_queues.append(
                                {
                                    "exchange": "groups",
                                    "routing_key": "__ANON__.OUT",
                                    "queue": self.uid,
                                }
                            )

                        self.rabbit_channel.basic_consume(
                            queue=self.callsign, on_message_callback=self.on_message, auto_ack=True
                        )
                        self.rabbit_channel.basic_consume(
                            queue=self.uid, on_message_callback=self.on_message, auto_ack=True
                        )
                        self.rabbit_connection.ioloop.add_callback_threadsafe(self.claim_uid)

            if "phone" in contact.attrs and contact.attrs["phone"]:
                self.phone_number = contact.attrs["phone"]

            with self.app.app_context():
                __group = event.find("__group")
                team = Team()

                if __group:
                    team.name = bleach.clean(__group.attrs["name"])

                    try:
                        chatroom = db.session.execute(
                            select(Chatroom).filter(Chatroom.name == team.name)
                        ).first()[0]
                        team.chatroom_id = chatroom.id
                    except TypeError:
                        chatroom = None

                    try:
                        db.session.add(team)
                        db.session.commit()
                    except sqlalchemy.exc.IntegrityError:
                        db.session.rollback()
                        team = db.session.execute(
                            select(Team).filter(Team.name == __group.attrs["name"])
                        ).first()[0]
                        if not team.chatroom_id and chatroom:
                            team.chatroom_id = chatroom.id
                            db.session.execute(
                                update(Team)
                                .filter(Team.name == chatroom.id)
                                .values(chatroom_id=chatroom.id)
                            )

                try:
                    eud = db.session.execute(select(EUD).filter_by(uid=uid)).first()[0]
                except:
                    eud = EUD()

                eud.uid = uid
                if self.callsign:
                    eud.callsign = self.callsign
                if device:
                    eud.device = device

                eud.os = operating_system
                eud.platform = platform
                eud.version = version
                eud.phone_number = self.phone_number
                eud.last_event_time = datetime_from_iso8601_string(event.attrs["start"])
                eud.last_status = "Connected"
                eud.user_id = self.user.id if self.user else None

                # Set a Meshtastic ID for TAK EUDs to be identified by in the Meshtastic network
                if not eud.meshtastic_id and eud.platform != "Meshtastic":
                    meshtastic_id = "{:x}".format(int.from_bytes(os.urandom(4), "big"))
                    while len(meshtastic_id) < 8:
                        meshtastic_id = "0" + meshtastic_id
                    eud.meshtastic_id = int(meshtastic_id, 16)
                elif not eud.meshtastic_id and eud.platform == "Meshtastic":
                    try:
                        eud.meshtastic_id = int(takv.attrs["meshtastic_id"], 16)
                    except:
                        meshtastic_id = "{:x}".format(int.from_bytes(os.urandom(4), "big"))
                        while len(meshtastic_id) < 8:
                            meshtastic_id = "0" + meshtastic_id
                        eud.meshtastic_id = int(meshtastic_id, 16)

                # Get the Meshtastic device's mac address or generate a random one for TAK EUDs
                if takv and "macaddr" in takv.attrs:
                    eud.meshtastic_macaddr = takv.attrs["macaddr"]
                else:
                    eud.meshtastic_macaddr = base64.b64encode(os.urandom(6)).decode("ascii")

                if __group:
                    eud.team_id = team.id
                    eud.team_role = bleach.clean(__group.attrs["role"])

                try:
                    db.session.add(eud)
                    db.session.commit()
                except sqlalchemy.exc.IntegrityError:
                    db.session.rollback()
                    db.session.execute(
                        update(EUD).where(EUD.uid == eud.uid).values(**eud.serialize())
                    )
                    db.session.commit()

                # If the RabbitMQ channel is open, publish the EUD info to socketio to be displayed on the web UI map.
                # Also save the EUD's info for on_channel_open to publish
                self.eud = eud
                if self.rabbit_channel:
                    message = {
                        "method": "emit",
                        "event": "eud",
                        "data": eud.to_json(),
                        "namespace": "/socket.io",
                        "room": None,
                        "skip_sid": None,
                        "callback": None,
                        "binary": False,
                        "host_id": uuid.uuid4().hex,
                    }
                    self.rabbit_channel.basic_publish(
                        exchange="flask-socketio",
                        routing_key="",
                        body=json.dumps(message).encode(),
                        properties=pika.BasicProperties(
                            expiration=self.app.config.get("OTS_RABBITMQ_TTL")
                        ),
                    )

    def unbind_rabbitmq_queues(self):
        if (
            self.uid
            and self.rabbit_channel
            and not self.rabbit_channel.is_closing
            and not self.rabbit_channel.is_closed
        ):
            self.rabbit_channel.queue_unbind(
                queue=self.uid, exchange="missions", routing_key="missions"
            )
            self.rabbit_channel.queue_unbind(queue=self.uid, exchange="groups")

            for bind in self.bound_queues:
                self.rabbit_channel.queue_unbind(
                    exchange=bind["exchange"], queue=bind["queue"], routing_key=bind["routing_key"]
                )
