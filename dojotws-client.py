import argparse
from config import CONFIG
from dojotapi.api import DojotAPI
from utils import Utils
import websocket
import threading
from time import sleep


def on_message(ws, message):
    logger.info("message received")
    logger.info(message)


def on_close(ws):
    logger.warning("### closed ###")


def get_connection_string(topic, fields, where, secure):
    prefix = 'wss' if secure else 'ws'
    connectionString = "{0}/kafka-ws/v1/topics/{1}?ticket={2}".format(CONFIG['dojot']['url'].replace('http', prefix, 1),
                                                                       topic, ticket)
    if fields is None:
        logger.info("no fields selector available")
    else:
        connectionString += '&fields=' + fields

    if where is None:
        logger.info("no where condition available")
    else:
        connectionString += '&where=' + where
    return connectionString


if __name__ == "__main__":
    # Building the argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--topic", help="topic")
    parser.add_argument("--fields", help="fields selector")
    parser.add_argument("--where", help='where condition')
    parser.add_argument("-s", "--secure", help='use TLS', action="store_true")
    parser.add_argument("--ticket", help='use  ticket')
    args = parser.parse_args()
    topic = args.topic
    fields = args.fields
    where = args.where

    logger = Utils.create_logger("main")

    logger.info("Getting JWT")
    jwt = DojotAPI.get_jwt()
    logger.info("Getting ticket")
    ticket = DojotAPI.get_ws_ticket(jwt) if args.ticket is None else args.ticket
    logger.info("Ticket is : " + ticket)
    tenant = CONFIG['app']['tenant']
    if topic is None:
        topic = tenant
    websocket.enableTrace(True)

    #get ws connection string
    connectionString = get_connection_string(topic, fields, where, args.secure)

    logger.info(f'Connection string is {connectionString}')
    #connectionString = "ws://echo.websocket.org/"

    #sslopt
    sslopt = {"ca_certs": "./cert/CA.crt",
              "certfile": "./cert/client.crt",
              "keyfile": "./cert/client.key",
              "check_hostname": False} if args.secure else None

    ws = websocket.WebSocketApp(connectionString, on_message=on_message, on_close=on_close)
    wst = threading.Thread(target=ws.run_forever, kwargs={'sslopt': sslopt})
    wst.daemon = True
    try:
        logger.info("WS thread starting")
        wst.start()
        logger.info("WS thread started")
        conn_timeout = 5
        while not ws.sock.connected and conn_timeout:
            sleep(1)
            conn_timeout -= 1
        logger.info("WS connection done")
        logger.info("Waiting for keyboard interrupt...")
        while True:
            input()

    except KeyboardInterrupt:
        print("Ctrl+C pressed")
        ws.close()
