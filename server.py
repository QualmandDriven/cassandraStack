#!/usr/bin/env python

import logging

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, BatchStatement

from flask import Flask, jsonify, request

import uuid

app = Flask(__name__)

KEYSPACE = "testkeyspace"

def get_cassandra_session():
    log.info("Get cluster")
    cluster = Cluster(['cassandra_db'])
    log.info("Connect to cluster")
    session = cluster.connect()
    log.info("setting keyspace...")
    session.set_keyspace(KEYSPACE)

    return session

@app.route('/')
def index():
    return "API is live!"

@app.route('/channels/<int:channel_id>/messages', methods=["GET"])
def get_channel_messages(channel_id):
    session = get_cassandra_session()

    future = session.execute_async("SELECT * FROM messages WHERE channel_id=" + str(channel_id))

    try:
        rows = future.result()
    except Exception:
        log.exeception()

    messages =  []

    for row in rows:
        messages.append({
            'channel_id': row.channel_id,
            'message_id': row.message_id,
            'author_id': row.author_id,
            'message': row.message
        })

    return jsonify({'messages': messages}), 200

@app.route('/channels/<int:channel_id>/messages', methods=["POST"])
def post_channel_messages(channel_id):
    if not request.json or not request.json['author_id'] or not request.json['message']:
        abort(400)

    session = get_cassandra_session()

    insert_message = session.prepare("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (?, now(), ?, ?)")

    author_id = request.json['author_id']
    message = request.json['message']

    # QUORUM ist nur möglich, wenn es auch mehere Nodes gibt
    #batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    batch.add(insert_message, (channel_id, uuid.UUID(author_id), message))
    session.execute(batch)

    return "", 201

@app.route('/messages', methods=["GET"])
def messages():
    session = get_cassandra_session()

    future = session.execute_async("SELECT * FROM messages")

    try:
        rows = future.result()
    except Exception:
        log.exeception()

    messages =  []

    for row in rows:
        messages.append({
            'channel_id': row.channel_id,
            'message_id': row.message_id,
            'author_id': row.author_id,
            'message': row.message
        })

    return jsonify({'messages': messages}), 200

@app.route('/messages/insert', methods=["GET"])
def messages_insert_all():
    session = get_cassandra_session()

    log.info("setting keyspace...")
    session.set_keyspace(KEYSPACE)

    log.info("creating table...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            channel_id bigint,
            message_id uuid,
            author_id uuid,
            message text,
            PRIMARY KEY (channel_id, message_id)
        ) WITH CLUSTERING ORDER BY (message_id DESC)
        """)

    batch = BatchStatement()
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), a8098c1a-f86e-11da-bd1a-00112444be1e, 'hi')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), a8098c1a-f86e-11da-bd1a-00112444be1e, 'hi')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), ab398c12-f86e-23da-bd1a-aabb2233be1e, 'hi1')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), a8098c1a-f86e-11da-bd1a-00112444be1e, 'hi2')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), ab398c12-f86e-23da-bd1a-aabb2233be1e, 'hi3')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), a8098c1a-f86e-11da-bd1a-00112444be1e, 'hi4')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), ab398c12-f86e-23da-bd1a-aabb2233be1e, 'hi5')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (2, now(), a8098c1a-f86e-11da-bd1a-00112444be1e, 'hi6')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), ab398c12-f86e-23da-bd1a-aabb2233be1e, 'hi7')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), a8098c1a-f86e-11da-bd1a-00112444be1e, 'hi8')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), ab398c12-f86e-23da-bd1a-aabb2233be1e, 'hi9')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (2, now(), a8098c1a-f86e-11da-bd1a-00112444be1e, 'hi10')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), ab398c12-f86e-23da-bd1a-aabb2233be1e, 'hi11')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), a8098c1a-f86e-11da-bd1a-00112444be1e, 'hi12')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (3, now(), ab398c12-f86e-23da-bd1a-aabb2233be1e, 'hi13')"))
    session.execute(batch)

    return "Table and Messages created", 201

@app.route('/messages/create', methods=["GET"])
def createKeySpace():
    log.info("creating keyspace...")
    cluster = Cluster(['cassandra_db'])
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)

    return "Created", 201

@app.route('/messages/drop', methods=["GET"])
def messages_insert():
    session = get_cassandra_session()
    session.execute("DROP KEYSPACE IF EXISTS " + KEYSPACE)

    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
