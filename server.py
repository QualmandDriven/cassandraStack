#!/usr/bin/env python


""" REST API mit Hilfe von Flask.
    Verbindet sich mit einer Cassandra-Datenbank.
    Schnittstelle hat folgende Endpunkte:
    / GET
    /create GET
    /drop GET
    /messages/create GET
    /messages GET
    /channels/<channel_id>/messages GET|POST
    /users GET
    /users/create GET
    /users/login POST
    /users/register POST
     
    Bei einem neuen Cluster kann mittels der folgenden Aufrufe
    der Keyspace und die Tabelle initalisiert werden:
    /create -> /messages/create -> /users/create
    """

# Standard-Logger
import logging

# Webframework und REST Methoden
from flask import Flask, jsonify, request

# Objekte von Cassandra-Treiber laden
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, BatchStatement

# UUID ist fuer Cassandra praktisch, da Keys normalerweise als UUID dargestellt werden
import uuid

# Logger konfigurieren
log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Keyspace fuer Cassandra festlegen
KEYSPACE = "socialmessagekeyspace"

# Webframework erzeugen
app = Flask(__name__)


def get_cassandra_session():
    """ Laedt das Cluster und erstellt eine Session
    Sobald die Session vorhanden ist, wird der Keyspace gesetzt """

    log.info("Get cluster")
    cluster = Cluster(['cassandradb1'])
    log.info("Connect to cluster")
    session = cluster.connect()
    log.info("setting keyspace...")
    session.set_keyspace(KEYSPACE)

    return session

@app.route('/')
def index():
    """ Index Uri von Web API """
    return "API is live!"

@app.route('/create', methods=["GET"])
def createKeySpace():
    """ Erstellt den angegebenen Keyspace, wenn noch nicht vorhanden. """

    log.info("creating keyspace...")
    cluster = Cluster(['cassandradb1'])
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)

    return "Created", 201

@app.route('/drop', methods=["GET"])
def dropKeySpace():
    """ Loescht den angegebenen Keyspace. """

    session = get_cassandra_session()
    session.execute("DROP KEYSPACE IF EXISTS " + KEYSPACE)

    return "OK", 200

@app.route('/channels/<int:channel_id>/messages', methods=["GET"])
def get_channel_messages(channel_id):
    """ Holt fuer einen bestimmten Kanal die Nachrichten aus der Datenbank"""

    session = get_cassandra_session()

    future = session.execute_async("SELECT * FROM messages WHERE channel_id=%s", (channel_id,))

    try:
        rows = future.result()
    except Exception:
        log.exeception()

    messages = []

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
    """ Einem bestimmten Kanal eine Nachricht hinzufuegen.
        author_id und message sind als POST-Argument notwendig.
    """

    if not request.json or not request.json['author_id'] or not request.json['message']:
        abort(400)

    session = get_cassandra_session()

    author_id = request.json['author_id']
    message = request.json['message']

    insert_message = session.prepare("""
        INSERT INTO messages (channel_id, message_id, author_id, message) 
                      VALUES (?, now(), ?, ?)""")

    # QUORUM ist nur moeglich, wenn es auch mehere Nodes gibt
    #batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    batch.add(insert_message, (channel_id, uuid.UUID(author_id), message))
    session.execute(batch)

    return "", 201

@app.route('/messages', methods=["GET"])
def get_all_messages():
    """ Alle Nachrichten aus der Datenbank laden. """

    session = get_cassandra_session()

    future = session.execute_async("SELECT * FROM messages")

    try:
        rows = future.result()
    except Exception:
        log.exeception()

    messages = []

    for row in rows:
        messages.append({
            'channel_id': row.channel_id,
            'message_id': row.message_id,
            'author_id': row.author_id,
            'message': row.message
        })

    return jsonify({'messages': messages}), 200

@app.route('/messages/create', methods=["GET"])
def messages_create():
    """ Erstellt die Tabelle 'messages' und initialisiert diese mit Default-Werte. """

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

    # CQL unterstuetzt kein INSERT mit mehreren VALUES
    # Dafuer gibt es das BatchStatement, damit koennen
    # mehrere Statements auf einmal abgesetzt werden.
    batch = BatchStatement()
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), a8098c1a-f86e-11da-bd1a-00112444be1e, 'Hi there')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), a8098c1a-f86e-11da-bd1a-00112444be1e, 'Someone in here')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), ab398c12-f86e-23da-bd1a-aabb2233be1e, 'Hey, yeah sure')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), a8098c1a-f86e-11da-bd1a-00112444be1e, 'Cool :) What is up man?')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), ab398c12-f86e-23da-bd1a-aabb2233be1e, 'I am writing a little API...')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), a8098c1a-f86e-11da-bd1a-00112444be1e, 'What is the API about?')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), ab398c12-f86e-23da-bd1a-aabb2233be1e, 'Connecting to a Cassandra Database')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), a8098c1a-f86e-11da-bd1a-00112444be1e, 'Oh wow sound interesting!')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), ab398c12-f86e-23da-bd1a-aabb2233be1e, 'Yeah, it is a bit different but I am slowly getting it')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), a8098c1a-f86e-11da-bd1a-00112444be1e, 'Is it very different?')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), ab398c12-f86e-23da-bd1a-aabb2233be1e, 'From the outside no, but if you get deeper it is very different.')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (2, now(), a8098c1a-f86e-11da-bd1a-00112444be1e, 'Hey, someone in this channel?')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), ab398c12-f86e-23da-bd1a-aabb2233be1e, 'But you get it eventually!')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (1, now(), a8098c1a-f86e-11da-bd1a-00112444be1e, 'Looks like no one is in here...')"))
    batch.add(SimpleStatement("INSERT INTO messages (channel_id, message_id, author_id, message) VALUES (3, now(), ab398c12-f86e-23da-bd1a-aabb2233be1e, 'Hey, what is this channel about?')"))
    session.execute(batch)

    return "Table and Messages created", 201

@app.route('/users', methods=["GET"])
def get_users():
    """ Alle Benutzer aus der Datenbank laden. """

    session = get_cassandra_session()

    future = session.execute_async("SELECT user_id, username, email FROM users")

    try:
        rows = future.result()
    except Exception:
        log.exeception()

    users = []

    for row in rows:
        users.append({
            'user_id': row.user_id,
            'username': row.username,
            'email': row.email
        })

    return jsonify({'users': users}), 200

@app.route('/users/login', methods=["POST"])
def user_login():
    """ Fuehrt einen login durch, d.h. Passwoerter werden verglichen.
        username und password muessen als POST-Argument uebergeben werden.
        Wenn das Passwort aus dem Argument und der Datenbank übereinstimmt wird ein OK gesendet,
        ansonsten ein Unauthorized. """

    if not request.json or not request.json['username'] or not request.json['password']:
        abort(400)

    session = get_cassandra_session()

    username = request.json['username']
    password = request.json['password']

    results = session.execute("SELECT user_id, username, email, password FROM users WHERE username=%s", (username,))
    row = results[0]

    if not row:
        return "", 401

    if row.password != password:
        return "", 401

    return jsonify({'user_id': row.user_id, 'username': row.username, 'email': row.email}), 200

@app.route('/users/register', methods=["POST"])
def user_register():
    """ Registriert einen neuen Benutzer in der Datenbank.
        Als POST-Argumente werden username, password und email erwartet. """

    if not request.json or not request.json['username'] or not request.json['password'] or not request.json['email']:
        abort(400)

    session = get_cassandra_session()

    username = request.json['username']
    email = request.json['email']
    password = request.json['password']

    insert_user = session.prepare("INSERT INTO users (user_id, username, email, password) VALUES (now(), ?, ?, ?)")

    # QUORUM ist nur möglich, wenn es auch mehere Nodes gibt
    #batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    batch.add(insert_user, (username, email, password))
    session.execute(batch)

    return "Registered", 201

@app.route('/users/create', methods=["GET"])
def users_create():
    """ Erstellt die Tabelle users in der Datenbank.
        Zudem wird diese mit Default-Werten initialisiert. """

    session = get_cassandra_session()

    log.info("setting keyspace...")
    session.set_keyspace(KEYSPACE)

    log.info("creating table...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id uuid,
            username text,
            email text,
            password text,
            PRIMARY KEY (username)
        )
        """)

    batch = BatchStatement()
    batch.add(SimpleStatement("INSERT INTO users (user_id, username, email, password) VALUES (now(), 'Alex', 'a.scholli@mail.de', 'alex')"))
    batch.add(SimpleStatement("INSERT INTO users (user_id, username, email, password) VALUES (now(), 'Bianca','b.name@mail.de', 'bianca')"))
    session.execute(batch)

    return "Table and Users created", 201

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
