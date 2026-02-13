from kafka import KafkaConsumer
import json
import mysql.connector
import uuid
import time

broker = 'my-kafka.gran4u-dev.svc.cluster.local:9092'
topic = 'dbserver1.eurynome.CUSTOMER'

db_config = {
    'user': 'mygreaterpuser',
    'password': 'mygreaterp',
    'host': 'mygreaterp-db.gran4u-dev.svc.cluster.local',
    'port': 3306,
    'database': 'mygreaterp'
}

# --- Création du consumer ---
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[broker],
    sasl_mechanism='PLAIN',
    security_protocol='SASL_PLAINTEXT',
    sasl_plain_username='user1',
    sasl_plain_password='YQYw853vKb',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# --- Connexion à la base de données ---
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

print("Synchronisation en cours...")

for message in consumer:
    data = message.value
    before = data.get('payload', {}).get('before')
    after = data.get('payload', {}).get('after')

    # CAS INSERT 
    if before is None and after is not None:
        c_id = str(uuid.uuid4())
        a_id = str(uuid.uuid4())
        l_id = str(uuid.uuid4())

        try:
            # Insertion 
            cursor.execute("INSERT INTO contacts (id, first_name, last_name) VALUES (%s, %s, %s)", (c_id, after['prenom'], after['nom']))
            cursor.execute("INSERT INTO addresses (id, street, city, state) VALUES (%s, %s, %s, %s)", (a_id, after['adresse_rue'], after['adresse_ville'], after['adresse_region']))
            cursor.execute("INSERT INTO contacts_addresses (id, contact_id, address_id) VALUES (%s, %s, %s)", (l_id, c_id, a_id))
            
            # La table de référence 
            cursor.execute("INSERT INTO referential.customers_ref (eurynome_id, mygreaterp_id) VALUES (%s, %s)", (after['id'], c_id))
            
            conn.commit()
            print(f"Client {after['nom']} synchronisé.")
        except Exception as e:
            conn.rollback()
            print(f"Erreur: {e}")

    # CAS DELETE 
    elif after is None and before is not None:
        cursor.execute("SELECT mygreaterp_id FROM referential.customers_ref WHERE eurynome_id = %s", (before['id'],))
        row = cursor.fetchone()
        if row:
            cursor.execute("DELETE FROM contacts WHERE id = %s", (row[0],))
            cursor.execute("DELETE FROM referential.customers_ref WHERE eurynome_id = %s", (before['id'],))
            conn.commit()
            print(f"Client {before['id']} supprimé.")