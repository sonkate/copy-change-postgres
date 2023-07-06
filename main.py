import psycopg2
from dotenv import load_dotenv
import os
import json



load_dotenv()
_host = os.environ.get('HOST')
_port = os.environ.get('PORT')
_dbname = os.environ.get('DBNAME')
# _user = os.environ.get('USER')
_user = 'root'
_password = os.environ.get('PASSWORD')
_target_dbname = os.environ.get('TARGET_DBNAME')

# Connect to the source PostgreSQL database
source_conn = psycopg2.connect(
    host= _host,
    port= _port,
    dbname= _dbname,                                                             
    user= _user,
    password= _password
)

# Connect to the target PostgreSQL database
target_conn = psycopg2.connect(
    host= _host,
    port= _port,
    dbname= _target_dbname,
    user= _user,
    password= _password
)

# Create a replication cursor on the source database
cur = source_conn.cursor()


while(True):
    cur.execute("SELECT * FROM pg_logical_slot_get_changes('jpos_slot_listen', NULL, NULL);")

    if cur:
        for message in cur:
            data = json.loads(message[2])
            # Extract kind, schema, and columnvalues
            final_data = data['change'][0]
            kind = final_data['kind']
            schema = final_data['schema']
            columnvalues = final_data['columnvalues']
            columnnames = final_data['columnnames']
            columnvalues.pop()
            del columnvalues[37]
            del columnvalues[36]
            del columnvalues[32]
            source_id = columnvalues[0]
            columnvalues+=[None,None,None,None,None,None,None]

            result_arr = []
            # print(message)
            # print(kind, schema, columnvalues, len(columnvalues))

            # target cursor
            target_cur = target_conn.cursor()
            
            if kind == 'insert':
                insert_sql = "INSERT INTO transaction_model(source_id,auth_id_response, bank_code, batch_no, card_no, card_type, created_unix_time, customer_id, icc_system_data, invoice_no, is_reversal, is_settle, is_void, mid, original_transaction_date, request_amount, response_code, retrieval_ref_no, reversal_txn_id, reversal_unix_time, serial_no, settle_unix_time, signature_data, tid, tip, transaction_type, txn_id, updated_unix_time, void_txn_id, void_unix_time, card_hash, routing_bank_code, merchant_industry, pos_entry_mode, system_trace_no, is_clear_batch, transaction_id, is_offline, is_refund,	write_date,	create_date, source_type, write_uid) VALUES %s".format(schema)
                target_cur.execute(insert_sql, [tuple(columnvalues)]) 
                target_conn.commit()
            elif kind == 'update':
                for i in range(len(columnnames)):
                    if columnnames[i] == 'id':
                        continue
                    if (columnnames[i] == "fee_percentage") | (columnnames[i] == "is_complete") | (columnnames[i] == "complete_txn_id") | (columnnames[i] == "complete_unix_time") :
                        continue
                    if type(columnvalues[i]) == str:
                        result_arr.append( f"{columnnames[i]} = '{columnvalues[i]}'")
                    else:
                        result_arr.append( f"{columnnames[i]} = {columnvalues[i]}")
                params = ','.join(str(x) for x in result_arr)
                params = params.replace('None', 'null')
                update_sql = f"UPDATE transaction_model SET {params} WHERE source_id = {source_id};"
                # print(update_sql)
                target_cur.execute(update_sql)
            
else:
    source_conn.close()
    target_conn.close()
