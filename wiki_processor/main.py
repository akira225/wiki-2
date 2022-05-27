import wikipediaapi
import pika
import json
import sqlite3
import pickle
from State import State
import os
import time


RETRIES = 3


def on_request(ch, method, props, body):
    global state
    r = json.loads(body)
    if props.correlation_id + ".state" in os.listdir("./failed_requests"):
        with open(f"./failed_requests/{worker_uuid}.state", 'rb') as file:
            state = pickle.load(file)
        if state.retries > RETRIES + 1:
            ch.basic_publish(exchange='',
                             routing_key=props.reply_to,
                             properties=pika.BasicProperties(correlation_id=props.correlation_id),
                             body=json.dumps({"success": False, "msg": state.error}))
            ch.basic_ack(delivery_tag=method.delivery_tag)
            state = None
            return
    for j in range(RETRIES):
        try:
            if state is not None:
                if state.operation == 'validate':
                    response = {'title': article_title()}
                elif state.operation == 'path':
                    response = wiki_find_path()
                else:
                    response = {}
            else:
                state = State()
                state.operation = r.get('action')
                state.local_state = dict()
                state.body = r
                state.request_uuid = props.correlation_id
                if r.get('action') == 'validate':
                    state.local_state["article"] = r['data']['article']
                    response = {'title': article_title()}
                elif r.get('action') == 'path':
                    state.local_state["A"] = r['data']['A']
                    state.local_state["B"] = r['data']['B']
                    response = wiki_find_path()
                else:
                    response = {}
        except Exception as ex:
            state.retries += 1
            if j == RETRIES - 1:
                raise ex
        else:
            ch.basic_publish(exchange='',
                             routing_key=props.reply_to,
                             properties=pika.BasicProperties(correlation_id=props.correlation_id),
                             body=json.dumps(response))
            ch.basic_ack(delivery_tag=method.delivery_tag)
            state = None
            return


connection = None
state = None
db_connection = None
wiki_wiki = wikipediaapi.Wikipedia(
    language='ru',
    extract_format=wikipediaapi.ExtractFormat.WIKI
)
banned_articles = set()


def update_banned_articles():
    global banned_articles
    cursor = db_connection.cursor()
    cursor.execute("select article from ban")
    articles = cursor.fetchall()
    for article in articles:
        banned_articles.add(article[0])


def article_title():
    a = wiki_wiki.page(state.local_state["article"])
    if a.exists():
        state.local_state = {"exists": True}
        return a.displaytitle
    else:
        state.local_state = {"exists": False}
        return None


def wiki_find_path():
    global state
    global banned_articles
    if state.local_state.get("a") is None:
        state.local_state["a"] = wiki_wiki.page(state.local_state["A"])
        state.local_state["b"] = wiki_wiki.page(state.local_state["B"])
        if not state.local_state["a"].exists() or not state.local_state["b"].exists():
            return {'success': False, 'path': None}
        state.local_state["A"] = state.local_state["a"].displaytitle
        state.local_state["B"] = state.local_state["b"].displaytitle
        update_banned_articles()
        if state.local_state["A"] in banned_articles or state.local_state["B"] in banned_articles:
            return {'success': False, 'path': None}
        state.local_state["processed_links"] = dict()
        state.local_state["next_layer"] = [state.local_state["A"]]
        state.local_state["processed_links"][state.local_state["A"]] = None
        state.local_state["length"] = 1
        state.local_state["current_layer"] = list()
    while True:
        for article in state.local_state["next_layer"]:
            wiki_page = wiki_wiki.page(article)
            try:
                links = wiki_page.links
            except:
                continue
            for link in links.keys():
                if link == state.local_state["B"]:
                    state.local_state["processed_links"][link] = article
                    return {'success': True, 'path': restore_path(state.local_state["processed_links"], state.local_state["B"])}
                elif link in state.local_state["processed_links"] or link in banned_articles:
                    continue
                else:
                    state.local_state["processed_links"][link] = article
                    state.local_state["current_layer"].append(link)
        state.local_state["length"] += 1
        state.local_state["next_layer"] = state.local_state["current_layer"]
        state.local_state["current_layer"] = list()


def restore_path(graph, B):
    current = graph.get(B)
    path = [B]
    while current:
        path.insert(0, current)
        current = graph.get(current)
    res = f"{path[0]}"
    del path[0]
    for node in path:
        res += f" -> {node}"
    state.local_state["path"] = res
    return res

time.sleep(10)
if __name__ == "__main__":
    for i in range(RETRIES):
        try:
            db_connection = sqlite3.connect("./sqlite/baza.db")
            state = None
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='127.0.0.1'))
            channel = connection.channel()
            channel.queue_declare(queue='rpc_queue')
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)
            channel.start_consuming()
        except Exception as ex:
            state.error = str(ex)
            with open("./failed_requests/"+state.request_uuid+".state", "wb") as file:
                pickle.dump(state, file)
        finally:
            db_connection.close()
            if connection and not connection.is_closed:
                connection.close()
