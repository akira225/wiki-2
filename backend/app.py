from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta
import bcrypt
from flask_jwt_extended import create_access_token
from flask_jwt_extended import get_jwt_identity
from flask_jwt_extended import jwt_required
from flask_jwt_extended import JWTManager
import time
from api_docks import *
from flasgger import Swagger, swag_from
from WikiRpcClient import WikiRpcClient

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///sqlite/baza.db'
app.config["JWT_SECRET_KEY"] = "Yijf341isdHYG66YT757huPgh86788jioo38aldsFDDRY7867"
db = SQLAlchemy(app)
jwt = JWTManager(app)
swagger = Swagger(app)
wiki_rpc = WikiRpcClient()


def find_path(a, b):
    return wiki_find_path(a, b)


def check_limit(user):
    if user.subscription == 'pro':
        return True
    elif user.subscription == 'free':
        last_request = History.query.filter_by(user_id=user.id).order_by(History.req_date.desc()).first()
        if last_request and (datetime.utcnow() - last_request.req_date).days >= 1:
            return True
        else:
            return False
    else:
        last_request = History.query.filter_by(user_id=user.id).order_by(History.req_date.desc()).limit(19).all()
        if last_request and len(last_request) > 0:
            if len(last_request) < 19 or (datetime.utcnow() - last_request[-1].req_date).days >= 1:
                return True
            else:
                return False
        else:
            return True


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String, unique=True, nullable=False)
    hashed_password = db.Column(db.String, nullable=False)
    subscription = db.Column(db.String, nullable=True, default="free")  # [free, standard, pro]


class History(db.Model):
    request_id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    A = db.Column(db.String, nullable=False)
    B = db.Column(db.String, nullable=False)
    path = db.Column(db.String, nullable=False)
    req_date = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class Ban(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    article = db.Column(db.String, nullable=False)


@app.route('/', methods=['GET'])
def success_response():
    return jsonify({'status': 'ok'})


@app.route('/subscribe/<subscription>', methods=['PUT'])
@jwt_required()
@swag_from(subscribe_swag)
def subscribe(subscription):
    if not subscription.lower() in ['free', 'standard', 'pro']:
        return jsonify({'msg': "Wrong subscription type"}), 400
    else:
        user = User.query.filter_by(username=get_jwt_identity()).first()
        user.subscription = subscription
        db.session.add(user)
        db.session.commit()
        return {'username': user.username, 'subscription': subscription}, 200


@app.route('/register', methods=['POST'])
@swag_from(registration_swag)
def register_user():
    username = request.json.get('username')
    password = request.json.get('password')
    if not username:
        return jsonify({'msg': "No username provided"}), 400
    if not password:
        return jsonify({'msg': "No password provided"}), 400
    if User.query.filter_by(username=username).first():
        return jsonify({'msg': "User with this name already exists"}), 400
    pw_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    db.session.add(User(username=username, hashed_password=pw_hash))
    db.session.commit()
    access_token = create_access_token(identity=username, expires_delta=False)
    return jsonify({'username': username, 'token': access_token}), 201


@app.route('/token', methods=['GET'])
@app.route('/login', methods=['GET'])
@swag_from(login_swag)
def generate_access_token():
    username = request.json.get('username')
    password = request.json.get('password')
    if not username:
        return jsonify({'msg': "No username provided"}), 400
    if not password:
        return jsonify({'msg': "No password provided"}), 400
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'msg': "User does not exist"}), 400
    if bcrypt.checkpw(password.encode('utf-8'), user.hashed_password.encode('utf-8')):
        return jsonify({'token': create_access_token(identity=username, expires_delta=False), 'username': user.username}), 200
    else:
        return jsonify({'msg': "Password is incorrect"}), 400


@app.route('/history', methods=['GET'])
@jwt_required()
@swag_from(history_swag)
def user_history():
    user = User.query.filter_by(username=get_jwt_identity()).first()
    history = History.query.filter_by(user_id=user.id).order_by(History.req_date.desc()).all()
    to_return = list()
    for item in history:
        to_return.append({"date": item.req_date, "A": item.A, "B": item.B, "path": item.path})
    return jsonify({'history': to_return}), 200


@app.route('/path', methods=['GET'])
@jwt_required()
@swag_from(path_swag)
def process_path():
    A = request.json.get('A')
    B = request.json.get('B')
    if not A or not B:
        return jsonify({'msg': "Incorrect data"}), 400
    user = User.query.filter_by(username=get_jwt_identity()).first()
    if not check_limit(user):
        return jsonify({'msg': "Your daily request limit has expired"}), 400
    path = wiki_rpc.call_path(A, B)
    if path.get('success'):
        db.session.add(History(user_id=user.id, A=A, B=B, path=path.get('path')))
        db.session.commit()
        return {'path': {'A': A, 'B': B, 'path': path.get('path')}}, 200
    else:
        return jsonify({'msg': "Error processing path"}), 400


@app.route('/ban', methods=['POST'])
@jwt_required()
@swag_from(ban_swag)
def ban_node():
    article = request.json.get('article')
    if not article:
        return jsonify({"msg": "Missing required attribute \'article\'"}), 400
    title = wiki_rpc.call_validate(article).get('title')
    if not title:
        return jsonify({"msg": "Article does not exist"}), 400
    db.session.add(Ban(article=title))
    db.session.commit()
    return jsonify({'msg': 'success'})


if __name__ == '__main__':
    time.sleep(12)
    app.run()
