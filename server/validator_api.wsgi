import bottle
from bottle import response, request, post, get
from Crypto.Signature import PKCS1_v1_5 
from Crypto.Hash import SHA256 
from Crypto.PublicKey import RSA
import base64
import os.path

import os, json


class Config():

    _config = None

    def __init__(self):
        path = os.getenv('CONFIG')
        if not path:
            raise Exception("Environment variable CONFIG must be defined")
        if not os.path.exists(path):
            raise Exception("Environment variable CONFIG must point toward a "
                            "local file: %s" % path)

        class RaiiFile():
            def __init__(self, path):
                self.f = open(path, 'r')

            def __del__(self):
                self.f.close()

        f = RaiiFile(path)
        try:
            Config._config = json.load(f.f)
        except ValueError:
            raise Exception("Failed to parse JSON from file pointed by CONFIG")

    def __getattr__(self, name):
        parts = name.split(".")
        pointer = Config._config
        while len(parts) > 0:
            part = parts.pop(0)
            if part not in pointer:
                raise AttributeError(name + " not found in config")
            pointer = pointer[part]
        return pointer

    def __setattr__(self, name, value):
        self._config[name] = value

    def __contains__(self, key):
        return key in self._config


config = Config()


# #public key cipher
public_key = RSA.importKey(open(config.key).read())
signer = PKCS1_v1_5.new(public_key) 

@post("/verifier/activation_sign_in")
def post_activation_code():

    version = request.json['activation_code'][:3]
    if version == '001':
        code = request.json['activation_code'][3:]
    elif version == '002':
        code = request.json['activation_code'][4:]           
  
    email = request.json['email']
    email = email.encode('ascii', 'replace')

    try:
        code = base64.b64decode(code)
    except TypeError:
        response.status = 401
        return {}

    # Verify the signature
    digest = SHA256.new() 
    digest.update(email) 

    if signer.verify(digest, code):
        # create a file with the validation code in it
        with open(config.activation_key, 'w') as f:
            code = request.json['activation_code'].replace('\n', '')
            code = code.replace('\r', '')
            f.write(code + '\n')
            f.write(request.json['email'])

        response.status = 201
    else:
        response.status = 401

    return {}

@get("/verifier/activation")
def verify_current_authentication():
    if os.path.isfile(config.activation_key):
        response.status = 201
    else:
        response.status = 401
    return {}

application = bottle.default_app()