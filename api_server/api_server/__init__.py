from flask import Flask
from flask_restful import Api
from api_server.config import config
from api_server.resources.stream import StreamResource

def register_resources(app):
    api = Api(app) 
    api.add_resource(StreamResource, '/streams')

def create_app():
    app = Flask(__name__)
    app.config.from_object(config['development'])
    register_resources(app)
    return app

#if __name__ == '__main__':
#    app = create_app()
#    app.run()
#    app.run(debug=True,host='0.0.0.0',port=int(os.environ.get('PORT', 8080)))
