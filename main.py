from flask import Flask
from app.api.routes import api
from config.config import Config

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    
    # Register blueprints
    app.register_blueprint(api, url_prefix='/api')  # This adds /api prefix
    
    return app

# Expose the app instance for Gunicorn (standard pattern)
app = create_app()

if __name__ == '__main__':
    app = create_app()
    # app.run(host='0.0.0.0', port=8000, debug=True)
    app.run(debug=True)  # Debug mode for development
    # Note: In production, consider using a WSGI server like Gunicorn or uvicorn
    # and set debug=False for security reasons.