from app import app, db

# Push application context and create the tables
with app.app_context():
    db.create_all()
    print("Tables created successfully!")
