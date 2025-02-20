from app import app, db, Offense

# Create offenses
offense1 = Offense(code="12345", description="Battery Theft")
offense2 = Offense(code="67890", description="Assault on people")
offense3 = Offense(code="23456", description="Car Theft")
offense4 = Offense(code="67810", description="Assault on Animal")
offense5 = Offense(code="10928", description="Threaten to kill")
offense6 = Offense(code="40002", description="Murder by AK47")

# Use the app context to interact with the database
with app.app_context():
    db.session.add(offense1)
    db.session.add(offense2)
    db.session.add(offense3)
    db.session.add(offense4)
    db.session.add(offense5)
    db.session.add(offense6)
    db.session.commit()

print("Offenses added successfully!")
