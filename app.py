from flask import Flask, render_template, request, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime


# Initialize Flask app
app = Flask(__name__)

# Create a connection string for SQLAlchemy and SQL Server (using PyODBC)
server = 'localhost'
database = 'Chicago_PD'
username = 'sa'
password = 'reallyStrongPwd123'

conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

# Configuring the SQLAlchemy database URI
app.config["SQLALCHEMY_DATABASE_URI"] = conn_str
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# Initialize the database
db = SQLAlchemy(app)

# Models

class Offender(db.Model):
    __tablename__ = 'offender'
    __table_args__ = {'schema': 'Rogers_Park'}
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    dob = db.Column(db.Date, nullable=False)
    address = db.Column(db.String(200), nullable=True)
    phone = db.Column(db.String(15), nullable=True)
    race = db.Column(db.String(50), nullable=True)  # New field for Race
    sex = db.Column(db.String(10), nullable=True)   # New field for Sex
    ethnicity = db.Column(db.String(50), nullable=True)  # New field for Ethnicity
    ssn = db.Column(db.String(9), nullable=False)  # New field for Ethnicity

    bookings = db.relationship('Booking', backref='offender', lazy=True)

class Offense(db.Model):
    __tablename__ = 'offense'
    __table_args__ = {'schema': 'Rogers_Park'}
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(50))
    description = db.Column(db.Text)

    bookings = db.relationship('BookingOffense', backref='offense', lazy=True)

class Booking(db.Model):
    __tablename__ = 'booking'
    __table_args__ = {'schema': 'Rogers_Park'}
    id = db.Column(db.Integer, primary_key=True)
    offender_id = db.Column(db.Integer, db.ForeignKey('Rogers_Park.offender.id'), nullable=False)
    booking_date = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    offenses = db.relationship('BookingOffense', backref='booking', lazy=True)

class BookingOffense(db.Model):
    __tablename__ = 'booking_offense'
    __table_args__ = {'schema': 'Rogers_Park'}
    booking_id = db.Column(db.Integer, db.ForeignKey('Rogers_Park.booking.id'), primary_key=True)
    offense_id = db.Column(db.Integer, db.ForeignKey('Rogers_Park.offense.id'), primary_key=True)

# Lincoln Park Models !
class lincoln_Offender(db.Model):
    __tablename__ = 'lincoln_Offender'
    __table_args__ = {'schema': 'Lincoln_Park'}
    id = db.Column(db.Integer, primary_key=True)
    first_name = db.Column(db.String(100), nullable=False)
    last_name = db.Column(db.String(100), nullable=False)
    height = db.Column(db.Float, nullable=True)  # in cm
    weight = db.Column(db.Float, nullable=True)  # in kg
    dob = db.Column(db.Date, nullable=False)
    address = db.Column(db.String(200), nullable=True)
    phone = db.Column(db.String(15), nullable=True)
    race = db.Column(db.String(50), nullable=True)  # New field for Race
    sex = db.Column(db.String(10), nullable=True)   # New field for Sex
    ethnicity = db.Column(db.String(50), nullable=True)  # New field for Ethnicity
    ssn = db.Column(db.String(9), nullable=False)

    bookings = db.relationship('lincoln_Booking', backref='lincoln_Offender', lazy=True)

class lincoln_Offense(db.Model):
    __tablename__ = 'lincoln_Offense'
    __table_args__ = {'schema': 'Lincoln_Park'}
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(50))
    description = db.Column(db.Text)

    bookings = db.relationship('lincoln_BookingOffense', backref='lincoln_Offense', lazy=True)

class lincoln_Booking(db.Model):
    __tablename__ = 'lincoln_Booking'
    __table_args__ = {'schema': 'Lincoln_Park'}
    id = db.Column(db.Integer, primary_key=True)
    offender_id = db.Column(db.Integer, db.ForeignKey('Lincoln_Park.lincoln_Offender.id'), nullable=False)
    booking_date = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    offenses = db.relationship('lincoln_BookingOffense', backref='lincoln_Booking', lazy=True)

class lincoln_BookingOffense(db.Model):
    __tablename__ = 'lincoln_BookingOffense'
    __table_args__ = {'schema': 'Lincoln_Park'}
    booking_id = db.Column(db.Integer, db.ForeignKey('Lincoln_Park.lincoln_Booking.id'), primary_key=True)
    offense_id = db.Column(db.Integer, db.ForeignKey('Lincoln_Park.lincoln_Offense.id'), primary_key=True)


# Routes

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/book_offender', methods=['GET', 'POST'])
def book_offender():
    if request.method == 'POST':
        # Capture form data, including race, sex, and ethnicity
        name = request.form['name']
        dob = request.form['dob']
        address = request.form['address']
        phone = request.form['phone']
        ssn = request.form['ssn']
        race = request.form['race']  # Race from the dropdown
        sex = request.form['sex']    # Sex from the dropdown
        ethnicity = request.form['ethnicity']  # Ethnicity from the dropdown
        offenses = request.form.getlist('offenses')  # List of offense IDs

        # Create a new offender with the captured details
        new_offender = Offender(
            name=name,
            dob=dob,
            address=address,
            phone=phone,
            ssn=ssn,
            race=race,  # Store race
            sex=sex,    # Store sex
            ethnicity=ethnicity  # Store ethnicity
        )
        
        # Add offender to the database
        db.session.add(new_offender)
        db.session.commit()

        # Create a new booking
        new_booking = Booking(offender_id=new_offender.id, booking_date=datetime.utcnow())
        db.session.add(new_booking)
        db.session.commit()

        # Link selected offenses to the booking
        for offense_id in offenses:
            booking_offense = BookingOffense(booking_id=new_booking.id, offense_id=int(offense_id))
            db.session.add(booking_offense)

        # Commit all changes
        db.session.commit()

        # Redirect back to the home page after successful booking
        return redirect(url_for('index'))

    # For GET request, retrieve all offenses and render the booking form
    offenses = Offense.query.all()
    return render_template('booking_form.html', offenses=offenses)

@app.route('/book_offender_lincoln_park', methods=['GET', 'POST'])
def book_offender_lincoln_park():
    if request.method == 'POST':
        # Capture form data, including race, sex, and ethnicity
        first_name = request.form['first_name']
        last_name = request.form['last_name']
        dob = request.form['dob']
        address = request.form['address']
        height = request.form['height']
        weight = request.form['weight']
        phone = request.form['phone']
        ssn = request.form['ssn']
        race = request.form['race']  # Race from the dropdown
        sex = request.form['sex']    # Sex from the dropdown
        ethnicity = request.form['ethnicity']  # Ethnicity from the dropdown
        offenses = request.form.getlist('offenses')  # List of offense IDs

        # Create a new offender with the captured details
        new_offender = lincoln_Offender(
            first_name=first_name,
            last_name=last_name,
            dob=dob,
            address=address,
            height=height,
            weight=weight,
            phone=phone,
            ssn=ssn,
            race=race,  # Store race
            sex=sex,    # Store sex
            ethnicity=ethnicity  # Store ethnicity
        )
        
        # Add offender to the database
        db.session.add(new_offender)
        db.session.commit()

        # Create a new booking
        new_booking = lincoln_Booking(offender_id=new_offender.id, booking_date=datetime.utcnow())
        db.session.add(new_booking)
        db.session.commit()

        # Link selected offenses to the booking
        for offense_id in offenses:
            booking_offense = lincoln_BookingOffense(booking_id=new_booking.id, offense_id=int(offense_id))
            db.session.add(booking_offense)

        # Commit all changes
        db.session.commit()

        # Redirect back to the home page after successful booking
        return redirect(url_for('index'))

    # For GET request, retrieve all offenses and render the booking form
    lincoln_offenses = lincoln_Offense.query.all()
    return render_template('lincoln_booking_form.html', offenses=lincoln_offenses)

if __name__ == '__main__':
    app.run(debug=True)
