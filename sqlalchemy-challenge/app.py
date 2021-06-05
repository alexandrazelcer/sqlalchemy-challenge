
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect

from datetime import datetime as dt
from datetime import timedelta


from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(bind=engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def home():
    """List all available API routes."""
    return (
        f"Welcome to the Hawaii Climate Analysis API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start date/end date"
    )



@app.route("/api/v1.0/precipitation")
def precip():
    #open session
    session = Session(engine)

    """Return the precipitation data for the last year"""

    # Calculate the date 1 year ago from last date in database
    final_date = session.query(func.max(Measurement.date)).all()
    #convert results to datetime object
    final_date = dt.strptime(final_date[0][0], "%Y-%m-%d")
    #calculate date 12 months prior to final point
    year_ago = final_date - timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores
    precip_data = session.query(Measurement.date,Measurement.prcp).filter(Measurement.date > year_ago).all()

    #convert query results to dictionary
    data_dict = {}
    for date, rain in precip_data:
        data_dict.setdefault(date, []).append(rain)
    
    #close session
    session.close()

    #return jsonified results
    return jsonify(data_dict)

@app.route("/api/v1.0/stations")
def stations():
    #open session
    session = Session(engine)

    #query station data
    station_data = session.query(Station.station, Station.name,\
                                Station.latitude, Station.longitude,\
                                Station.elevation).all()

    #convert results into dictionary
    sta_dict = {a:{"name":b,
                   "latitude":c,
                   "longitude":d,
                   "elevation":e}
                for a,b,c,d,e in station_data}
    
    #close session
    session.close()

    #return jsonified results
    return jsonify(sta_dict)
    

@app.route("/api/v1.0/tobs")
def tobs():
    #open session
    session = Session(engine)

    # List the stations and the counts in descending order.
    active_stations = session.query(Measurement.station,func.count(Measurement.station))\
                                .group_by(Measurement.station)\
                                .order_by(func.count(Measurement.station).desc())\
                                .all()

    #identity most active station
    target_station = active_stations[0][0]
    
    #find last data point for target station
    final_date = session.query(func.max(Measurement.date))\
                        .filter(Measurement.station == target_station).all()
    #convert result to timedate object
    final_date = dt.strptime(final_date[0][0], "%Y-%m-%d")
    #Calculate the date 1 year ago from the last data point in the database
    year_ago = final_date - timedelta(days=365)

    #query the last 12 months of temp data for the target station
    temp_data = session.query(Measurement.date, Measurement.tobs)\
                        .filter(Measurement.date > year_ago)\
                        .filter(Measurement.station == target_station)\
                        .all()

    #convert results into dictionary
    temp_dict = dict(temp_data)

    #close session
    session.close()

    #return jsonified results
    return jsonify(temp_dict)

@app.route("/api/v1.0/<start>")
def start(start):
    #open session
    session = Session(engine)

    #return error if date is entered in incorrect format
    #----------------------------------------------------------
    try:
        #convert input string into date object
        start_date = dt.strptime(start, "%Y-%m-%d")
    except:
        return ({"error":"date must be in the form: YYYY-MM-DD"})
    #----------------------------------------------------------

    #return error if start date is outside the dataset
    #------------------------------------------------------------
    #define last data point
    final_date = session.query(func.max(Measurement.date)).all()
    #convert results to datetime object
    final_date = dt.strptime(final_date[0][0], "%Y-%m-%d")
    #return error if start date is outside the dataset
    if start_date > final_date:
        return ({"error":f"no data exists beyond {final_date}"})
    #---------------------------------------------------------------
    
    #query tempertures after start date
    results = session.query(func.min(Measurement.tobs),\
                            func.avg(Measurement.tobs),\
                            func.max(Measurement.tobs))\
                                .filter(Measurement.date > start_date).all()
    
    #unpack results
    TMIN, TAVG, TMAX = results[0]

    #create dictionary or resultant values
    results_dict = {"TMIN":TMIN,
                    "TAVG":TAVG,
                    "TMAX":TMAX}

    #close session
    session.close()

    #return jsonified response
    return jsonify(results_dict)

@app.route("/api/v1.0/<start>/<end>")
def start_end(start, end):
    #open session
    session = Session(engine)

    #ERROR HANDLING
    #====================================================================
    #return error if date is entered in incorrect format
    #----------------------------------------------------------
    try:
        #convert input string into date object
        start_date = dt.strptime(start, "%Y-%m-%d")
        end_date = dt.strptime(end, "%Y-%m-%d")
    except:
        return ({"error":"dates must be in the form: YYYY-MM-DD"})
    #----------------------------------------------------------

    #return error if start date is outside the dataset
    #------------------------------------------------------------
    #define last data point
    final_date = session.query(func.max(Measurement.date)).all()
   
    #convert results to datetime object
    final_date = dt.strptime(final_date[0][0], "%Y-%m-%d")
    
    #return error if either date is outside the dataset
    if (start_date > final_date) or (end_date > final_date) :
        return ({"error":f"no data exists beyond {final_date}"})
    #---------------------------------------------------------------
    
    #return error if start date is beyond end date
    #------------------------------------------------------------
    if start_date > end_date:
        return ({"error":f"start date cannot be greater than end date"})
    #------------------------------------------------------------
    #=======================================================================
    
    #query tempertures after start date
    results = session.query(func.min(Measurement.tobs),\
                            func.avg(Measurement.tobs),\
                            func.max(Measurement.tobs))\
                                .filter(Measurement.date > start_date)\
                                .filter(Measurement.date < end_date).all()
    
    #unpack results
    TMIN, TAVG, TMAX = results[0]

    #create dictionary or resultant values
    results_dict = {"TMIN":TMIN,
                    "TAVG":TAVG,
                    "TMAX":TMAX}

    #close session
    session.close()

    #return jsonified response
    return jsonify(results_dict)


if __name__ == "__main__":
    app.run(debug=True)
