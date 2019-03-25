require 'geocoder'
require 'json'
require 'pg'
require 'sinatra'

set :bind, '0.0.0.0'

# DB connection credentials are passed via environment
# variables
DB_HOST = ENV["DB_HOST"] || 'localhost'
DB_DATABASE = ENV["DB_DATABASE"] || 'geocode'
DB_USER = ENV["DB_USER"] || 'postgres'
DB_PASS = ENV["DB_PASS"] || 'password'

# Connect to the database and create table if it doesn't exist
conn = PG.connect( dbname: DB_DATABASE, host: DB_HOST, 
    password: DB_PASS, user: DB_USER)
conn.exec "CREATE TABLE IF NOT EXISTS events (
    id varchar(20) NOT NULL PRIMARY KEY,
    timestamp timestamp,
    lat double precision,
    lon double precision,
    type text,
    measure real,
    address text
);"

# Store an event
post '/' do
    d = JSON.parse(request.body.read.to_s)
    address = coords_to_address(d["lat"], d["long"])
    id = d["id"]
    
    begin
        conn.prepare("insert_#{id}", 'INSERT INTO events VALUES ($1, $2, $3, $4, $5, $6, $7)')
    rescue PG::DuplicatePstatement => e
        puts "Duplicate preared statement: #{id}"
    ensure
        conn.exec_prepared("insert_#{id}", [d["id"], d["time"], d["lat"], d["long"], d["type"], d["measure"], address.to_json])
    end
end

# Get all events from the last 24 hours
get '/' do
    type = params[:type]
    select_statement = "select * from events where 
        timestamp > 'now'::timestamp - '24 hours'::interval";
    select_suffix = ";"
    select_suffix = " and type = '" + type + "';" unless type.nil?
    select_statement = select_statement + select_suffix;
    results = conn.exec(select_statement)
    jResults = []
    results.each do |row|
        jResults << row
    end

    content_type 'application/json'
    headers 'Access-Control-Allow-Origin' => "*"
    return jResults.to_json
end

# Get the address from a given set of coordinates
def coords_to_address(lat, lon)
    coords = [lat, lon]
    results = Geocoder.search(coords)

    a = results.first
    address = {
        address: a.address,
        house_number: a.house_number,
        street: a.street,
        county: a.county,
        city: a.city,
        state: a.state,
        state_code: a.state_code,
        postal_code: a.postal_code,
        country: a.country,
        country_code: a.country_code,
        coordinates: a.coordinates
    }

    return address
end