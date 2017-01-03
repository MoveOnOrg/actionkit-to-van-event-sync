from datetime import datetime, timedelta
from dateutil import parser, tz
from ngpvan_api.event import NGPVANEventAPI
import requests
import psycopg2
import psycopg2.extras
import sqlalchemy

import settings


# Prepare database and date information

yesterday = datetime.now() - timedelta(days=1)
yesterday_date = yesterday.strftime("%Y-%m-%d")

db_connection = psycopg2.connect(
    host=settings.DB_HOST,
    port=settings.DB_PORT,
    user=settings.DB_USER,
    password=settings.DB_PWD,
    database=settings.DB_NAME
)
db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

events_stage_table = sqlalchemy.Table("%s.events_stage" % settings.DB_VAN_SCHEMA,
    sqlalchemy.MetaData(),
    sqlalchemy.Column('van_event_id'),
    sqlalchemy.Column('title'),
    sqlalchemy.Column('venue'),
    sqlalchemy.Column('address1'),
    sqlalchemy.Column('address2'),
    sqlalchemy.Column('city'),
    sqlalchemy.Column('state'),
    sqlalchemy.Column('zip'),
    sqlalchemy.Column('country'),
    sqlalchemy.Column('starts_at_utc'),
    sqlalchemy.Column('ends_at_utc'),
    sqlalchemy.Column('ak_event_id'),
    quote=False
)

# Loop through states

for state in settings.NGPVAN_API_KEYS.keys():

    events = []
    ngpvan_event_api = NGPVANEventAPI({
        'NGPVAN_BASE_URL': settings.NGPVAN_BASE_URL,
        'NGPVAN_API_KEY': settings.NGPVAN_API_KEYS.get(state) + '|1',
        'NGPVAN_API_APP': settings.NGPVAN_API_APP,
    })

    # Get all events in state

    for type_name in list(settings.EVENT_TYPE_NAME_CAMPAIGN_MAP.keys()):
        events.extend(ngpvan_event_api.get_events_by_type_name(type_name, params={'startingAfter': yesterday_date}).get('events'))

    print("Found %s events for %s." % (len(events), state))

    # Get event details

    event_details = []

    for event in events:
        details = ngpvan_event_api.get_event(event['eventId'], params={'$expand': 'locations'}).get('events')[0]
        # Clean up missing location data
        if len(details.get('locations', [{}])) == 0:
            details['locations'] = [{}]
        if details.get('locations', [{}])[0].get('address', {}) == None:
            details['locations'][0]['address'] = {}
        event_details.append(details)

    # Populate stage table

    db_cursor.execute("TRUNCATE %s.events_stage" % settings.DB_VAN_SCHEMA)

    values = [
        {
            'van_event_id': event.get('eventId', ''),
            'title': event.get('name', ''),
            'venue': event.get('locations', [{}])[0].get('name', ''),
            'address1': event.get('locations', [{}])[0].get('address', {}).get('addressLine1', ''),
            'address2': event.get('locations', [{}])[0].get('address', {}).get('addressLine2', ''),
            'city': event.get('locations', [{}])[0].get('address', {}).get('city', ''),
            'state': state,
            'zip': event.get('locations', [{}])[0].get('address', {}).get('zipOrPostalCode', ''),
            'country': event.get('locations', [{}])[0].get('address', {}).get('countryCode', ''),
            'starts_at_utc': parser.parse(event.get('startDate', '')).astimezone(tz.tzutc()).strftime('%Y-%m-%d %H:%M:%S'),
            'ends_at_utc': parser.parse(event.get('endDate', '')).astimezone(tz.tzutc()).strftime('%Y-%m-%d %H:%M:%S'),
            'ak_event_id': event.get('shortName', '')
        }
        for event in event_details
    ]

    print("Inserting events into stage for %s..." % state)

    print(values)

    db_cursor.execute(sqlalchemy.sql.expression.insert(events_stage_table, values))

    print("Inserting and updating events for %s..." % state)

    # Run updates on existing events

    update_sql = """UPDATE %s.events
    SET title = s.title,
    venue = s.venue,
    address1 = s.address1,
    address2 = s.address2,
    city = s.city,
    state = s.state,
    zip = s.zip,
    country = s.country,
    starts_at_utc = s.starts_at_utc,
    ends_at_utc = s.ends_at_utc,
    ak_event_id = s.ak_event_id
    FROM %s.events_stage s
    WHERE %s.events.van_event_id = s.van_event_id""" % (settings.DB_VAN_SCHEMA, settings.DB_VAN_SCHEMA, settings.DB_VAN_SCHEMA)

    db_cursor.execute(update_sql)

    # Run inserts on new events

    insert_sql = """INSERT INTO %s.events
    SELECT s.* FROM %s.events_stage s
    LEFT JOIN %s.events
      ON s.van_event_id = %s.events.van_event_id
    WHERE %s.events.van_event_id IS NULL""" % (settings.DB_VAN_SCHEMA, settings.DB_VAN_SCHEMA, settings.DB_VAN_SCHEMA, settings.DB_VAN_SCHEMA, settings.DB_VAN_SCHEMA)

    db_cursor.execute(insert_sql)
