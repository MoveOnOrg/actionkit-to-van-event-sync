from dateutil import parser, tz
from datetime import timedelta
import itertools
from ngpvan_api.event import NGPVANEventAPI
from ngpvan_api.location import NGPVANLocationAPI
import psycopg2
import psycopg2.extras

import settings

# Prepare database connection information

db_connection = psycopg2.connect(
    host=settings.DB_HOST,
    port=settings.DB_PORT,
    user=settings.DB_USER,
    password=settings.DB_PWD,
    database=settings.DB_NAME
)
db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

events_table = sqlalchemy.Table("%s.events" % settings.DB_VAN_SCHEMA,
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

# Find events that are only in AK (with PostgreSQL query)

events_query = """SELECT e.id AS ak_event_id,
       e.title,
       e.venue,
       e.address1,
       e.address2,
       e.city,
       e.state,
       e.postal,
       e.zip,
       e.plus4,
       e.country,
       e.starts_at_utc::text,
       e.ends_at_utc::text,
       e.creator_id AS creator_ak_id,
       e.campaign_id
FROM %s.events_event e
LEFT JOIN %s.events van ON van.ak_event_id = e.id
WHERE e.campaign_id IN (%s)
AND e.host_is_confirmed = 1
AND e.status = 'active'
AND e.starts_at_utc >= DATE(GETDATE())
AND van.ak_event_id IS NULL""" % (settings.DB_AK_SCHEMA, settings.DB_VAN_SCHEMA, EVENT_TYPE_NAME_CAMPAIGN_MAP.values())

db_cursor.execute(events_query)
events_to_add = db_cursor.fetchall()

# Group and loop through events per state

events_by_state = itertools.groupby(sorted(events_to_add, key=lambda event: event['state']), key=lambda event: event['state'])

for state, events in events_by_state:

    if settings.NGPVAN_API_KEYS.get(state, False):

        # Get general VAN API info per state

        api_creds = {
            'NGPVAN_BASE_URL': settings.NGPVAN_BASE_URL,
            'NGPVAN_API_KEY': settings.NGPVAN_API_KEYS.get(state) + '|1',
            'NGPVAN_API_APP': settings.NGPVAN_API_APP,
        }

        ngpvan_location_api = NGPVANLocationAPI(api_creds)
        ngpvan_event_api = NGPVANEventAPI(api_creds)

        event_types = {}

        for type_name in list(settings.EVENT_TYPE_NAME_CAMPAIGN_MAP.keys()):
            event_types[type_name] = ngpvan_event_api.get_event_type_by_name(type_name)

        events = list(events)

        print("Found %s new events for %s." % (len(events), state))

        for event_to_add in events:

            print("Adding event from AK:")
            print(event_to_add)

            # Get/create location

            location_data = {
                'name': event_to_add['venue'][0:50],
                'address': {
                    'addressLine1': event_to_add['address1'],
                    'addressLine2': event_to_add['address2'],
                    'city': event_to_add['city'],
                    'stateOrProvince': event_to_add['state'],
                    'zipOrPostalCode': event_to_add['zip']
                }
            }

            event_type_name = list(EVENT_TYPE_NAME_CAMPAIGN_MAP.keys())[list(EVENT_TYPE_NAME_CAMPAIGN_MAP.values().index(event_to_add['campaign_id']))]

            location_id = ngpvan_location_api.get_or_create_location(location_data).get('location_id', '')

            print("Identified location_id as %s" % location_id)

            # Prepare event info

            new_van_event = {
                'name': event_to_add['title'],
                'shortName': event_to_add['ak_event_id'],
                'startDate': parser.parse(event_to_add['starts_at_utc']).replace(tzinfo=tz.tzutc()).isoformat(),
                'eventType': {
                    'eventTypeId': event_types[event_type_name]['eventTypeId']
                },
                'isOnlyEditableByCreatingUser': False,
                'locations': [
                    {
                      'locationId': location_id
                    }
                ],
                'roles': [
                    role for role in event_types[event_type_name]['roles']
                    if role['name'] == 'Host' or role['name'] == 'Participant'
                ]
            }

            # If ends_at_utc is not supplied, add 1 hour to starts_at_utc

            if event_to_add['ends_at_utc']:
                new_van_event['endDate'] =  parser.parse(event_to_add['ends_at_utc']).replace(tzinfo=tz.tzutc()).isoformat(),
            else:
                end_date = parser.parse(event_to_add['starts_at_utc']) + timedelta(hours=1)
                new_van_event['endDate'] = end_date.replace(tzinfo=tz.tzutc()).isoformat()

            # Always do 1 shift spanning entire event

            new_van_event['shifts'] = [
                {
                    'name': 'Single Shift',
                    'startTime': new_van_event['startDate'],
                    'endTime': new_van_event['endDate'],
                }
            ]

            # Add event to VAN

            add = ngpvan_event_api.create_event(new_van_event)

            print("Added event to VAN:")
            print(new_van_event)
            print("New event ID in VAN is %s" % add.get('event_id', ''))

            if not isinstance(add['event_id'], dict):

                # Add event to ngpvan.events table, to avoid duplication

                # Get details on new event, with full location info.
                new_event_details = ngpvan_event_api.get_event(add.get('event_id', ''), params={'$expand': 'locations'}).get('events')[0]
                # Clean up missing location data
                if len(new_event_details.get('locations', [{}])) == 0:
                    new_event_details['locations'] = [{}]
                if new_event_details.get('locations', [{}])[0].get('address', {}) == None:
                    new_event_details['locations'][0]['address'] = {}

                values = [
                    {
                        'van_event_id': new_event_details.get('eventId', ''),
                        'title': new_event_details.get('name', ''),
                        'venue': new_event_details.get('locations', [{}])[0].get('name', ''),
                        'address1': new_event_details.get('locations', [{}])[0].get('address', {}).get('addressLine1', ''),
                        'address2': new_event_details.get('locations', [{}])[0].get('address', {}).get('addressLine2', ''),
                        'city': new_event_details.get('locations', [{}])[0].get('address', {}).get('city', ''),
                        'state': state,
                        'zip': new_event_details.get('locations', [{}])[0].get('address', {}).get('zipOrPostalCode', ''),
                        'country': new_event_details.get('locations', [{}])[0].get('address', {}).get('countryCode', ''),
                        'starts_at_utc': parser.parse(new_event_details.get('startDate', '')).astimezone(tz.tzutc()).strftime('%Y-%m-%d %H:%M:%S'),
                        'ends_at_utc': parser.parse(new_event_details.get('endDate', '')).astimezone(tz.tzutc()).strftime('%Y-%m-%d %H:%M:%S'),
                        'ak_event_id': new_event_details.get('shortName', '')
                    }
                ]

                print(values)

                db_cursor.execute(sqlalchemy.sql.expression.insert(events_table, values))
