# import necessary libraries and files
import requests
import csv
import time
import math
import itertools
import urllib
import json
from airflow.hooks.http_hook import HttpHook

# API KEY for prospect model data collection project
# API_KEY = ["AIzaSyDf20vYh2GymxaXmcu0NMGlaOAbVNWbEEc"]


# class to send different kinds of requests to google maps api
class GMapAPI(object):

    # constructor takes in the socket object to update the api monitor
    def __init__(self):
        self.http_hook = HttpHook(method='POST',
                                  http_conn_id='GooglePlacesRefresh')

        creds_connection = self.http_hook.get_connection('GooglePlacesRefresh')
        self.creds_key = [creds_connection.password]

    # destructor asks react to reset the monitor
    def __del__(self):
        pass

    def whoosh(self, search_type, params):
        while True:
            try:
                if (search_type != "geocode"):
                    res = requests.get(self.url_generator(search_type), params=params).json()
                else:
                    res = requests.get(self.geocode_url_generator(), params=params).json()
                break
            except:
                pass

        results = res

        return results

    def status_check(self, data):
        if (data["status"] == "OK"):
            return True
        else:
            self.error = data["status"]
            return False

    def url_generator(self, search_type):
        return f"https://maps.googleapis.com/maps/api/place/{search_type}/json?"

    def geocode_url_generator(self):
        return f"https://maps.googleapis.com/maps/api/geocode/json?"

    def get_type(self, place_id):
        params = {
            'place_id': place_id,
            'fields': 'type',
            'key': self.creds_key
        }

        results = self.whoosh("details", params)

        if (self.status_check(results)):
            try:
                for item in results["result"]["types"]:
                    if (item == "administrative_area_level_1"):
                        return "state"
                    elif (item == "administrative_area_level_2"):
                        return "county"
                    else:
                        return "none"
            except (KeyError, IndexError) as e:
                return e
        else:
            return self.error

    def get_county_coordinates(self, query):

        params = {
            'query': query,
            'inputtype': 'textquery',
            'fields': 'geometry',
            'key': self.creds_key
        }

        results = self.whoosh("textsearch", params)

        if (self.status_check(results)):
            try:
                for item in results['results']:
                    return item['geometry']
            except (KeyError, IndexError) as e:
                return e

        else:
            return self.error

    def get_stores_within_radius(self, location, radius):
        stores = []
        params = {
            'location': location,
            'radius': radius,
            'types': 'jewelry_store',
            'key': self.creds_key
        }

        results = self.whoosh("nearbysearch", params)
        print(params)

        if (self.status_check(results)):
            try:
                stores.extend(results['results'])

                time.sleep(2)
                while "next_page_token" in results:
                    params['pagetoken'] = results['next_page_token'],
                    results = self.whoosh("nearbysearch", params)

                    if (self.status_check(results)):
                        try:
                            stores.extend(results['results'])
                            time.sleep(2)
                        except (KeyError, IndexError) as e:
                            return e
                return stores
            except (KeyError, IndexError) as e:
                return e
        else:
            return self.error

    def get_store_details(self, place_id):

        endpoint_url = self.url_generator("details")

        params = {
            'placeid': place_id,
            'key': self.creds_key
        }

        res = requests.get(endpoint_url, params=params)
        place_details = res.json()
        return place_details

    def is_on_county(self, lat, lng, county):

        params = {
            'latlng': str(lat) + "," + str(lng),
            'result_type': 'administrative_area_level_2',
            'key': self.creds_key
        }

        results = self.whoosh("geocode", params)

        if (self.status_check(results)):
            try:
                county_working = results['results'][0]['formatted_address'].lower()
                county = county.lower()
                if (fuzz.ratio(county_working.lower(), county.lower()) > 90):
                    return True
                else:
                    return False
            except IndexError:
                return False
            except KeyError:
                return False
        else:
            return self.error

    def get_store_lat_lng(self, place_id):

        params = {
            'placeid': place_id,
            'fields': "geometry",
            'key': self.creds_key
        }

        result = self.whoosh("details", params)
        geometry = result['result']['geometry']
        return geometry['location']

    def place_id_refresh(self, place_id):
        endpoint_url = self.url_generator("details")

        params = {
            'placeid': place_id,
            'fields': "place_id",
            'key': self.creds_key
        }

        res = requests.get(endpoint_url, params=params)
        place_details = res.json()
        return place_details

    def name_addr_lookup(self, name, address):

        endpoint_url = self.url_generator("findplacefromtext")

        inpurstr = name + " " + address

        params = {
            'key': self.creds_key,
            'input': inputstr,
            'inputtype': 'textquery',
            'fields': 'place_id,name,formatted_address'
        }

        res = requests.get(endpoint_url, params=params)
        results = res.json()

        if (results['status'] == 'OK'):
            return results
        else:
            return None


if __name__ == "__main__":
    main()
