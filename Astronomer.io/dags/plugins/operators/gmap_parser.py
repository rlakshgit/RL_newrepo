import json
import logging


class GMapParser(object):

    def __init__(self, json_dump):
        if 'result' in json_dump:
            self.json = json_dump['result']

        self.status = json_dump['status']

    def getFormattedAddr(self):
        try:
            formatted_address = self.json['formatted_address']
        except KeyError:
            formatted_address = None

        return formatted_address

    def getState(self):
        state = []
        for comp in self.json['address_components']:
            for addr_type in comp['types']:
                if (addr_type == "administrative_area_level_1"):
                    state.append(comp['long_name'])
                    state.append(comp['short_name'])

        return state

    def getCounty(self):
        for comp in self.json['address_components']:
            for addr_type in comp['types']:
                if (addr_type == "administrative_area_level_2"):
                    county = comp['long_name']

        return county

    def getCity(self):
        for comp in self.json['address_components']:
            for addr_type in comp['types']:
                if (addr_type == "administrative_area_level_3"):
                    city = comp['long_name']

        return city

    def getZip(self):
        for comp in self.json['address_components']:
            for addr_type in comp['types']:
                if (addr_type == "post_code"):
                    zip_code = comp['long_name']

        return zip_code

    def getLat(self):
        try:
            lat = self.json['geometry']['location']['lat']
        except KeyError:
            lat = None

        return lat

    def getLng(self):
        try:
            lng = self.json['geometry']['location']['lng']
        except KeyError:
            lng = None

        return lng

    def getLatLng(self):
        latlng = []

        try:
            latlng.append(self.json['geometry']['location']['lat'])
            latlng.append(self.json['geometry']['location']['lng'])
        except KeyError:
            pass

        return latlng

    def getName(self):
        try:
            name = self.json['name']
        except KeyError:
            name = None

        return name

    def getPlaceID(self):
        if 'place_id' in self.json:
            place_id = self.json['place_id']
        else:
            place_id = None

        return place_id

    def getTypes(self):
        types = []

        try:
            for item in self.json['types']:
                types.append(item)
        except KeyError:
            pass

        return types

    def isJewelryStore(self):

        for item in self.json['types']:
            if (item == "jewelry_store"):
                return True

        return False

    def isEstablishment(self):
        try:
            for item in self.json['types']:
                if (item == "establishment"):
                    return 'True'
        except KeyError:
            logging.info("Failed parsing json/Types field not found")
            pass

        return 'False'

    def getWebsite(self):
        try:
            website = self.json['website']
        except KeyError:
            website = None

        return website

    def getFormattedPhone(self):
        try:
            phone = self.json['formatted_phone_number']
        except KeyError:
            phone = None

        return phone

    def getNumRatings(self):
        try:
            num_ratings = self.json['user_ratings_total']
        except KeyError:
            num_ratings = None

        return num_ratings

    def getRating(self):
        try:
            rating = self.json['rating']
        except KeyError:
            rating = None

        return rating

    def getPriceLevel(self):
        try:
            price_level = self.json['price_level']
        except KeyError:
            price_level = None

        return price_level

    def getStatus(self):
        return self.status





