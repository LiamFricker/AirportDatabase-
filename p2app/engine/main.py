# p2app/engine/main.py
#
# ICS 33 Winter 2025
# Project 2: Learning to Fly
#
# An object that represents the engine of the application.
#
# This is the outermost layer of the part of the program that you'll need to build,
# which means that YOU WILL DEFINITELY NEED TO MAKE CHANGES TO THIS FILE.

from collections import namedtuple

import sqlite3

import p2app.events.app as appEvents
import p2app.events.database as dbEvents
import p2app.events.continents as contEvents
import p2app.events.countries as countryEvents
import p2app.events.regions as regionEvents
from p2app.events import OpenDatabaseEvent

Continent = namedtuple('Continent', ['continent_id', 'continent_code', 'name'])

Continent.__annotations__ = {
    'continent_id': int | None,
    'continent_code': str | None,
    'name': str | None
}

Country = namedtuple(
    'Country',
    ['country_id', 'country_code', 'name', 'continent_id', 'wikipedia_link', 'keywords'])

Country.__annotations__ = {
    'country_id': int | None,
    'country_code': str | None,
    'name': str | None,
    'continent_id': int | None,
    'wikipedia_link': str | None,
    'keywords': str | None
}

Region = namedtuple(
    'Region',
    ['region_id', 'region_code', 'local_code', 'name',
     'continent_id', 'country_id', 'wikipedia_link', 'keywords'])

Region.__annotations__ = {
    'region_id': int | None,
    'region_code': str | None,
    'local_code': str | None,
    'name': str | None,
    'continent_id': int | None,
    'country_id': int | None,
    'wikipedia_link': str | None,
    'keywords': str | None
}

class Engine:
    """An object that represents the application's engine, whose main role is to
    process events sent to it by the user interface, then generate events that are
    sent back to the user interface in response, allowing the user interface to be
    unaware of any details of how the engine is implemented.
    """

    def __init__(self):
        """Initializes the engine"""
        self._connection = None
        self._errorEncountered = ""
        self._tempRow = None

    def process_event(self, event):
        """A generator function that processes one event sent from the user interface,
        yielding zero or more events in response."""
        sendBack = None
        self._errorEncountered = ""
        match type(event):
            case(appEvents.QuitInitiatedEvent):
                sendBack = appEvents.EndApplicationEvent()

            case(dbEvents.OpenDatabaseEvent):
                sendBack = dbEvents.DatabaseOpenedEvent(event.path()) if (
                    self._OpenDatabase(event.path())) else dbEvents.DatabaseOpenFailedEvent(
                    self._errorEncountered)
                self._errorEncountered = ""
            case (dbEvents.CloseDatabaseEvent):
                self._CloseDatabase()
                sendBack = dbEvents.DatabaseClosedEvent()

            case (contEvents.StartContinentSearchEvent):
                cgen = self._searchContinents(event.name(), event.continent_code())
                c = next(cgen)
                if self._errorEncountered != "":
                    c = None
                while c is not None:
                    yield contEvents.ContinentSearchResultEvent(c)
                    c = next(cgen)
                    if self._errorEncountered != "":
                        c = None

            case (contEvents.LoadContinentEvent):
                sendBack = contEvents.ContinentLoadedEvent(self._loadContinent(event.continent_id()))

            case (contEvents.SaveNewContinentEvent):
                sendBack = contEvents.ContinentSavedEvent(self._tempRow) if (
                    self._saveContinent(event.continent(), True)) else contEvents.SaveContinentFailedEvent (
                    self._errorEncountered)
                self._errorEncountered = ""

            case (contEvents.SaveContinentEvent):
                sendBack = contEvents.ContinentSavedEvent(event.continent()) if (
                    self._saveContinent(event.continent(), False)) else contEvents.SaveContinentFailedEvent(
                    self._errorEncountered)
                self._errorEncountered = ""

            case (countryEvents.StartCountrySearchEvent):
                cgen = self._searchCountries(event.name(), event.country_code())
                c = next(cgen)
                if self._errorEncountered != "":
                    c = None
                while c is not None:
                    yield countryEvents.CountrySearchResultEvent(c)
                    c = next(cgen)
                    if self._errorEncountered != "":
                        c = None

            case (countryEvents.LoadCountryEvent):
                sendBack = countryEvents.CountryLoadedEvent(self._loadCountry(event.country_id()))

            case (countryEvents.SaveNewCountryEvent):
                sendBack = countryEvents.CountrySavedEvent(self._tempRow) if (
                    self._saveCountry(event.country(),
                                      True)) else countryEvents.SaveCountryFailedEvent(
                    self._errorEncountered)
                self._errorEncountered = ""

            case (countryEvents.SaveCountryEvent):
                sendBack = countryEvents.CountrySavedEvent(event.country()) if (
                    self._saveCountry(event.country(),
                                        False)) else countryEvents.SaveCountryFailedEvent(
                    self._errorEncountered)
                self._errorEncountered = ""

            case (regionEvents.StartRegionSearchEvent):
                rgen = self._searchRegions(event.name(), event.region_code(), event.local_code())
                r = next(rgen)
                if self._errorEncountered != "":
                    r = None
                while r is not None:
                    yield regionEvents.RegionSearchResultEvent(r)
                    r = next(rgen)
                    if self._errorEncountered != "":
                        r = None

            case (regionEvents.LoadRegionEvent):
                sendBack = regionEvents.RegionLoadedEvent(self._loadRegion(event.region_id()))

            case (regionEvents.SaveNewRegionEvent):
                sendBack = regionEvents.RegionSavedEvent(self._tempRow) if (
                    self._saveRegion(event.region(),
                                     True)) else regionEvents.SaveRegionFailedEvent(
                    self._errorEncountered)
                self._errorEncountered = ""

            case (regionEvents.SaveRegionEvent):
                sendBack = regionEvents.RegionSavedEvent(event.region()) if (
                    self._saveRegion(event.region(),
                                      False)) else regionEvents.SaveRegionFailedEvent(
                    self._errorEncountered)
                self._errorEncountered = ""

        if self._errorEncountered != "":
            sendBack = appEvents.ErrorEvent(self._errorEncountered)

        # This is a way to write a generator function that always yields zero values.
        # You'll want to remove this and replace it with your own code, once you start
        # writing your engine, but this at least allows the program to run.
        if sendBack is None:
            yield from ()
        yield sendBack

    def _OpenDatabase(self, path: str) -> bool:
        """This method opens a database. It accepts a path of type str and opens a database
        at said path. If the path does not lead to a valid Database, this function returns
        false and specifies and error to return in the DatabaseOpenFailedEvent
        If no problems arise, this function returns true to signal that the database has been
        successfully opened"""
        try:
            self._connection = self._connect(path)
        except sqlite3.Error:
            self._errorEncountered = "File not found or Database Invalid."
            return False
        if self._connection is None:
            return False

        return True

    def _CloseDatabase(self):
        """Closes the currently open database. If a connection has not been made somehow
        and the user is able to close the database, it sets an error for an error event"""
        if self._connection is not None:
            self._connection.close()
        else:
            self._errorEncountered = "Database cannot be closed if it has not been opened yet."


    def _connect(self, database_path):
        """This method is a helper method of open database. It accepts a path of type str and
        opens a database at said path. If the path does not lead to a valid Database,
        this function returns false and specifies an error to return in the DatabaseOpenFailedEvent
        If no problems arise, this function returns true to signal that the connection has been
        successfully made.
        """
        connection = sqlite3.connect(database_path, isolation_level = None)
        cursor = None
        try:
            cursor = connection.execute('PRAGMA foreign_keys = ON;')
            valid = connection.execute('PRAGMA schema_version;')
        except sqlite3.Error:
            self._errorEncountered = "Database invalid."
            if cursor is not None:
                connection.close()
            return None
        if cursor is not None:
            cursor.close()
        else:
            self._errorEncountered = "Database invalid."
            return False
        if valid == 0:
            self._errorEncountered = "Database invalid."
            return False
        return connection

    def _searchContinents(self, name = None, code = None):
        """This method is a generator that searches for a continent given a name and/or a code.
        It then generates continents that match the exactly specified query. If a continent is not
        found, nothing will be generated. If an error is encountered, an error event will be
        triggered and nothing will be generated.
        """
        cursor = None
        if code is None and name is None:
            self._errorEncountered = "Invalid name/code specified."
            yield None
        try:
            if code is None:
                cursor = self._connection.execute('SELECT * FROM continent WHERE name = (:name);', (name,))
            elif name is None:
                cursor = self._connection.execute('SELECT * FROM continent WHERE continent_code = (:continent_code);', (code,))
            else:
                cursor = self._connection.execute('SELECT * FROM continent WHERE continent_code = (:continent_code) AND name = (:name);', (code, name))
            c = cursor.fetchone()
            while c is not None:
                yield Continent(c[0], c[1], c[2])
                c = cursor.fetchone()
            yield None
        except sqlite3.Error as e:
            self._errorEncountered = "Error encountered during search."
            if cursor is not None:
                cursor.close()
            yield None
        if cursor is not None:
            cursor.close()

    def _loadContinent(self, c_id):
        """This method finds a continent given a continent id. It then returns a continent.
        Since the user does not have direct access to this method, it's unlikely that an
        error will occur, but there are safeguards incase in the future the program wishes
        to implement a search by ID using loadContinent.
        """
        cursor = None
        try:
            cursor = self._connection.execute('SELECT * FROM continent WHERE continent_id = (:continent_id);', (c_id,))
        except sqlite3.Error:
            self._errorEncountered = "Error encountered while loading a continent."
            if cursor is not None:
                cursor.close()
            return None
        if cursor is not None:
            c = cursor.fetchone()
            cursor.close()
            if c is None:
                self._errorEncountered = "Continent could not be loaded."
            return Continent(c[0], c[1], c[2])
        self._errorEncountered = "Continent could not be loaded."
        return None

    def _saveContinent(self, continent: Continent, newContinent = True):
        """This method saves a continent to a table given a continent specified. It also accepts
        a second parameter which allows the engine to insert a new continent or update an
        already existing one. If any errors are encountered such as invalid names, non-existent
        save locations, etc., the method will not save the continent and return False while
        specifying an error message. Otherwise, if the continent was saved successfully, it
        returns true.
        """
        cursor = None
        try:
            if newContinent:
                tempID = 0
                try:
                    cursor = self._connection.execute('SELECT continent_id FROM continent ORDER BY continent_id DESC')
                    c = cursor.fetchone()
                    if c is not None:
                        tempID = c[0]
                except sqlite3.Error:
                    self._errorEncountered = "Error continent table invalid."
                    if cursor is not None:
                        cursor.close()
                    return False
                self._tempRow = Continent(tempID+1, continent[1], continent[2])
                cursor = self._connection.execute(
                    'INSERT INTO continent (continent_id,continent_code,name) VALUES (:continent_id, :continent_code, :name);',
                    (tempID+1, continent[1], continent[2]))
            else:
                try:
                    cursor = self._connection.execute('SELECT * FROM continent WHERE continent_id = (:continent_id)',
                                                      (continent[0],))
                    c = cursor.fetchone()
                    if c is None:
                        self._errorEncountered = "Error finding continent provided."
                        cursor.close()
                        return False
                except sqlite3.Error as e:
                    self._errorEncountered = "Error finding continent provided."
                    if cursor is not None:
                        cursor.close()
                    return False
                cursor = self._connection.execute(
                    'UPDATE continent SET continent_id = (:continent_id), continent_code = (:continent_code), name = (:name) WHERE continent_id = (:id);',
                    (continent[0], continent[1], continent[2], continent[0]))
        except sqlite3.Error as e:
            self._errorEncountered = "Error with saving your continent: Duplicate Continent Info."
            if cursor is not None:
                cursor.close()
            return False
        if cursor is not None:
            cursor.close()
            return True
        return False


    def _searchCountries(self, name = None, code = None):
        """This method is a generator that searches for a country given a name and/or a code.
        It then generates countries that match the exactly specified query. If a country is not
        found, nothing will be generated. If an error is encountered, an error event will be
        triggered and nothing will be generated.
        """
        cursor = None
        if code is None and name is None:
            self._errorEncountered = "Invalid name/code specified."
            yield None
        try:
            if code is None:
                cursor = self._connection.execute('SELECT * FROM country WHERE name = (:name);', (name,))
            elif name is None:
                cursor = self._connection.execute('SELECT * FROM country WHERE country_code = (:country_code);', (code,))
            else:
                cursor = self._connection.execute('SELECT * FROM country WHERE country_code = (:country_code) AND name = (:name);', (code, name))
            c = cursor.fetchone()
            while c is not None:
                yield Country(c[0], c[1], c[2], c[3], c[4], c[5])
                c = cursor.fetchone()
            yield None
        except sqlite3.Error as e:
            self._errorEncountered = "Error encountered during search."
            if cursor is not None:
                cursor.close()
            yield None
        if cursor is not None:
            cursor.close()

    def _loadCountry(self, c_id):
        """This method finds a country given a country id. It then returns a country.
        Since the user does not have direct access to this method, it's unlikely that an
        error will occur, but there are safeguards incase in the future the program wishes
        to implement a search by ID using loadCountry.
        """
        cursor = None
        try:
            cursor = self._connection.execute('SELECT * FROM country WHERE country_id = (:country_id);', (c_id,))
        except sqlite3.Error:
            self._errorEncountered = "Error encountered while loading a country."
            if cursor is not None:
                cursor.close()
            return None
        if cursor is not None:
            c = cursor.fetchone()
            cursor.close()
            if c is None:
                self._errorEncountered = "Country could not be loaded."
            return Country(c[0], c[1], c[2], c[3], c[4], c[5])
        self._errorEncountered = "Country could not be loaded."
        return None

    def _saveCountry(self, country: Country, newCountry = True):
        """This method saves a country to a table given a country specified. It also accepts
        a second parameter which allows the engine to insert a new country or update an
        already existing one. If any errors are encountered such as invalid names, non-existent
        save locations, etc., the method will not save the country and return False while
        specifying an error message. Otherwise, if the country was saved successfully, it
        returns true.
        """
        cursor = None
        try:
            if newCountry:
                tempID = 0
                try:
                    cursor = self._connection.execute('SELECT country_id FROM country ORDER BY country_id DESC')
                    c = cursor.fetchone()
                    if c is not None:
                        tempID = c[0]
                except sqlite3.Error:
                    self._errorEncountered = "Error country table invalid."
                    if cursor is not None:
                        cursor.close()
                    return False
                try:
                    cursor = self._connection.execute('SELECT * FROM continent WHERE continent_id = (:continent_id)',
                                                      (country[3],))
                    c = cursor.fetchone()
                    if c is None:
                        self._errorEncountered = "Error: continent matching continent id provided does not exist!"
                        cursor.close()
                        return False
                except sqlite3.Error as e:
                    self._errorEncountered = "Error: continent matching continent id provided does not exist!"
                    if cursor is not None:
                        cursor.close()
                    return False
                self._tempRow = Country(tempID+1, country[1], country[2], country[3], country[4], country[5])
                if country[5] == "":
                    cursor = self._connection.execute(
                        'INSERT INTO country (country_id,country_code,name, continent_id, wikipedia_link, keywords) VALUES (:country_id, :country_code, :name, :continent_id, :wikipedia_link, :keywords);',
                        (tempID+1, country[1], country[2], country[3], country[4], 'NULL'))
                else:
                    cursor = self._connection.execute(
                        'INSERT INTO country (country_id,country_code,name, continent_id, wikipedia_link, keywords) VALUES (:country_id, :country_code, :name, :continent_id, :wikipedia_link, :keywords);',
                        (tempID + 1, country[1], country[2], country[3], country[4], country[5]))
            else:
                try:
                    cursor = self._connection.execute('SELECT * FROM country WHERE country_id = (:country_id)',
                                                      (country[0],))
                    c = cursor.fetchone()
                    if c is None:
                        self._errorEncountered = "Error finding country provided."
                        cursor.close()
                        return False
                except sqlite3.Error as e:
                    self._errorEncountered = "Error finding country provided."
                    if cursor is not None:
                        cursor.close()
                    return False
                try:
                    cursor = self._connection.execute('SELECT * FROM continent WHERE continent_id = (:continent_id)',
                                                      (country[3],))
                    c = cursor.fetchone()
                    if c is None:
                        self._errorEncountered = "Error finding continent matching continent id provided."
                        cursor.close()
                        return False
                except sqlite3.Error as e:
                    self._errorEncountered = "Error finding continent matching continent id provided."
                    if cursor is not None:
                        cursor.close()
                    return False
                if country[5] == "":
                    cursor = self._connection.execute(
                        'UPDATE country SET country_id = (:country_id), country_code = (:country_code), name = (:name), continent_id = (:continent_id), wikipedia_link = (:wikipedia_link), keywords = (:keywords) WHERE country_id = (:id);',
                        (country[0], country[1], country[2], country[3], country[4], 'NULL',
                         country[0]))
                else:
                    cursor = self._connection.execute(
                        'UPDATE country SET country_id = (:country_id), country_code = (:country_code), name = (:name), continent_id = (:continent_id), wikipedia_link = (:wikipedia_link), keywords = (:keywords) WHERE country_id = (:id);',
                        (country[0], country[1], country[2], country[3], country[4], country[5],
                         country[0]))

        except sqlite3.Error as e:
            self._errorEncountered = "Error with saving your country: Duplicate Country Info."
            if cursor is not None:
                cursor.close()
            return False
        if cursor is not None:
            cursor.close()
            return True
        return False

    def _searchRegions(self, name = None, code = None, local_code = None):
        """This method is a generator that searches for a region given a name and/or a code.
        It then generates regions that match the exactly specified query. If a region is not
        found, nothing will be generated. If an error is encountered, an error event will be
        triggered and nothing will be generated.
        """
        cursor = None
        if code is None and name is None and local_code is None:
            self._errorEncountered = "Invalid name/code specified."
            yield None
        try:
            if code is None:
                if local_code is None:
                    cursor = self._connection.execute('SELECT * FROM region WHERE name = (:name);', (name,))
                elif name is None:
                    cursor = self._connection.execute('SELECT * FROM region WHERE local_code = (:local_code);',
                                                      (local_code,))
                else:
                    cursor = self._connection.execute(
                        'SELECT * FROM region WHERE local_code = (:local_code) AND name = (:name);',
                        (local_code, name))

            elif name is None:
                if local_code is None:
                    cursor = self._connection.execute('SELECT * FROM region WHERE region_code = (:region_code);', (code,))
                elif code is None:
                    cursor = self._connection.execute('SELECT * FROM region WHERE local_code = (:local_code);',
                                                      (local_code,))
                else:
                    cursor = self._connection.execute(
                        'SELECT * FROM region WHERE local_code = (:local_code) AND region_code = (:code);',
                        (local_code, code))

            elif local_code is None:
                if name is None:
                    cursor = self._connection.execute('SELECT * FROM region WHERE region_code = (:region_code);', (code,))
                elif code is None:
                    cursor = self._connection.execute('SELECT * FROM region WHERE name = (:name);', (name,))
                else:
                    cursor = self._connection.execute(
                        'SELECT * FROM region WHERE region_code = (:region_code) AND name = (:name);',
                        (code, name))

            else:
                cursor = self._connection.execute('SELECT * FROM region WHERE region_code = (:region_code) AND name = (:name) AND local_code = (:local_code);', (code, name, local_code))
            c = cursor.fetchone()
            while c is not None:
                yield Region(c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7])
                c = cursor.fetchone()
            yield None
        except sqlite3.Error as e:
            self._errorEncountered = "Error encountered during search."
            if cursor is not None:
                cursor.close()
            yield None
        if cursor is not None:
            cursor.close()

    def _loadRegion(self, r_id):
        """This method finds a region given a region id. It then returns a region.
        Since the user does not have direct access to this method, it's unlikely that an
        error will occur, but there are safeguards incase in the future the program wishes
        to implement a search by ID using loadRegion.
        """
        cursor = None
        try:
            cursor = self._connection.execute('SELECT * FROM region WHERE region_id = (:region_id);', (r_id,))
        except sqlite3.Error:
            self._errorEncountered = "Error encountered while loading a region."
            if cursor is not None:
                cursor.close()
            return None
        if cursor is not None:
            c = cursor.fetchone()
            cursor.close()
            if c is None:
                self._errorEncountered = "region could not be loaded."
            return Region(c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7])
        self._errorEncountered = "region could not be loaded."
        return None

    def _saveRegion(self, region: Region, newRegion = True):
        """This method saves a region to a table given a region specified. It also accepts
        a second parameter which allows the engine to insert a new region or update an
        already existing one. If any errors are encountered such as invalid names, non-existent
        save locations, etc., the method will not save the region and return False while
        specifying an error message. Otherwise, if the region was saved successfully, it
        returns true.
        """
        cursor = None
        try:
            if newRegion:
                tempID = 0
                try:
                    cursor = self._connection.execute('SELECT region_id FROM region ORDER BY region_id DESC')
                    c = cursor.fetchone()
                    if c is not None:
                        tempID = c[0]
                except sqlite3.Error:
                    self._errorEncountered = "Error region table invalid."
                    if cursor is not None:
                        cursor.close()
                    return False
                try:
                    cursor = self._connection.execute('SELECT * FROM continent WHERE continent_id = (:continent_id)',
                                                      (region[4],))
                    c = cursor.fetchone()
                    if c is None:
                        self._errorEncountered = "Error: continent matching continent id provided does not exist!"
                        cursor.close()
                        return False
                except sqlite3.Error as e:
                    self._errorEncountered = "Error: continent matching continent id provided does not exist!"
                    if cursor is not None:
                        cursor.close()
                    return False
                try:
                    cursor = self._connection.execute('SELECT * FROM country WHERE country_id = (:country_id)',
                                                      (region[5],))
                    c = cursor.fetchone()
                    if c is None:
                        self._errorEncountered = "Error: country matching country id provided does not exist!"
                        cursor.close()
                        return False
                except sqlite3.Error as e:
                    self._errorEncountered = "Error: country matching country id provided does not exist!"
                    if cursor is not None:
                        cursor.close()
                    return False
                self._tempRow = Region(tempID+1, region[1], region[2], region[3], region[4], region[5], region[6], region[7])
                tempW = region[6] if region[6] != "" else 'NULL'
                tempK = region[7] if region[7] != "" else 'NULL'

                cursor = self._connection.execute(
                    'INSERT INTO region (region_id,region_code,local_code,name, continent_id, country_id, wikipedia_link, keywords) VALUES (:region_id, :region_code, :local_code, :name, :continent_id, :country_id, :wikipedia_link, :keywords);',
                    (tempID+1, region[1], region[2], region[3], region[4], region[5], tempW, tempK))
            else:
                try:
                    cursor = self._connection.execute('SELECT * FROM region WHERE region_id = (:region_id)',
                                                      (region[0],))
                    c = cursor.fetchone()
                    if c is None:
                        self._errorEncountered = "Error finding region provided."
                        cursor.close()
                        return False
                except sqlite3.Error as e:
                    self._errorEncountered = "Error finding region provided."
                    if cursor is not None:
                        cursor.close()
                    return False
                try:
                    cursor = self._connection.execute('SELECT * FROM continent WHERE continent_id = (:continent_id)',
                                                      (region[4],))
                    c = cursor.fetchone()
                    if c is None:
                        self._errorEncountered = "Error finding continent matching continent id provided."
                        cursor.close()
                        return False
                except sqlite3.Error as e:
                    self._errorEncountered = "Error finding continent matching continent id provided."
                    if cursor is not None:
                        cursor.close()
                    return False
                try:
                    cursor = self._connection.execute('SELECT * FROM country WHERE country_id = (:country_id)',
                                                      (region[5],))
                    c = cursor.fetchone()
                    if c is None:
                        self._errorEncountered = "Error finding country matching country id provided."
                        cursor.close()
                        return False
                except sqlite3.Error as e:
                    self._errorEncountered = "Error finding country matching country id provided."
                    if cursor is not None:
                        cursor.close()
                    return False
                tempW = region[6] if region[6] != "" else 'NULL'
                tempK = region[7] if region[7] != "" else 'NULL'
                cursor = self._connection.execute(
                    'UPDATE region SET region_id = (:region_id), region_code = (:region_code), local_code = (:local_code), name = (:name), continent_id = (:continent_id), country_id = (:country_id), wikipedia_link = (:wikipedia_link), keywords = (:keywords) WHERE region_id = (:id);',
                    (region[0], region[1], region[2], region[3], region[4], region[5], tempW, tempK, region[0]))
        except sqlite3.Error as e:
            self._errorEncountered = "Error with saving your region: Duplicate Region Info."
            if cursor is not None:
                cursor.close()
            return False
        if cursor is not None:
            cursor.close()
            return True
        self._errorEncountered = "Error with saving your region"
        return False