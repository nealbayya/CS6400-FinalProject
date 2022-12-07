from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
from fma import utils
import pandas as pd
import random

class SocialDB:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def create_person(self, name):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(self._create_and_return_person, name)

    @staticmethod
    def _create_and_return_person(tx, name):
        query = (
            "CREATE (p:Person { name: $pname }) "
            "RETURN p"
        )
        result = tx.run(query, pname=name)
        try:
            return result
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

    def delete_person(self, name):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(self._delete_and_return_person, name)

    @staticmethod
    def _delete_and_return_person(tx, name):
        query = (
            "MATCH (p:Person) "
            "WHERE p.name = $pname "
            "DELETE p;"
        )
        result = tx.run(query, pname=name)
        try:
            return result
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

    def create_song(self, track_id):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(self._create_and_return_song, track_id)

    @staticmethod
    def _create_and_return_song(tx, track_id):
        query = (
            "CREATE (t:Song { id: $trackidentifier }) "
            "RETURN t"
        )
        result = tx.run(query, trackidentifier=track_id)
        try:
            return result
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

    def log_like(self, name, track_id):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(self._create_and_return_like, name, track_id)

    @staticmethod
    def _create_and_return_like(tx, name, track_id):
        query = (
            "MATCH (a:Person),(b:Song) "
            "WHERE a.name = $pname AND b.id = $trackidentifier "
            "CREATE (a)-[r:LIKES]->(b) "
            "RETURN r"
        )
        result = tx.run(query, pname=name, trackidentifier=track_id)
        try:
            return result
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise


    def remove_like(self, name, track_id):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(self._destroy_and_return_like, name, track_id)

    @staticmethod
    def _destroy_and_return_like(tx, name, track_id):
        query = (
            "MATCH (a:Person)-[r:LIKES]->(b:Song) "
            "WHERE a.name = $pname AND b.id = $trackidentifier "
            "DELETE r "
            "RETURN a, b"
        )
        result = tx.run(query, pname=name, trackidentifier=track_id)
        try:
            return result
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise


    def create_friendship(self, a, b):
        with self.driver.session(database="neo4j") as session:
            session.execute_write(self._create_and_return_friendship, a, b)

    @staticmethod
    def _create_and_return_friendship(tx, a, b):
        query = (
            "MATCH (a:Person),(b:Person) "
            "WHERE a.name = $paname AND b.name = $pbname "
            "CREATE (a)-[r:FRIENDS_WITH]->(b) "
            "RETURN r"
        )
        tx.run(query, paname=a, pbname=b)
        tx.run(query, paname=b, pbname=a)


    def remove_friendship(self, a, b):
        with self.driver.session(database="neo4j") as session:
            session.execute_write(self._destroy_friendship, a, b)
    
    @staticmethod
    def _destroy_friendship(tx, a, b):
        query = (
            "MATCH (a:Person)-[r:FRIENDS_WITH]->(b:Person) "
            "WHERE a.name = $paname AND b.name = $pbname "
            "DELETE r "
            "RETURN a, b"
        )
        tx.run(query, paname=a, pbname=b)
        tx.run(query, paname=b, pbname=a)

    def get_liked_songs(self, name):
        with self.driver.session(database="neo4j") as session:
            liked_songs = session.execute_write(self._retrieve_liked_songs, name)
            return liked_songs

    @staticmethod
    def _retrieve_liked_songs(tx, name):
        query = (
            "MATCH (a:Person)-[r:LIKES]->(b:Song) "
            "WHERE a.name = $paname "
            "RETURN b"
        )
        result = tx.run(query, paname=name)
        try:
            out = []
            for ls in result:
                out.append(ls['b']['id'])
            return out
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise


    def get_friends(self, name):
        with self.driver.session(database="neo4j") as session:
            friends = session.execute_write(self._retrieve_friends, name)
            return friends

    @staticmethod
    def _retrieve_friends(tx, name):
        query = (
            "MATCH (a:Person)-[r:FRIENDS_WITH]->(b:Person) "
            "WHERE a.name = $pname "
            "RETURN b"
        )
        result = tx.run(query, pname=name)
        try:
            out = []
            for ls in result:
                out.append(ls['b']['name'])
            return out
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

    def get_friends_liked_songs(self, name):
        friends = self.get_friends(name)
        network_liked_songs = []
        for f in friends:
            liked_songs = self.get_liked_songs(f)
            network_liked_songs.extend(liked_songs)
        return network_liked_songs

    def retrieve_all_likes_data(self):
        user_names = None
        with self.driver.session(database="neo4j") as session:
            user_names = session.execute_write(self._get_all_user_names)
        
        user2likes = {}
        for user in user_names:
            user2likes[user] = self.get_liked_songs(user)
        return user2likes

    @staticmethod
    def _get_all_user_names(tx):
        query = (
            "MATCH (p:Person) "
            "RETURN p"
        )
        result = tx.run(query)
        try:
            out = []
            for res in result:
                out.append(res['p']['name'])
            return out
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

    def most_popular_songs(self, k):
        with self.driver.session(database="neo4j") as session:
            songs = session.execute_write(self._query_most_popular, k)
            return songs
    
    @staticmethod
    def _query_most_popular(tx, k):
        query = (
            "MATCH (t:Song) "
            "RETURN t, SIZE((t) <-- ()) AS num_listeners "
            "ORDER BY num_listeners DESC "
            "LIMIT $l"
        )
        result = tx.run(query, l=k)
        try:
            out = []
            for res in result:
                out.append((res['t']['id'], res['num_listeners']))
            return out
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

    def close(self):
        self.driver.close()

#-------------------------------------
#ONLY RUN ONCE
def populate_graphdb_tracks(conn):
    tracks = utils.load('fma/data/fma_metadata/tracks.csv')
    track_ids = list(tracks.index.values)
    for id in track_ids:
        conn.create_song(int(id))

def populate_graphdb_people(conn):
    names = ['Neal', 'Chris', 'Sham', 'Aditya', 'Lauren', 'Jerry', 'Ankith', \
        'Patrick', 'Anne', 'Pranav', 'Neha', 'Yao', 'Taylor', 'Griffin', 'Andy']
    for n in names:
        conn.create_person(n)

def populate_graphdb_likes(conn):
    from audiolib_server import MusicDB
    names = ['Neal', 'Chris', 'Sham', 'Aditya', 'Lauren', 'Jerry', 'Ankith', \
    'Patrick', 'Anne', 'Pranav', 'Neha', 'Yao', 'Taylor', 'Griffin', 'Andy']
    genres = ['Pop', 'Hip-Hop', 'Folk', 'Jazz', 'Classical', 'Blues', 'International', 'Experimental', 'Electronic']
    genre2tracks = {}
    for g in genres:
        genre2tracks[g] = MusicDB().get_tracks_by_genre(g, '30')
    for i, name in enumerate(names):
        tracks = random.sample(genre2tracks['Pop'], 10)
        genre_pick = i % len(genres)
        tracks.extend(random.sample(genre2tracks[genres[genre_pick]], 10))
        for t in tracks:
            conn.log_like(name, t)

def populate_graphdb_friends(conn):
    conn.create_friendship('Neal', 'Chris')
    conn.create_friendship('Neal', 'Sham')
    conn.create_friendship('Chris', 'Patrick')
    conn.create_friendship("Anne", "Yao")
    conn.create_friendship('Taylor', 'Griffin')
    conn.create_friendship('Ankith', 'Patrick')
    conn.create_friendship('Neha', 'Pranav')
    conn.create_friendship('Aditya', 'Sham')
    conn.create_friendship('Lauren', 'Andy')
    conn.create_friendship('Aditya', 'Chris')
    conn.create_friendship('Taylor', 'Chris')
#-------------------------------------

if __name__ == "__main__":
    conn = SocialDB("bolt://localhost:7687", "neo4j", "password")
    
    #populate_graphdb_friends(conn)
    #populate_graphdb_likes(conn)

    print(conn.most_popular_songs(5))
    conn.close()