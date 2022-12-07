import psycopg2
import os
import numpy as np
import math
from pydub import AudioSegment
from pydub.playback import play


class MusicDB:
    
    def __init__(self):
        self.audio_dir = "fma/"

    def get_db_connection(self):
        return psycopg2.connect(database="musiclib", user='postgres', password='password', host='127.0.0.1', port= '5432')

    def most_similar_search(self, track_id, k):
        k += 1
        conn = None
        out = []
        select_query = """SELECT feature1, feature2, feature3 FROM track 
        WHERE track.id = %s;"""

        compute_dist_query = '''
        SELECT id, title, SQRT(POWER(feature1 - %s, 2) + POWER(feature2 - %s, 2) + POWER(feature3 - %s, 2)) AS DIST
        FROM track 
        ORDER BY DIST 
        LIMIT %s;
        '''

        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute(select_query, (track_id,))
            features = cursor.fetchone()
            if features is not None:
                cursor.execute(compute_dist_query, (features[0], features[1], features[2], k))
                matching =  cursor.fetchall()
                if matching is not None:
                    for m in matching[1:]:
                        out.append((m[0], m[1].strip(), m[2]))
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
            return out


    def get_track_features(self, track_id):
        conn = None
        track_features = []
        select_query = """SELECT feature1, feature2, feature3 FROM track 
        WHERE track.id = %s;"""
        
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute(select_query, (track_id,))
            features = cursor.fetchone()
            if features is not None:
                track_features = [features[0], features[1], features[2]]
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
            return track_features

    def liked_songs_similarity(self, user1_features, user2_features):
        max_dist = None
        for f1 in user1_features:
            for f2 in user2_features:
                dist = math.sqrt((f1[0] - f2[0]) ** 2 + (f1[1] - f2[1]) ** 2 + (f1[2] - f2[2]) ** 2)
                if max_dist == None or dist > max_dist:
                    max_dist = dist
        if max_dist is None:
            return 999999
        return max_dist

    def recommend_friends(self, user, user2likes, k):
        user_tracks = user2likes[user]
        base_user_features = [self.get_track_features(track) for track in user_tracks]
        similarity_arr = []
        for cmp_user in user2likes:
            if cmp_user == user:
                continue
            cmp_user_tracks = user2likes[cmp_user]
            cmp_user_features = [self.get_track_features(track) for track in cmp_user_tracks]
            similarity = self.liked_songs_similarity(base_user_features, cmp_user_features)
            similarity_arr.append((similarity, cmp_user))
        
        similarity_arr.sort()
        if k >= len(similarity_arr):
            return similarity_arr
        return similarity_arr[:k]



    def get_genres(self, track_id):
        conn = None
        out = []
        select_query = """SELECT genre FROM genre 
        WHERE genre.track_id = %s;"""

        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute(select_query, (track_id,))
            genres = cursor.fetchall()
            if genres is not None:
                for g in genres:
                    out.append(g[0].strip())
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
            return out

    def get_track_info(self, track_id):
        conn = None
        out = None
        select_query = """SELECT track.id, track.title, artist.name, track.listens, track.date_created, track.duration 
        FROM track INNER JOIN artist  
        ON track.artist_id = artist.id 
        WHERE track.id = %s;"""

        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute(select_query, (track_id,))
            info = cursor.fetchone()
            if info is not None:
                out = (info[0], info[1].strip(), info[2].strip(), info[3], info[4].strftime('%m/%d/%Y'), info[5])
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
            return out


    def get_track_id(self, song_title):
        prep_input = '%' + song_title.lower() + '%'
        conn = None
        track_id = -1
        select_query = """SELECT track.id FROM track 
        WHERE LOWER(track.title) LIKE %s;"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute(select_query, (prep_input,))
            row = cursor.fetchone()
            if row is not None:
                track_id = row[0]
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
            return track_id

    def get_songs_like(self, song_keyword):
        prep_input = '%' + song_keyword.lower() + '%'
        conn = None
        out = []
        select_query = """SELECT track.id, track.title FROM track 
        WHERE LOWER(track.title) LIKE %s;"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute(select_query, (prep_input,))
            rows = cursor.fetchall()
            if rows is not None:
                for r in rows:
                    out.append((r[0], r[1].strip()))
                cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
            return out

    def play_song(self, song_title):
        prep_input = '%' + song_title.lower() + '%'
        conn = None
        select_query = """SELECT track.id, track.title, audio.file_path FROM track 
        INNER JOIN audio 
        ON track.id = audio.track_id 
        WHERE LOWER(track.title) LIKE %s;"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute(select_query, (prep_input,))
            row = cursor.fetchone()
            if row is not None:
                print("Playing: {}".format(row[1]))
                audio_file_path = self.audio_dir + row[2]
                song = AudioSegment.from_mp3(audio_file_path)[:10000]
                play(song)
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def get_artist_songs(self, artist):
        prep_input = '%' + artist.lower() + '%'
        conn = None
        out = []
        select_query = """SELECT track.id, track.title FROM artist 
        INNER JOIN track 
        ON artist.id = track.artist_id 
        WHERE LOWER(artist.name) LIKE %s;"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute(select_query, (prep_input,))
            rows = cursor.fetchall()
            if rows is not None:
                for r in rows:
                    out.append((r[0], r[1].strip()))
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
            return out

    def get_tracks_by_genre(self, genre, k):
        conn = None
        out = []
        prep_input = '%' + genre.lower() + '%'
        select_query = """SELECT track_id FROM genre WHERE LOWER(genre) LIKE %s LIMIT %s;"""

        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute(select_query, (prep_input, k ))
            rows = cursor.fetchall()
            if rows is not None:
                for r in rows:
                    out.append(r[0])
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
            return out



if __name__ == '__main__':
    #play_song("electric")
    #print(get_artist_songs('kurt'))
    
    #print(get_songs_like("elec"))
    #print(get_track_id("Electro magnetic pulse")) #expect 143645
    #get_track_id("Electric Kalimba 10") #expect 155014
    #most_similar_search(17203, 10)
    #print(most_similar_search(3, 20))
    #print(get_genres(143645))
    conn = MusicDB()
    #print(conn.get_tracks_by_genre("Hip-Hop", 40))
    print(conn.get_track_info(3))