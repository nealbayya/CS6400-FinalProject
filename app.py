from audiolib_server import *
from graph_server import *
import json
import sys
from flask import Flask, jsonify, request

app = Flask(__name__)
fma_db = MusicDB()
graph_db = SocialDB("bolt://localhost:7687", "neo4j", "password")


@app.route('/user', methods=['PUT'])
def create_user():
    user_name = request.args['name']
    print(user_name, file=sys.stdout)
    graph_db.create_person(user_name)
    return jsonify({'name': user_name})


@app.route('/user', methods=['DELETE'])
def delete_user():
    user_name = request.args.get('name')
    graph_db.delete_person(user_name)
    return jsonify({'name': user_name})

@app.route('/like', methods=['GET'])
def get_likes():
    user_name = request.args.get('name')
    tracks_liked = graph_db.get_liked_songs(user_name)
    likes_arr = []
    for track in tracks_liked:
        track_info = fma_db.get_track_info(track)
        track_genres = fma_db.get_genres(track)
        info_dict = {'track_id': track_info[0], 
            'track_name': track_info[1],
            'artist': track_info[2],
            'listens': track_info[3],
            'date_created': track_info[4],
            'track_duration': track_info[5],
            'genres': track_genres
        }
        likes_arr.append(info_dict)
    return jsonify(likes_arr)


@app.route('/like', methods=['PUT'])
def like():
    user_name = request.args.get('name')
    track_id = int(request.args.get('trackid'))
    print(user_name, file=sys.stdout)
    print(track_id, file=sys.stdout)
    graph_db.log_like(user_name, track_id)
    track_info = fma_db.get_track_info(track_id)
    track_genres = fma_db.get_genres(track_id)
    like_info = {'track_id': track_info[0], 
            'track_name': track_info[1],
            'artist': track_info[2],
            'listens': track_info[3],
            'date_created': track_info[4],
            'track_duration': track_info[5],
            'genres': track_genres,
            'person': user_name
    }
    return jsonify(like_info)

@app.route('/like', methods=['DELETE'])
def remove_like():
    user_name = request.args.get('name')
    track_id = int(request.args.get('trackid'))
    print(user_name, file=sys.stdout)
    print(track_id, file=sys.stdout)
    graph_db.remove_like(user_name, track_id)
    track_info = fma_db.get_track_info(track_id)
    track_genres = fma_db.get_genres(track_id)
    like_info = {'track_id': track_info[0], 
            'track_name': track_info[1],
            'artist': track_info[2],
            'listens': track_info[3],
            'date_created': track_info[4],
            'track_duration': track_info[5],
            'genres': track_genres,
            'person': user_name
    }
    return jsonify(like_info)


@app.route('/friend', methods=['GET'])
def get_friends():
    user_name = request.args.get('name')
    friends = graph_db.get_friends(user_name)
    return jsonify(friends)

@app.route('/friend', methods=['PUT'])
def friend():
    user1 = request.args.get('f1')
    user2 = request.args.get('f2')
    graph_db.create_friendship(user1, user2)
    return jsonify({'user1': user1, 'user2': user2})

@app.route('/friend', methods=['DELETE'])
def remove_friend():
    user1 = request.args.get('f1')
    user2 = request.args.get('f2')
    graph_db.remove_friendship(user1, user2)
    return jsonify({'user1': user1, 'user2': user2})

@app.route('/elikes', methods=['GET'])
def get_friends_likes():
    user_name = request.args.get('name')
    friends_likes = [*set(graph_db.get_friends_liked_songs(user_name))]
    likes_arr = []
    for track in friends_likes:
        track_info = fma_db.get_track_info(track)
        track_genres = fma_db.get_genres(track)
        info_dict = {'track_id': track_info[0], 
            'track_name': track_info[1],
            'artist': track_info[2],
            'listens': track_info[3],
            'date_created': track_info[4],
            'track_duration': track_info[5],
            'genres': track_genres
        }
        likes_arr.append(info_dict)
    return jsonify(likes_arr)
        

@app.route('/popular', methods=['GET'])
def get_most_popular():
    k = int(request.args.get('k'))
    most_popular = graph_db.most_popular_songs(k)
    popular_arr = []
    for track, followers in most_popular:
        track_info = fma_db.get_track_info(track)
        track_genres = fma_db.get_genres(track)
        info_dict = {'track_id': track_info[0], 
            'num_listeners': followers,
            'track_name': track_info[1],
            'artist': track_info[2],
            'listens': track_info[3],
            'date_created': track_info[4],
            'track_duration': track_info[5],
            'genres': track_genres
        }
        popular_arr.append(info_dict)
    return jsonify(popular_arr)

@app.route('/recommend', methods=['GET'])
def recommend_songs():
    k = int(request.args.get('k'))
    track_id = int(request.args.get('trackid'))
    most_similar_tracks = fma_db.most_similar_search(track_id, k)
    out = []
    for id, title, similarity in most_similar_tracks:
        track_info = fma_db.get_track_info(id)
        track_genres = fma_db.get_genres(id)
        info_dict = {'track_id': track_info[0], 
            'track_name': track_info[1],
            'artist': track_info[2],
            'listens': track_info[3],
            'date_created': track_info[4],
            'track_duration': track_info[5],
            'similarity': similarity,
            'genres': track_genres
        }
        out.append(info_dict)
    return jsonify(out)


@app.route('/recommend_friends', methods=['GET'])
def recommend_friends():
    name = request.args.get('name')
    k = int(request.args.get('k'))
    graph_data = graph_db.retrieve_all_likes_data()
    recommendations = fma_db.recommend_friends(name, graph_data, k)
    out = []
    for rec in recommendations:
        out.append({
            'friend': rec[1],
            'similarity': rec[0]
        })
    return jsonify(out)

@app.route('/search', methods=['GET'])
def search_songs():
    keyword = request.args.get('keyword')
    similar = fma_db.get_songs_like(keyword)
    out = []
    for id, title in similar:
        info_dict = {'track_id': id, 
            'track_name': title
        }
        out.append(info_dict)
    return jsonify(out)


@app.route('/artist', methods=['GET'])
def get_artist_songs():
    artist_name = request.args.get('name')
    tracks = fma_db.get_artist_songs(artist_name)
    out = []
    for id, title in tracks:
        info_dict = {'track_id': id, 
            'track_name': title
        }
        out.append(info_dict)
    return jsonify(out)

@app.route('/play', methods=['GET'])
def play_song():
    track_id = int(request.args.get('trackid'))
    track_info = fma_db.get_track_info(track_id)
    fma_db.play_song(track_info[1])
    return '', 200

if __name__ == '__main__':
    app.run()


