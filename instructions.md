# CS 6400 Final Project

[Presentation](https://docs.google.com/presentation/d/1hUfnvTdaBzGVGG_PzAuiE_yYDZ9AfZbYI0pBAkfWOvM/edit?usp=sharing)

## Data Preparation and Setup
 
### Dependencies and Systems

Below are a list of database systems used for this project:
- [PostgreSQL (v15)](https://www.postgresql.org/download/)
- [Neo4j (v1.5.6)](https://neo4j.com/download/)

For running our application, ensure Python 3 is [installed](https://www.python.org/downloads/) along with the following required Python packages:
- neo4j
- psycopg2
- flask
- pydub
- librosa
- sklearn
- numpy
- pandas
- matplotlib
- seaborn

These packages can be installed with the command: `pip install <package>`

### Setup and Executing Code
Create a directory with these files, and then clone the [FMA scripts](https://github.com/mdeff/fma) (`fma/`) within it  as a subdirectory. As per the setup guide detailed in FMA, navigate to `fma/data/` and run: `curl -O https://os.unil.cloud.switch.ch/fma/fma_metadata.zip` to download the metadata. This database application does not require the audio tracks to be downloaded, but they can optionally be downloaded as per the FMA README in Usage (Step 4). 

Open the PostgreSQL app and create a database named `musiclib`. Open the `musiclib` database in a psql terminal (by double clicking on the newly-created database) and run the following table creation queries:
- `CREATE TABLE artist(id INT PRIMARY KEY NOT NULL, name TEXT NOT NULL, bio TEXT, location CHAR(50));`
- `CREATE TABLE track(id INT PRIMARY KEY NOT NULL, title CHAR(100),                                                             artist_id INT NOT NULL, interest INT, listens INT, date_created DATE NOT NULL,                                                   duration INT NOT NULL, language CHAR(10), feature1 REAL NOT NULL, feature2 REAL NOT NULL,  feature3 REAL NOT NULL, CONSTRAINT fk_artist FOREIGN KEY(artist_id) REFERENCES artist(id) ON DELETE SET NULL);`
- `CREATE TABLE genre(track_id INT NOT NULL, genre CHAR(50),                                                                 CONSTRAINT fk_genre FOREIGN KEY(track_id) REFERENCES track(id) ON DELETE SET NULL);`
- `CREATE TABLE audio(track_id INT NOT NULL, file_path TEXT,                                                                 CONSTRAINT fk_track_audio FOREIGN KEY(track_id) REFERENCES track(id) ON DELETE SET NULL);`

Next, move the `db-creation.ipynb` file to the `fma` directory, and execute all of the commands to populate the relational database.

Next, open the Neo4j Desktop app and create and run an empty graph database (name of database does not matter). To initialize the graph database with data, run `python3 graph_server.py` once. This file will not need to be executed again.

Ensuring that both database servers are running, execute `python app.py` to run the application server. Below is a demonstration of various queries that can be executed once the application server is running:

1. Create a User
`curl -X PUT "http://127.0.0.1:5000/user?name=<name>"`
2. See songs tracks with titles similar to a keyword
	`curl -X GET "http://127.0.0.1:5000/search?keyword=<name>"`
3. Play a song
	`curl -X GET "http://127.0.0.1:5000/play?trackid=<id>"`
4. Like songs
	`curl -X PUT "http://127.0.0.1:5000/like?name=<name>&trackid=<id>"`
5. See all tracks liked by user
	`curl -X GET "http://127.0.0.1:5000/like?name=<name>"`
6. See suggested friends
	`curl -X GET "http://127.0.0.1:5000/recommend_friends?name=<name>&k=<k>"`
7. Create friendship
	`curl -X PUT "http://127.0.0.1:5000/friend?f1=<name>&f2=<name>"`
8. See all tracks liked by friends
    `curl -X GET "http://127.0.0.1:5000/elikes?name=<name>"`
9. Suggest songs similar to a given song
    `curl -X GET "http://127.0.0.1:5000/recommend?trackid=<id>&k=<k>"`
10. Get most popular track in network
    `curl -X GET "http://127.0.0.1:5000/popular?k=<k>"`


## Application and Code

We exclusively used Python3 in this project. Please see the **Dependencies and Systems** section for the Python dependencies which need to be installed.  

## Code Documentation and References
All of the code in this repository was solely written by the authors, Neal Bayya and Christopher Lo. 