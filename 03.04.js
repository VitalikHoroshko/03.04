const request = require('request');
const gunzipMaybe = require('gunzip-maybe');
const JSONStream = require('JSONStream');
const MongoClient = require('mongodb').MongoClient;

const url = 'mongodb://localhost:27017';
const dbName = 'movie_database';


MongoClient.connect(url, function(err, client) {
  if (err) throw err;

 
  const db = client.db(dbName);

  
  const movieCollection = db.collection('movies');
  const movieStream = movieCollection.initializeOrderedBulkOp();

  
  const stream = request.get('https://popwatch-staging.s3.us-east-2.amazonaws.com/movies_1.gz')
    .pipe(gunzipMaybe())
    .pipe(JSONStream.parse('*'));

  
  stream.on('data', function(movie) {
    movieStream.insert(movie);
  });

  
  stream.on('error', function(err) {
    console.error('Error parsing movie data', err);
    process.exit(1);
  });

  
  stream.on('end', function() {
    movieStream.execute(function(err, result) {
      if (err) throw err;
      console.log('Successfully inserted', result.nInserted, 'movies into the database');
      client.close();
    });
  });
});