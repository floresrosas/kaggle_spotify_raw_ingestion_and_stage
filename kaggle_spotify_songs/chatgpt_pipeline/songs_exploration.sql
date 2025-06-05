--ALl data from the longest sond
-- SELECT *
-- FROM songs
-- LIMIT 100;


-- General info about my fav artist (Drake)
SELECT popularity, song, artists
FROM songs
WHERE artists ilike 'drake,%' 
or artists ilike '%,drake,%' 
or artists ilike 'drake'
ORDER BY popularity desc
-- The aritsts are not associated with the paired song
-- The song is not called Runaway Girl although the data looks good
-- Runaway girl is not his most popular song and doesnt have artists SZA and Sexy Red - Rich Baby Daddy more likely
-- Debugged issue and its not with processing its a raw data issue. Need to confirm with vendor
-- All other analysis can stop since this failed
/*
raw data from file
{'Artist(s)': 'Drake,Sexyy Red,SZA', 'song': 'Runaway Girl', 'text': "Hey there material girl  \r The neighbors told me that you...e on your way  \r Run away girl  \r (X2)\r \r ', 'Length': '05:19', 'emotion': 'joy', 'Genre': 'hip hop', 'Album': 'For All The Dogs', 'Release Date': '2023-10-06', 'Key': 'D Maj', 'Tempo': 0.6804733728, 'Loudness (db)': 0.8266715116, 'Time signature': '4/4', 'Explicit': 'Yes', 'Popularity': '91', 'Energy': '73', 'Danceability': '64', 'Positiveness': '14', 'Speechiness': '5', 'Liveness': '38', 'Acousticness': '4', 'Instrumentalness': '0', 'Good for Party': 1, 'Good for Work/Study': 0, 'Good for Relaxation/Meditation': 0, 'Good for Exercise': 1, 'Good for Running': 1, 'Good for Yoga/Stretching': 0, 'Good for Driving': 0, 'Good for Social Gatherings': 0, 'Good for Morning Routine': 0, 'Similar Songs': [{...}, {...}, {...}]}
*/