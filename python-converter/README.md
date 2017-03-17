The script provided here is music-hdfs-ingestor.py and music-hdfs-ingestor-one.py

The purpose of these scripts are to convert HDF5 million music dataset files into JSON 
format either as separated (bunch of) files like the original dataset or group some files into one big json file.

I used some libraries provided by Million Songs (https://labrosa.ee.columbia.edu/millionsong/) and can be found in
https://github.com/tbertinmahieux/MSongsDB/tree/master/PythonSrc

These libraries are used to read the data structure of music hdf5 files.

Credit to
Thierry Bertin-Mahieux, Daniel P.W. Ellis, Brian Whitman, and Paul Lamere. 
The Million Song Dataset. In Proceedings of the 12th International Society
for Music Information Retrieval Conference (ISMIR 2011), 2011.
