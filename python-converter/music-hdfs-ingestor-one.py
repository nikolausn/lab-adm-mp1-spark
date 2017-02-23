
import os;
import sys;
import numpy as np;
import hdf5_getters;
import csv;
import simplejson as json;
import tables;
import codecs;
import base64;


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
            #return obj.toString()
            """
            return {
                '__ndarray__': base64.b64encode(obj.tostring()),
                'dtype': obj.dtype.str,
                'shape': obj.shape,
            }
            """
        elif isinstance(obj, np.bytes_):
        	return str(obj,'utf-8')
        else:
            return super(MyEncoder, self).default(obj)

if __name__ == '__main__':
	""" MAIN """
	arg = sys.argv
	# Trace through directory
	# the data has 3 level
	# we will traverse into all directories and convert the h5 file into csv format before ingesting into hadoop
	# using walk operation, recur our finding into all the contens

	blacklistedProperties = [
		'get_segments_loudness_start',
		'get_segments_timbre',
		'get_tatums_start',
		'get_segments_pitches',
		'get_segments_confidence',
		'get_beats_confidence',
		'get_bars_start',
		'get_bars_confidence',
		'get_beats_start',
		'get_tatums_confidence',
		'get_sections_start',
		'get_segments_start',
		'get_segments_loudness_max',
		'get_segments_loudness_max_time'
	]
	jsonfile = arg[2];
	jsonFile = open(jsonfile,'w');
	arrJsonContent = [];
	for dirname, dirnames, filenames in os.walk(arg[1]):
		for filename in filenames:
			if filename.find('.h5')>=0 and filename.find('json')<0:
			# if filename contains h5				
				print(filename);
				hdf5path = os.path.join(dirname,filename);
				jsonpath = os.path.join(dirname,jsonfile);
				print(jsonpath);

				# sanity check
				if not os.path.isfile(hdf5path):
					print('ERROR: file',hdf5path,'does not exist.')
					sys.exit(0)

				h5 = hdf5_getters.open_h5_file_read(hdf5path)
				numSongs = hdf5_getters.get_num_songs(h5)
				songidx = 0
				if songidx >= numSongs:
					print('ERROR: file contains only',numSongs)
					h5.close()
					sys.exit(0)

				# get all getters
				getters = filter(lambda x: x[:4] == 'get_', hdf5_getters.__dict__.keys())
				#getters.remove("get_num_songs") # special case
				onegetter = ''
				if onegetter == 'num_songs' or onegetter == 'get_num_songs':
				    getters = []
				elif onegetter != '':
				    if onegetter[:4] != 'get_':
				        onegetter = 'get_' + onegetter
				    try:
				        getters.index(onegetter)
				    except ValueError:
				        print('ERROR: getter requested:',onegetter,'does not exist.')
				        h5.close()
				        sys.exit(0)
				    getters = [onegetter]
				#getters = np.sort(getters)
				print(getters);

				#get number of songs
				numsong = hdf5_getters.get_num_songs(h5);
				print(numsong);

				# convert to dict object
				arrRes = [];
				for songidx in range(0,numsong):
					res = {}
					for getter in getters:
						#print(getter);
						if getter != 'get_num_songs' and getter not in blacklistedProperties:
							try:
								res[getter] = hdf5_getters.__getattribute__(getter)(h5,songidx)
							except(AttributeError, e):
								if summary:
									continue
								else:
									print(e)
									print('forgot -summary flag? specified wrong getter?')
							#print(tempRes.__class__.__name__ );
							#if tempRes.__class__.__name__ == 'ndarray':
							#	res[getter] = tempRes.toList();
							#	json.dumps(tempRes);
							#if tempRes.__class__.__name__ != 'ndarray':
							#res[getter] = tempRes;
							#print(tempRes)
							#json.dumps(tempRes,cls=MyEncoder);
					arrRes.append(res);
					arrJsonContent.append(res);
						#else:
						#    print getter[4:]+":",res

				#jsonFile.write(json.dumps(arrRes,cls=MyEncoder))				
				#print(json.dumps(res));
				#json.dump(res, codecs.open(jsonpath, 'w', encoding='utf-8'), separators=(',', ':'), sort_keys=True, indent=4) ### this saves the array in .json format
				h5.close()
	jsonFile.write(json.dumps(arrJsonContent,cls=MyEncoder))
	jsonFile.close();
