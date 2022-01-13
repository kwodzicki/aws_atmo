This directory contains IDL include files containing different lists of NEXRAD stations
to be used in the GridRad data processing.  Unless there are special requirements,
programs should use the complete list, nexrad_station_id_list.pro.

The files are:

	nexrad_station_id_list.pro						Comprehensive list of NEXRAD stations for the contiguous U.S.  Currently this is the same
															as nexrad_station_id_list_143.pro

	nexrad_station_id_list_125.pro				The original list of NEXRAD stations covering most of the U.S. except the west coast
	nexrad_station_id_list_west_coast.pro		16 west coast stations not included in nexrad_station_id_list_125.pro
	nexrad_station_id_list_141.pro				Combined lists from nexrad_station_id_list_125.pro and nexrad_station_id_list_west_coast.pro
	nexrad_station_id_list_west_coast_2.pro	2 west coast stations not included in nexrad_station_id_list_west_coast.pro
	nexrad_station_id_list_143.pro				Combined lists from nexrad_station_id_list_141.pro and nexrad_station_id_list_west_coast_2.pro
	