# BrewER
Giovanni Simonini, Luca Zecchini, Sonia Bergamaschi, Felix Naumann: "Entity Resolution On-Demand" (paper submitted to VLDB 2022)

Code written using Python 3.

The files uploaded to the folder "dataset_generation" can be used to generate the pre-processed versions of the four considered datasets (as used for the paper) from their raw versions. These files are designed to be located in the main folder, retrieving the raw data from a folder called "raw_data" and storing the obtained pre-processed CSV files into another folder called "data".
Here are listed, for each one of the four paper datasets, the names used in the code to refer to them, together with the raw files to be stored in the folder "raw_data" in order to get their pre-processed versions; please notice that the suffix "no_nan" denotes the pre-processed versions obtained by filtering out the records with a null ordering value, i.e., the ones adopted as basic versions in the paper.
- SIGMOD20, referred to as alaska_camera: download the dataset X from http://www.inf.uniroma3.it/db/sigmod2020contest/task.html and put the folder "2013_camera_specs" into the folder "raw_data";
- SIGMOD21, referred to as altosight_sigmod;
- Altosight, referred to as altosight;
- Funding, referred to as funding: download the file "address.csv" (https://raw.githubusercontent.com/qcri/data_civilizer_system/master/grecord_service/gr/data/address/address.csv) and put it into the folder "raw_data".
