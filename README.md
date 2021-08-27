# BrewER
Giovanni Simonini, Luca Zecchini, Sonia Bergamaschi, Felix Naumann: "Entity Resolution On-Demand" (paper submitted to VLDB 2022)

Code written using Python 3.

The files uploaded to the folder "dataset_generation" can be used to generate the pre-processed versions of the four considered datasets (as used for the paper) from their raw versions. These files are designed to be located in the main folder, retrieving the raw data from a folder called "data_raw" and storing the obtained pre-processed CSV files into another folder called "data".
Here are listed, for each one of the four paper datasets, the names used in the code to refer to them, together with the raw files to be stored in the folder "data_raw" in order to get their pre-processed versions; please notice that the suffix "no_nan" denotes the pre-processed versions obtained by filtering out the records with a null ordering value, i.e., the ones adopted as basic versions in the paper.
- SIGMOD20, referred to as alaska_camera: download the dataset X from http://www.inf.uniroma3.it/db/sigmod2020contest/task.html and put the folder "2013_camera_specs" into the folder "data_raw";
- SIGMOD21, referred to as altosight_sigmod, only partially available at the moment (see the datasets X4 and Y4 at https://dbgroup.ing.unimo.it/sigmod21contest/task.shtml) - it will be made available in its completeness in the future;
- Altosight, referred to as altosight, which will be made available in the future;
- Funding, referred to as funding: download the file "address.csv" (https://raw.githubusercontent.com/qcri/data_civilizer_system/master/grecord_service/gr/data/address/address.csv) and put it into the folder "data_raw".

The file "main.py" contains the effective implementation of BrewER algorithm for ER-on-demand (progressive query-driven ER), while in the file "task_definition.py" are already implemented the classes that can be used to run batches of queries on each dataset version, where it is possible to set the parameters for the specific task. In the file "main.py", it will be enough to select the correct class and to set the indices for the queries to be executed for the current batch.
