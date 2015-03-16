Large Dataset Uploader
======================

Code for the ingestion of large datasets (GBIF and eBird) into Map Of Life's CartoDB instance.

Author: Javier Otegui (javier.otegui@gmail.com)

What it does
------------

This program takes an occurrence table already in RedShift, builds a slim version (with a user-defined set of fields) and migrates the slim version of the content to a CartoDB set of partitioned tables. The specific steps are defined in the "How does it work?" section of this doc file.

The whole process runs seamlessly, with the exception of two stages at which user input is needed. Specifically, steps #4 of part #1 and step #1 of part #2 require user input. More detail on their section, under "How does it work?".

How does it work?
-----------------

The program runs a sequence of steps, each one doing a single task. All the tasks are grouped into two parts, and they (both the tasks and the parts) can be called individually. When calling the main function, all steps are executed sequentially.

### Before executing: Edit `settings.py`

Before execution, the `settings.py` file must be edited. This file contains the names of certain variables and the description of certain tables, and the content must be edited to match the actual values of those variables and tables.

The variables that must be checked are:

- `source`: It reflects the data source to be processed. Basically, one of these two values: `gbif` or `ebird`. This variable is used to build other variables throughout the process.
- `redshift_table`: this variable must contain the name of the table in RedShift that holds the points to be migrated to CartoDB.
- `fields`: This variable holds an array of two-element arrays that represents the list of fields from the redshift table that must be included in the slim version of the table. Each two-element item represents a field, with the name of the field in the first place and the element type (`varchar`, `date`, `double precision`...) in the second position.
- `slices_folder`: the name of a folder that will be created in the location of the `PointProcessor.py` script. This folder will hold the slices of the slim version of the RedShift table before uploading them to CartoDB.
- `sciname_field`: Here, the user must specify the name of the field that holds the scientific name in the RedShift table. For eBird, this field is `scientific_name`, and for the latest version of GBIF, it is `species`.
- `threshold`: the maximum number of records that each partition must have. A larger `threshold` will allow more records per partition table, generating less tables, while a smaller `threshold` will limit the number of records in each partition table, generating more tables. See the `Grouper.py` section for more info on this variable.

There is a working example for `settings.py`, with the values for processing eBird data, in the repository.

### Executing `PointProcessor.py`

After `settings.py` have been properly modified, the main process can be called. There are several possible calls, each one for a particular purpose:

The whole process runs sequentially by executing:

	python PointProcessor.py

Another option is to call it part-by-part:

	from PointProcessor import PointProcessor
	p = PointProcessor()
	p.part_1()  # For executing part 1, or
	p.part_2()  # For executing part 2

Or even task-by-task:

	p.create_s3_bucket()  # For executing task 1, or
	p.create_unload()  # For executing task 2
	...

Check the names of the individual functions to call them one-by-one.

Here, we will see how the full process works. It consists of 2 parts:

### Part 1: Download a slim version of the data

This part handles the creation and download of the slim version of the table in RedShift. It is split in 5 different tasks.

#### Task 1: Create a bucket in Amazon S3

The first function creates a bucket in Amazon's S3, named `[source]_slim` (*e.g.*, `ebird_slim` when processing eBird data). This task calls the `s3cmd` command.

#### Task 2: Populate the download SQL script

This task populates the `unload.sql` template script with the appropriate values. This file holds a SQL query template to move the selection of fields from the specified table to a bucket in Amazon S3, the bucket generated in Task #1. The script is not executed yet, it is only filled with the required values. 

#### Task 3: Execute the download SQL script

Now, the populated `unload.sql` script is executed. This will generate a series of files in the specified S3 bucket, each one containing a slice of the table with the appropriate format.

#### Task 4: Download the content of the Amazon S3 bucket to the local system

The script creates the folder `slices_folder` in the base directory and downloads the content of the S3 bucket to it. After the download process has finished, **user input is required** to make sure it finished properly. The need for this confirmation came from one of the tests, when the connection was closed and the files were not properly downloaded. The script shows the current content of the `slices_folder` and asks if all the slices were downloaded correctly. If not, it wipes the folder and starts again. If they did, it continues.

#### Task 5: Delete the bucket in Amazon S3

Lastly, the S3 bucket and its contents are deleted. This is done in order to avoid extra charges by Amazon, and this is why it is important to make sure the files have been properly downloaded in the previous step.

### Part 2: Create the partitioned table infrastructure and upload the records

The second part handles the creation of the partition table scaffolding and the population of the tables with the actual content. It is split in 4 different tasks.

#### Task 1: Create master table in CartoDB UI

The only way a table in CartoDB is surfaced to the UI is if it has been created through the UI. Therefore, **user input is required** here to create the table using the CartoDB UI. **Just create an empty table and rename it**, there is no need to add any of the columns. The next step will add the required columns programmatically. The script gives the required instructions and freezes until the table has been created and it asks for the name of the table. This will be used as master table, and **it is important that no other table has the same name**.

#### Task 2: Modify the master table adding the appropriate columns

Once with the name of the master table, this task adds the columns with the names and definitions given in the `fields` variable in `settings.py`. This is done using the CartoDB SQL API, in the background, with a single call.

#### Task 3: Create the partitioned tables

A different script, `Grouper.py` is called to calculate and create the partitioned tables system. More info on this script after the last task.

#### Task 4: Upload the slices to the partitioned tables

After `Grouper.py` has finished, each slice is uploaded to the master table and each record is redirected to the proper sub-table.

#### Task 5: Update the content of the_geom and the_geom_webmercator fields

Finally, after all partitions have been uploaded, the_geom and the_geom_webmercator fields are updated. The function goes through all partitions converting the values in the coordinate fields into PostGIS point entities. This step may take a long time to complete, depending on the final volume of the tables.

#### Extra info: `Grouper.py` and the partitioned tables

`Grouper.py` is a different script that handles the definition of ranges and creation of partitioned tables. It works as follows:

##### First: Define the grouping factors

Each sub-table will contain the records that belong to a certain set of species.

To calculate the set of species for each table, the script starts by grouping the species names by their first character and counting the number of records for each group. Then, starting with the smallest group, it aggregates the groups until they reach (but not pass) the threshold value. The set of species for the first table is thus defined, and the next group starts the set for a new table.

When a single group is too large (has more records than the threshold value), a new process begins with the remaining groups, and the grouping factor is now the first two characters. The species are grouped by their first two characters and, beginning from the smallest group, they are aggregated until the total reaches the threshold, and so on.

It is important to define a good threshold. The amount of partitions and their volume, and in the end the efficiency of the whole system, depend on this value. It should be small enough so that scanning each table is efficient, but large enough to avoid the creation of hundreds of tables. And it should never be smaller than the amout of records of the largest species.

##### Second: Create the partition tables

After the grouping, this step just fills a series of templates and executes them in the CartoDB server. There are templates for:

- Creating each partition
- Creating indexes on the scientific name for each partition
- Create the trigger function to send each new record to the appropriate partition
- Create the trigger with the previous function to be executed before any `insert`

Requirements
------------

In order to work, the following python modules must be installed and available:

* requests
* psycopg2

They can be easily installed in a virtual environment for better version controlling. To do that, first create the virtual environment:

    virtualenv env

This will create a folder `env` with the required infrastructure. Then, activate the virtual environment by executing, from the base folder:

    . env/bin/activate

Then, create the lib folder to store the downloaded modules:

    mkdir ./lib

And, finally, download the modules to that folder, by executing:

    pip install -t ./lib -r requirements.txt

Also, the program needs a file called `cred.py` with the credentials for CartoDB, AWS and RedShift. This file should be located in the same folder as the `PointProcessor.py` file.

Installation
------------

The program is already located in a folder in Litoria, and it has everything it needs to work. It can be located in the following folder

	/home/javiero/GBIFUploader/large-dataset-uploader
