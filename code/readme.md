## Steps
* Generate dataset for model building
	
	Run etl queries in doordash-etl-python3 to generate a historcial table ** CHIZHANG.fact_variance_reduction_flf_inputs **. remember to change the time range to at least 4 weeks. Currently use 2020-04-30~2020-05-30, but can use more recent data. DON'T include the most recent 2 weeks.
	
	* Copy the table and save to dbfs 
	* Training data: at least 2 weeks
	* Testing data 1 or 2 week(s)


* Use the generated data for training notebook ** variance_reduction_v6_etl_live.ipynb **
	* Check a few things to make sure the model has predictive power
		* Model accuracy
		* Prediction and ground truth correlation 
	* Generate a csv or table for simulation -- we need to do this everytime we update the model

* Test in etl
	* Run etl in doordash-etl-python3 to generate a new ** CHIZHANG.fact_variance_reduction_flf_inputs ** using the ** most recent 2 weeks ** of data
	* Run etl prediction in doordash-etl-python3 to predict is_above_ideal_flf for the 1 or 2 weeks data generated in last step


