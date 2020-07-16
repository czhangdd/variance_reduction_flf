import datetime
import logging
import numpy as np
from etl.jobs.ddware import fact_base
from etl.lib import ml_utils
from etl.lib import snow
from etl.lib import s3
import pytz
from category_encoders import TargetEncoder

DOUBLE_QUOTES_ENCL_FORMAT_OPTIONS = """FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE 
                NULL_IF = ('NULL')"""


class ETL(fact_base.ETL):
    def __init__(self, options):
        self.config = self.get_config()
        self.pre_init_result = self.pre_init(options)
        super(ETL, self).__init__(options)

    def get_config(self):
        today_date = datetime.datetime.now(pytz.timezone('US/Pacific')).date()
 
        config_dict = {
            'job_run_date':today_date,
            's3_bucket':'doordash-data',
            's3_predictions_key_base_path':'var_reduction/predictions',
            'flf_prediction': {
                    's3_bucket':'doordash-data',
                    's3_models_key_base_path':'var_reduction/071520_flf_prediction_variance_reduction_ppl.dill',
                    's3_predictions_key_base_path':'var_reduction/predictions',
                    'model_path':'050320_flf_prediction_variance_reduction_ppl.dill',
                    'model_id':'050320_flf_prediction_variance_reduction_ppl'
                    },
        }
        return config_dict

    def insert(self):
        s3path = self.pre_init_result
        if not s3path:
            logging.info("S3 path is empty, skipping write to Snowflake")
        else:
            snow.load_to_snow(s3path=s3path, options=self.options, format_options=DOUBLE_QUOTES_ENCL_FORMAT_OPTIONS)

    def _write_to_s3(self, data, schemaname):
        job_run_date = self.config['job_run_date']

        logging.info('Saving predictions of shape {} to S3'.format(data.shape))
        local_predictions_filepath = 'flf_var_reduction_prediction.csv'
        data.to_csv(local_predictions_filepath, index=False, header=False)

        predictions_output_base_path = self.config['s3_predictions_key_base_path']
        predictions_output_path = '{}/{}_{}'.format(
            predictions_output_base_path,
            job_run_date.strftime("%Y-%m-%d"),
            schemaname
        )
        return s3.upload_to_s3(local_predictions_filepath,
                               bucket=self.config['s3_bucket'],
                               s3key=predictions_output_path)

    def _get_features_data(self, options):
        job_run_date = self.config['job_run_date']
        logging.info('Fetching features data to predict on '
                     'job_run_date={}'.format(job_run_date))
        
        query = "SELECT * FROM chizhang.fact_variance_reduction_flf_inputs"

        data = ml_utils.load_data_from_warehouse(options.get('snowflake'), query)

        return data

    def _get_model(self, options, model_type):
            ppl = ml_utils.get_model_from_s3(
                    bucket=self.config[model_type]['s3_bucket'],
                    key = self.config[model_type]['s3_models_key_base_path']
                    )
            return ppl
    
    def _make_predictions(self, data, model, model_type):
        data_with_predictions = data.copy()

        data_with_predictions.loc[:,'preds'] = model.predict_proba(data)[:, 1]
        data_with_predictions.loc[:,'model_id'] = self.config[model_type]['model_id']

        return data_with_predictions

    def pre_init(self, options):
        """
        Steps:
        1. Get all features from input table
        2. Fetch model(s) from S3
        3. Make predictions for model(s)
        4. [optional] Postprocess predictions
        5. Generate results in schema and upload to S3 (doesn't write to s3 if data is empty)
        """
        data = self._get_features_data(options)

        # feat_cat = ['day_of_week', 'hour_of_day', 'daypart']
        # feat_num = ['store_starting_point_id', 'submarket_id', 'window_id']

        feat_cat = ['day_of_week', 'hour_of_day', 'daypart', 'hh_hourly_weather_summary', 'hh_icon']
        feat_cat_target = ['store_starting_point_id', 'submarket_id', 'window_id']
        features_weather = ['hh_temperature', 'hh_apparent_temperature', 'hh_pressure', 'hh_humidity', 'hh_visibility', 'hh_wind_speed', 'hh_cloud_cover', 'hh_dewpoint', 'hh_precip_intensity', 'hh_precip_probability']
        features_supply_demand = ['pred_demand', 'actual_demand', 'under_predicted_demand']
        features_hist_flf = ['pw_hourly_avg_flf', 'pw_hourly_avg_is_flf_above_ideal', 'pw_hourly_avg_is_flf_above_max']
        features_incentive = ['pw_hourly_avg_dasher_pay_other']
        feat_num = feat_cat_target + features_weather + features_supply_demand + features_hist_flf + features_incentive
        features = feat_cat + feat_num

        data_to_predict_on = data[features]

        for f in feat_cat:
            data_to_predict_on[f] = data_to_predict_on[f].astype('category')
        for f in feat_num:
            data_to_predict_on[f] = data_to_predict_on[f].astype('float')

        pipeline = self._get_model(options, 'flf_prediction')

        flf_output_data = self._make_predictions(data_to_predict_on, pipeline, 'flf_prediction')
        flf_output_data = flf_output_data.join(data['created_at'])

        flf_output_data = flf_output_data[['created_at','store_starting_point_id', 'window_id','daypart', 'preds']]

        s3_path = None
        if flf_output_data is not None:
            s3_path = self._write_to_s3(flf_output_data, options['target_schema'])
        return s3_path
        