from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_quality_checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks #dictionary of sql queries and expected values

    def execute(self, context):
        self.log.info('Redshift Connection')
        redshift = PostgresHook(self.redshift_conn_id)
        error_count  = 0
        
        """Looping over the dictionary"""
        for i in self.data_quality_checks:
            
            check_query     = i.get('data_check_sql')
            expected_result = i.get('expected_value')
            
            result = redshift.get_records(check_query)[0]
            
            self.log.info(f"Running the query   : {check_query}")
            self.log.info(f"Expected result : {expected_result}")
            self.log.info(f"Check result    : {result}")
            
            """Checking if the actual value matches the expected value. 
            If they dont, the count of error goes up"""
            if result[0] != expected_result:
                error_count += 1
                self.log.info(f"Data quality check fails At   : {check_query}")
                
        """Checking of the error count is greater than zero which means the data quality check has failed"""    
        if error_count > 0:
            self.log.info('Data quality checks has Failed !')
        else:
            self.log.info('Data quality checks has Passed !')