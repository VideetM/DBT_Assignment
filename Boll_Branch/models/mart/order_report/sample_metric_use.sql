 -- depends_on: {{ ref('dbt_metrics_default_calendar') }}
 
select * 
from {{ metrics.calculate(
    metric('avg_order_unit'), 
    grain='day', 
    dimensions=['product_category'] 
   
) }}


-- line 5 - enter metric name (all metrics and details to be found in _source.yml file)
-- line 6 - time grain for the metric
-- line 7 dimension for slicing/dicing the data