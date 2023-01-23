 -- depends_on: {{ ref('dbt_metrics_default_calendar') }}
 
select * 
from {{ metrics.calculate(
    metric('avg_order_unit'),
    grain='day'
   
) }}


