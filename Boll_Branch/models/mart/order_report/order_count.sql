 -- depends_on: {{ ref('dbt_metrics_default_calendar') }}
 
select * 
from {{ metrics.calculate(
    metric('total_order_count'),
    grain='month',
    dimensions=['product_category']
) }}


