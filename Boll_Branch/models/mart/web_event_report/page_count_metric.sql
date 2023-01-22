-- depends_on: {{ ref('dbt_metrics_default_calendar') }}
select * 
from {{ metrics.calculate(
    metric('order_complete_count'),
    grain='month',
    dimensions=['utm_medium']
) }}

