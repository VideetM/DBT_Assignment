-- depends_on: {{ ref('dbt_metrics_default_calendar') }}
select * 
from {{ metrics.calculate(
    metric('total_web_user'),
    grain='month',
    dimensions=['utm_campaign']
) }}

