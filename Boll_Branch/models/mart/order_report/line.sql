select * 
from {{ metrics.calculate(
    metric('avg_order_value'),
    grain='month',
    dimensions=['order_id']
) }}