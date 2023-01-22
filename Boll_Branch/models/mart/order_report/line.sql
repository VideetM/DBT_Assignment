select * 
from {{ metrics.calculate(
    metric('avg_order_unit'),
    grain='month',
    dimensions=['order_created_at']
) }}