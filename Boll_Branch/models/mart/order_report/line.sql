select * 
from {{ metrics.calculate(
    metric('total_gross_revenue'),
    grain='month',
    dimensions=['product_category']
) }}