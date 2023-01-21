-- depends_on: {{ ref('dbt_metrics_default_calendar') }}
{% set my_metric_yml -%}

metrics:
  - name: develop_metric
    label: page views
    model: ref('web_events_report')
    description: "Total number of page views where event is page"

    calculation_method: count
    expression: event_id 

    timestamp: event_timestamp
    time_grains: [day, week, month, quarter, year]

    dimensions:
      - utm_source
      - utm_campaign
      - utm_medium

    filters:
      - field: event_name
        operator: '='
        value: "'page'"


{%- endset %}

select * 
from {{ metrics.develop(
        develop_yml=my_metric_yml,
        grain='week',
        metric_list=['develop_metric']
        )
    }}